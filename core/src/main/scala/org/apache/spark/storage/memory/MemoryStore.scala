/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage.memory

import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{UNROLL_MEMORY_CHECK_PERIOD, UNROLL_MEMORY_GROWTH_FACTOR}
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.serializer.{SerializationStream, SerializerManager}
import org.apache.spark.storage._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

private sealed trait MemoryEntry[T] {
  def size: Long             // 当前 block 的大小
  def memoryMode: MemoryMode // block 存入内存的内存模式
  def classTag: ClassTag[T]  // block 的类型标记
}
//  MemoryEntry  两个实现类
//  DeserializedMemoryEntry 反序列化后的MemoryEntry
private case class DeserializedMemoryEntry[T](
    value: Array[T],
    size: Long,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
  val memoryMode: MemoryMode = MemoryMode.ON_HEAP
}

//  SerializedMemoryEntry 序列化后的MemoryEntry
private case class SerializedMemoryEntry[T](
    buffer: ChunkedByteBuffer,
    memoryMode: MemoryMode,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
  def size: Long = buffer.size
}

/**
  * BlockEvictionHandler 用于将block从内存中驱逐出去
  */
private[storage] trait BlockEvictionHandler {
  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
   *
   * @return the block's new effective StorageLevel.
   */
  private[storage] def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel
}

/**
  *
  * MemoryStore负责将Block存储到内存。Spark通过将广播数据、RDD、Shuffle数据 存储到内存，
  * 减少了对磁盘I/O的依赖，提高了程序的读写效率。MemoryStore依赖于MemoryManager的服务
  * <br><br>
  *
  *
  *
  * MemoryStore内存模型，整个MemoryStore的存储分为三块: （P-269）<br>
  * 1、一块是Memory Store的entries属 性持有的很多MemoryEntry所占据的内存blocksMemoryUsed ;<br>
  * 2、—块是onHeapUnrollMemoryMap或offHeapUnrollMemoryMap 中使用展开方式占用的内存 currentUnroll-Memory。
  * 展开Block的行为类似于人们生活中的“占座”，一间教室里有些座位有人，有些 则空着。在座位上放一本书表示有人正在使用，
  * 那么别人就不会坐这些座位。这可以防止 在你需要座位的时候，却发现已经没有了位置。这样可以防止在向内存真正写人数据时，
  * 内存不足发生溢出。blocksMemoryUsed和currentUnrollMemory的空间之和是已经使用的空间，用memoryUsed表示。<br>
  * 3、还有一块内存没有任何标记，表示未使用。
  * <br><br>
  * BlockManager 实现了特质 BlockEvictionHandler，并重写了 dropFromMemory 方法，
  * BlockManager在构造MemoryStore时，将自身的引用作为blockEvictionHandler参数传递 给 MemoryStore 的构造器，
  * 因而 BlockEvictionHandler 就是 BlockManager。BlockManager 及BlockManager重写的dropFromMemory方法的实现
  * 都将在6.7节详细介绍。
  *
  *  Stores blocks in memory, either as Arrays of deserialized Java objects or as
  *  serialized ByteBuffers.
  *
  */

private[spark] class MemoryStore(
    conf: SparkConf,
    blockInfoManager: BlockInfoManager,
    serializerManager: SerializerManager,
    memoryManager: MemoryManager,
    blockEvictionHandler: BlockEvictionHandler)
  extends Logging {

  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
  // acquiring or releasing unroll memory, must be synchronized on `memoryManager`!

  /**
    * entries  >>>> LinkedHashMap[BlockId, MemoryEntry[_]
    * 注意:所有内存分配的变化，特别是放置块、清除块和获取或释放 unroll memory，必须在 memoryManager 上同步!
    * MemoryStore中维护的entries map 其实就是真正存放每个block的数据
    * 每个Block在内存中的数据，用MemoryEntry代表
    */
  private val entries = new LinkedHashMap[BlockId, MemoryEntry[_]](32, 0.75f, true)

  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `memoryManager`
  /**
    * 任务尝试线程的标识TaskAttemptld与任务尝试线程在堆内存展开的所有Block占用的内存大小之和之间的映射关系。
    */
  private val onHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()
  // Note: off-heap unroll memory is only used in putIteratorAsBytes() because off-heap caching
  // always stores serialized values.
  /**
    * 任务尝试线程的标识TaskAttemptld与任务尝试线程在堆外内存展开的所有Block占用的内存大小之和之间的映射关系。
    */
  private val offHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()

  // Initial memory to request before unrolling any block
  /**
    * 用来展开任何Block之前，初始请求的内存大小，
    * 可以修改属性 spark.storage.unrollMemoryThreshold (默认为 1MB)改变大小。
    */
  private val unrollMemoryThreshold: Long =
    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)

  /**
    * MemoryStore用于存储 Block 的最大内存，<br>
    * 其实质为 MemoryManager 的 maxOnHeapStorageMemory 和 maxOffHeapStorageMemory 之和。
    * 如果 Memory Manager 为 StaticMemoryManager ,那么 maxMemory 的大小是固定的。
    * 如果 Memory Manager 为 UnifiedMemoryManager,那么 maxMemory 的大小是动态变化的。<br>
    *
    * UnifiedMemoryManager <br>
    *
    *   堆内执行内存和堆内存储内存公用 maxHeapMemory<br>
    *   maxOnHeapStorageMemory =  maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
    *
    *   堆外执行内存和堆外存储内存公用 maxOffHeapMemory<br>
    *   maxOffHeapStorageMemory = maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
    *
    * Total amount of memory available for storage, in bytes.
    * */
  private def maxMemory: Long = {
    memoryManager.maxOnHeapStorageMemory + memoryManager.maxOffHeapStorageMemory
  }

  if (maxMemory < unrollMemoryThreshold) {
    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
      s"memory. Please configure Spark with more memory.")
  }

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /**
    * MemoryStore中已经使用的内存大小。<br>
    * 其实质为MemoryManager中 onHeapStorageMemoryPool已经使用的大小和<br>
    * offHeapStorageMemoryPool已经使用的大小之和。<br>
    *
    * Total storage memory used including unroll memory, in bytes. */
  private def memoryUsed: Long = memoryManager.storageMemoryUsed

  /**
    * MemoryStore 用于存储 Block (即 MemoryEntry)使用的内存大小，
    * 即 memoryUsed 与 currentUnrollMemory 的差值。<br>
    *
    *blocksMemoryUsed = memoryUsed - currentUnrollMemory<br>
   * Amount of storage memory, in bytes, used for caching blocks.
   * This does not include memory used for unrolling.
   */
  private def blocksMemoryUsed: Long = memoryManager.synchronized {
    memoryUsed - currentUnrollMemory
  }

  /**
     获取Blockld对应MemoryEntry (即Block的内存形式）所占用的大小
    */
  def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  /**
    * 将Blockld对应的Block (已经封装为ChunkedByteBuffer)写人内存<br>
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   *
   * The caller should guarantee that `size` is correct.
   *
   * @return true if the put() succeeded, false otherwise.
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      size: Long,
      memoryMode: MemoryMode,
      _bytes: () => ChunkedByteBuffer): Boolean = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) {
      // We acquired enough memory for the block, so go ahead and put it
      val bytes = _bytes()
      assert(bytes.size == size)
      val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
      entries.synchronized {
        entries.put(blockId, entry)
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      true
    } else {
      false
    }
  }

  /**
    * 尝试将给定的块作为值或字节存储在内存中。
    * <br>
    * 迭代器可能太大而无法物化并存储在内存中。为了避免OOM异常，此方法将逐步展开迭代器，同时定期检查
    * 是否有足够的空闲内存。如果块已成功物化，则物化过程中使用的临时展开内存被“转移”到存储内存中，
    * 所以我们不会获得比实际需要更多的内存来存储块。
    * <br>
    * <br>
    *
   * Attempt to put the given block in memory store as values or bytes.
   *
   * It's possible that the iterator is too large to materialize and store in memory. To avoid
   * OOM exceptions, this method will gradually unroll the iterator while periodically checking
   * whether there is enough free memory. If the block is successfully materialized, then the
   * temporary unroll memory used during the materialization is "transferred" to storage memory,
   * so we won't acquire more memory than is actually needed to store the block.
   *
   * @param blockId The block id.
   * @param values The values which need be stored.
   * @param classTag the [[ClassTag]] for the block.
   * @param memoryMode The values saved memory mode(ON_HEAP or OFF_HEAP).
   * @param valuesHolder A holder that supports storing record of values into memory store as
   *        values or bytes.
   * @return if the block is stored successfully, return the stored data size. Else return the
   *         memory has reserved for unrolling the block (There are two reasons for store failed:
   *         First, the block is partially-unrolled; second, the block is entirely unrolled and
   *         the actual stored data size is larger than reserved, but we can't request extra
   *         memory).
   */
  private def putIterator[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode,
      valuesHolder: ValuesHolder[T]): Either[Long, Long] = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // 已经展开的元素数量
    var elementsUnrolled = 0
    // MemoryStore是否仍然有足够的内存，以便于继续展开Block (即Iterator)
    var keepUnrolling = true
    // 用来展开任何Block之前，初始请求的内存大小，
    // 可以修改属性spark.storage.unrollMemoryThreshold (默认为 1 MB)改变大小。
    val initialMemoryThreshold = unrollMemoryThreshold
    // 检査内存是否有足够的阀值，此值默认为16。字面上有周期
    // 的含义，但是此周期并非指时间，而是已经展开的元素的数量elementsUnrolled。
    val memoryCheckPeriod = conf.get(UNROLL_MEMORY_CHECK_PERIOD)
    // 当前任务用于展开Block所保留的内存
    var memoryThreshold = initialMemoryThreshold
    // 展开内存不充足时，请求增长的因子。此值默认为1.5
    val memoryGrowthFactor = conf.get(UNROLL_MEMORY_GROWTH_FACTOR)
    // Block已经使用的展开内存大小，初始大小为 initialMemoryThreshold
    var unrollMemoryUsedByThisBlock = 0L

    // 请求足够的内存开始unroll , keepUnrolling 表示是否成功
    keepUnrolling =
      reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, memoryMode)

    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }


    /*
      安全地展开此块，定期检查我们是否超出了阈值
      valuesHolder 属于类DeserializedValuesHolder
      private class DeserializedValuesHolder[T] (classTag: ClassTag[T]) extends ValuesHolder[T] {
        var vector = new SizeTrackingVector[T]()(classTag)

        与之前的vector的原理相同
        vector 用于追踪block(即 iterator)每次迭代的数据
     */

    while (values.hasNext && keepUnrolling) {
      //类似 vector+= values.next()， valuesHolder 存储了迭代数据的集合
      valuesHolder.storeValue(values.next())
      if (elementsUnrolled % memoryCheckPeriod == 0) { //周期性地检查
        // currentSize = 估计valuesHolder 的大小
        val currentSize = valuesHolder.estimatedSize()
        // If our vector's size has exceeded the threshold, request more memory
        // memoryThreshold 当前任务用于展开Block所保留的内存
        // 当前已展开的内存大小超过了memoryThreshold
        if (currentSize >= memoryThreshold) {
          //
          val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
          keepUnrolling =
            reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
          if (keepUnrolling) {
            unrollMemoryUsedByThisBlock += amountToRequest
          }
          // New threshold is currentSize * memoryGrowthFactor
          memoryThreshold += amountToRequest
        }
      }
      elementsUnrolled += 1
    }

    // Make sure that we have enough memory to store the block. By this point, it is possible that
    // the block's actual memory usage has exceeded the unroll memory by a small amount, so we
    // perform one final call to attempt to allocate additional memory if necessary.

/*    如果展开 Iterator中所有的数据后, keepUnrolling为true,则说明已经为 Block申
      请到足够多的保留内存,接下来的处理步骤如下。*/
    if (keepUnrolling) {
      val entryBuilder = valuesHolder.getBuilder()
      val size = entryBuilder.preciseSize
      // 如果valuesHolder的精准的大小>前面已经展开的内存大小，继续申请内存，这一步是为了确保准确
      if (size > unrollMemoryUsedByThisBlock) {
        val amountToRequest = size - unrollMemoryUsedByThisBlock
        keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
        if (keepUnrolling) {
          unrollMemoryUsedByThisBlock += amountToRequest
        }
      }

      //如果内存还够，此时就可以把块放入内存
      if (keepUnrolling) {
        val entry = entryBuilder.build()
        // Synchronize so that transfer is atomic
        memoryManager.synchronized {
          // 释放任务尝试线程占用的内存（大小为 unrollMemoryUsedByThisBlock）
          releaseUnrollMemoryForThisTask(memoryMode, unrollMemoryUsedByThisBlock)
          // 为存储blockId对应的block ,从堆内存和堆外内存所获取的 entry.size(所有的block[memoryEntry]大小)()
          val success = memoryManager.acquireStorageMemory(blockId, entry.size, memoryMode)
          assert(success, "transferring unroll memory to storage memory failed")
        }
        // 将块放入内存缓存
        entries.synchronized {
          entries.put(blockId, entry)
        }

        logInfo("Block %s stored as values in memory (estimated size %s, free %s)".format(blockId,
          Utils.bytesToString(entry.size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
        Right(entry.size)
      } else {
        // We ran out of space while unrolling the values for this block
        logUnrollFailureMessage(blockId, entryBuilder.preciseSize)
        Left(unrollMemoryUsedByThisBlock)
      }
    } else {
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, valuesHolder.estimatedSize())
      Left(unrollMemoryUsedByThisBlock)
    }
  }

  /**
   *  此方法将Blockld对应的Block (已经转换为Iterator)写人内存。有时候放人内存的Block很大，
    * 所以一次性将此对象写人内存可能将引发OOM异常。为了避免这种情况的发生，首先需要将Block转换为Iterator,
    * 然后渐进式地展开此Iterator,并且周期性地检査是否有足够的展开内存。<br>
   *
   * @return in case of success, the estimated size of the stored data. In case of failure, return
   *         an iterator containing the values of the block. The returned iterator will be backed
   *         by the combination of the partially-unrolled block and the remaining elements of the
   *         original input iterator. The caller must either fully consume this iterator or call
   *         `close()` on it in order to free the storage memory consumed by the partially-unrolled
   *         block.
   */
  private[storage] def putIteratorAsValues[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long] = {

    val valuesHolder = new DeserializedValuesHolder[T](classTag)

    putIterator(blockId, values, classTag, MemoryMode.ON_HEAP, valuesHolder) match {
      case Right(storedSize) => Right(storedSize)
      case Left(unrollMemoryUsedByThisBlock) =>
        val unrolledIterator = if (valuesHolder.vector != null) {
          valuesHolder.vector.iterator
        } else {
          valuesHolder.arrayValues.toIterator
        }

        Left(new PartiallyUnrolledIterator(
          this,
          MemoryMode.ON_HEAP,
          unrollMemoryUsedByThisBlock,
          unrolled = unrolledIterator,
          rest = values))
    }
  }

  /**
   *  以序列化后的字节数组形式，将BlockId 对应的 block 写入内存
   *
   * @return in case of success, the estimated size of the stored data. In case of failure,
   *         return a handle which allows the caller to either finish the serialization by
   *         spilling to disk or to deserialize the partially-serialized block and reconstruct
   *         the original input iterator. The caller must either fully consume this result
   *         iterator or call `discard()` on it in order to free the storage memory consumed by the
   *         partially-unrolled block.
   */
  private[storage] def putIteratorAsBytes[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode): Either[PartiallySerializedBlock[T], Long] = {

    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // Initial per-task memory to request for unrolling blocks (bytes).
    val initialMemoryThreshold = unrollMemoryThreshold
    val chunkSize = if (initialMemoryThreshold > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
      logWarning(s"Initial memory threshold of ${Utils.bytesToString(initialMemoryThreshold)} " +
        s"is too large to be set as chunk size. Chunk size has been capped to " +
        s"${Utils.bytesToString(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)}")
      ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
    } else {
      initialMemoryThreshold.toInt
    }

    val valuesHolder = new SerializedValuesHolder[T](blockId, chunkSize, classTag,
      memoryMode, serializerManager)

    putIterator(blockId, values, classTag, memoryMode, valuesHolder) match {
      case Right(storedSize) => Right(storedSize)
      case Left(unrollMemoryUsedByThisBlock) =>
        Left(new PartiallySerializedBlock(
          this,
          serializerManager,
          blockId,
          valuesHolder.serializationStream,
          valuesHolder.redirectableStream,
          unrollMemoryUsedByThisBlock,
          memoryMode,
          valuesHolder.bbos,
          values,
          classTag))
    }
  }

  /**
    *  只能获取序列化的 block
    */
  def getBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: DeserializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getBytes on serialized blocks")
      case SerializedMemoryEntry(bytes, _, _) => Some(bytes)
    }
  }

  /**
    * 只能获取非序列化的 block
    */
  def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: SerializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getValues on deserialized blocks")
      case DeserializedMemoryEntry(values, _, _) =>
        val x = Some(values)
        x.map(_.iterator)
    }
  }

  /**
    *   内存中移除 blockId 对应的 block
    */
  def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
    val entry = entries.synchronized {
      entries.remove(blockId)
    }
    if (entry != null) {
      entry match {
        // 对应的 ChunkedByteBuffer也要清理
        case SerializedMemoryEntry(buffer, _, _) => buffer.dispose()
        case _ =>
      }
      memoryManager.releaseStorageMemory(entry.size, entry.memoryMode)
      logDebug(s"Block $blockId of size ${entry.size} dropped " +
        s"from memory (free ${maxMemory - blocksMemoryUsed})")
      true
    } else {
      false
    }
  }

  /**
    *  清空 memoryStore
    */
  def clear(): Unit = memoryManager.synchronized {
    entries.synchronized {
      entries.clear()
    }
    onHeapUnrollMemoryMap.clear()
    offHeapUnrollMemoryMap.clear()
    memoryManager.releaseAllStorageMemory()
    logInfo("MemoryStore cleared")
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  /**
    *
    *驱逐 block，以便释放一些block来存储新的block
    *
   * Try to evict blocks to free up a given amount of space to store a particular block.
   * Can fail if either the block is bigger than our memory or it would require replacing
   * another block from the same RDD (which leads to a wasteful cyclic replacement pattern for
   * RDDs that don't fit into memory that we want to avoid).
   *
   * @param blockId 需要存储的 block的blockid
   * @param space the size of this block 需要释放发的空间大小
   * @param memoryMode the type of memory to free (on- or off-heap)
   * @return the amount of memory (in bytes) freed by eviction
   */
  private[spark] def evictBlocksToFreeSpace(
      blockId: Option[BlockId],
      space: Long,
      memoryMode: MemoryMode): Long = {
    assert(space > 0)
    memoryManager.synchronized {
      /**已经可以释放的内存大小*/
      var freedMemory = 0L

      val rddToAdd = blockId.flatMap(getRddId)
      /**已经选择的用于驱逐的Block的BlockEd的数组*/
      val selectedBlocks = new ArrayBuffer[BlockId]
      def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
        entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))
      }
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.

/*      当freedMemory小于space时,不断迭代遍历 Iterator。对于每个entries中的
        BlockEd和 Memory Entry,首先找出其中符合条件的 Block(只有符合条件的 Block才会
        被驱逐),然后获取Bock的写锁,最后将此Block的BlockId放入selectedBlocks并且将
        freedMemory增加 Block的大小。哪些Bock将符合条件呢?同时满足以下两个条件的。
        1、Memory Entry的内存模式与所需的内存模式一致。
        2、BlockId对应的Block不是RDD,或者BlockEd与 blockEd不是同一个RDD。*/
      entries.synchronized {
        val iterator = entries.entrySet().iterator()
        while (freedMemory < space && iterator.hasNext) {
          val pair = iterator.next()
          val blockId = pair.getKey
          val entry = pair.getValue
          if (blockIsEvictable(blockId, entry)) {
            // We don't want to evict blocks which are currently being read, so we need to obtain
            // an exclusive write lock on blocks which are candidates for eviction. We perform a
            // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
            /*
                我们不想将当前正在读取的块逐出，所以我们需要获取一个独占的写锁
                对于可能被驱逐的块。我们执行一个非阻塞的tryLock在这里，以忽略那些被锁读的块:
             */
            if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
              selectedBlocks += blockId
              freedMemory += pair.getValue.size
            }
          }
        }
      }

      /**
        * 如果 Block从内存中迁移到其他存储(如DiskStore)中,那么需要调用 BlockInfoManager
        * 的unlock释放当前任务尝试线程获取的被迁移Block的写锁。如果Block从存储体系中彻底移除了,
        * 那么需要调用BlockInfoManager的removeBlock方法删除被迁移Block的信息。
        */
      def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit = {
        val data = entry match {
          case DeserializedMemoryEntry(values, _, _) => Left(values)
          case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
        }
        val newEffectiveStorageLevel =
          blockEvictionHandler.dropFromMemory(blockId, () => data)(entry.classTag)
        if (newEffectiveStorageLevel.isValid) {
          // The block is still present in at least one store, so release the lock
          // but don't delete the block info

          //至少有一个存储中仍然存在这个块，所以释放锁
          //但是不要删除块信息
          // TODO: 一个块是否可以存多个 block, 块和我们说的 block区别是什么？
          blockInfoManager.unlock(blockId)
        } else {
          // The block isn't present in any store, so delete the block info so that the
          // block can be stored again
          //块不存在于任何 store ，所以删除块信息，以便块可以再次存储
          blockInfoManager.removeBlock(blockId)
        }
      }
      /*
          如果 freedMemory大于等于space,这说明通过驱逐一定数量的 Block,已经为存储blockId
          对应的 Block腾出了足够的内存空间,此时需要遍历selectedBlocks中的每个 BlockEd,并移除
          每个 BlockId对应的 Block。如果 Block从内存中迁移到其他存储(如 DiskStore)中,
          那么需要调用 BlockInfoManager的 unlock释放当前任务尝试线程获取的被迁移Blok的写锁。
          如果Block从存储体系中彻底移除了,那么需要调用 BlockInfoManager的 removeBlock方法删除
          被迁移Block的信息。
       */
      if (freedMemory >= space) {
        var lastSuccessfulBlock = -1
        try {
          logInfo(s"${selectedBlocks.size} blocks selected for dropping " +
            s"(${Utils.bytesToString(freedMemory)} bytes)")
          (0 until selectedBlocks.size).foreach { idx =>
            val blockId = selectedBlocks(idx)
            val entry = entries.synchronized {
              entries.get(blockId)
            }
            // This should never be null as only one task should be dropping
            // blocks and removing entries. However the check is still here for
            // future safety.
            if (entry != null) {
              dropBlock(blockId, entry)
              afterDropAction(blockId)
            }
            lastSuccessfulBlock = idx
          }
          logInfo(s"After dropping ${selectedBlocks.size} blocks, " +
            s"free memory is ${Utils.bytesToString(maxMemory - blocksMemoryUsed)}")
          freedMemory
        } finally {
          // like BlockManager.doPut, we use a finally rather than a catch to avoid having to deal
          // with InterruptedException
          
          // 如果还有没有块没有释放，就释放掉锁
          // 驱逐块的时候全部锁定了？
          // TODO: 是否可以优化？ 
          if (lastSuccessfulBlock != selectedBlocks.size - 1) {
            // the blocks we didn't process successfully are still locked, so we have to unlock them
            (lastSuccessfulBlock + 1 until selectedBlocks.size).foreach { idx =>
              val blockId = selectedBlocks(idx)
              blockInfoManager.unlock(blockId)
            }
          }
        }
      } else {
        blockId.foreach { id =>
          logInfo(s"Will not store $id")
        }
        selectedBlocks.foreach { id =>
          blockInfoManager.unlock(id)
        }
        0L
      }
    }
  }

  // hook for testing, so we can simulate a race
  protected def afterDropAction(blockId: BlockId): Unit = {}

  def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
    * 用于为展开尝试执行任务给定的Block,保留指定内存模式上指定大小的内存<br>
   * Reserve memory for unrolling the given block for this task.
   *
   * @return whether the request is granted.
   */
  def reserveUnrollMemoryForThisTask(
      blockId: BlockId,
      memory: Long,
      memoryMode: MemoryMode): Boolean = {
    memoryManager.synchronized {
      val success = memoryManager.acquireUnrollMemory(blockId, memory, memoryMode)
      if (success) {
        val taskAttemptId = currentTaskAttemptId()
        val unrollMemoryMap = memoryMode match {
          case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
          case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
        }
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      success
    }
  }

  /**
    * 释放任务尝试线程占用的内存
   * Release memory used by this task for unrolling blocks.
   * If the amount is not specified, remove the current task's allocation altogether.
   */
  def releaseUnrollMemoryForThisTask(memoryMode: MemoryMode, memory: Long = Long.MaxValue): Unit = {
    // memory 指定要释放的大小
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized {
      val unrollMemoryMap = memoryMode match {
        case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
        case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
      }
      if (unrollMemoryMap.contains(taskAttemptId)) {
        // 计算实际要释放的内存大小，此大小为指定要释放的大小和任务尝试线程在堆内存或堆外内存
        // 实际占有的内存大小之和之间的最小值。
        val memoryToRelease = math.min(memory, unrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) {
          //更新taskAttemptld与任务尝试线程在堆内存或堆外内存展开的所有Block占用的内存大小之和之间的映射关系。
          unrollMemoryMap(taskAttemptId) -= memoryToRelease
          // 调用 MemoryManager 的 releaseUnrollMemory 方法释放内存。
          memoryManager.releaseUnrollMemory(memoryToRelease, memoryMode)
        }
        //如果任务尝试线程在堆内存或堆外内存展开的所有Block占用的内存大小之和为0,
        //则清除此taskAttemptld与任务尝试线程在堆内存或堆外内存展开的所有Block占用的内存大小之和之间的映射关系。
        if (unrollMemoryMap(taskAttemptId) == 0) {
          unrollMemoryMap.remove(taskAttemptId)
        }
      }
    }
  }

  /**
    * MemoryStore用于展开Block使用的内存大小。其实质为 onHeapUnrollMemoryMap中的所有用于展开
    * Block所占用的内存大小与offHeap-UnrollMemoryMap中的所有用于展开Block所占用的内存大小之和。
    * <br>
   * Return the amount of memory currently occupied for unrolling blocks across all tasks.
   */
  def currentUnrollMemory: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.values.sum + offHeapUnrollMemoryMap.values.sum
  }

  /**
    * 当前的任务尝试线程用于展开Block所占用的内存。<br>
    * 即onHeapUnrollMemoryMap中缓存的当前任务尝试线程对应的占用大小与 <br>
    *   offHeapUnrollMemoryMap中缓存的当前的任务尝试线程对应的占用大小之和。
    *  <br>
   * Return the amount of memory currently occupied for unrolling blocks by this task.
   */
  def currentUnrollMemoryForThisTask: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L) +
      offHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
    * 当前使用MemoryStore展开Block的任务的数量。
    * 其实质为 onHeapUnrollMemoryMap 的键集合与 offHeapUnrollMemoryMap 的键集合的并集。<br>
   * Return the number of tasks currently unrolling blocks.
   */
  private def numTasksUnrolling: Int = memoryManager.synchronized {
    (onHeapUnrollMemoryMap.keys ++ offHeapUnrollMemoryMap.keys).toSet.size
  }

  /**
   * Log information about current memory usage.
   */
  private def logMemoryUsage(): Unit = {
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemoryUsed)} (blocks) + " +
      s"${Utils.bytesToString(currentUnrollMemory)} (scratch space shared across " +
      s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(memoryUsed)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
   * Log a warning for failing to unroll a block.
   *
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector before unrolling failed.
   */
  private def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}

private trait MemoryEntryBuilder[T] {
  def preciseSize: Long
  def build(): MemoryEntry[T]
}

private trait ValuesHolder[T] {
  def storeValue(value: T): Unit
  def estimatedSize(): Long

  /**
   * Note: After this method is called, the ValuesHolder is invalid, we can't store data and
   * get estimate size again.
   * @return a MemoryEntryBuilder which is used to build a memory entry and get the stored data
   *         size.
   */
  def getBuilder(): MemoryEntryBuilder[T]
}

/**
 * A holder for storing the deserialized values.
 */
private class DeserializedValuesHolder[T] (classTag: ClassTag[T]) extends ValuesHolder[T] {
  // Underlying vector for unrolling the block
  var vector = new SizeTrackingVector[T]()(classTag)
  var arrayValues: Array[T] = null

  override def storeValue(value: T): Unit = {
    vector += value
  }

  override def estimatedSize(): Long = {
    vector.estimateSize()
  }

  override def getBuilder(): MemoryEntryBuilder[T] = new MemoryEntryBuilder[T] {
    // We successfully unrolled the entirety of this block
    arrayValues = vector.toArray
    vector = null

    override val preciseSize: Long = SizeEstimator.estimate(arrayValues)

    override def build(): MemoryEntry[T] =
      DeserializedMemoryEntry[T](arrayValues, preciseSize, classTag)
  }
}

/**
 * A holder for storing the serialized values.
 */
private class SerializedValuesHolder[T](
    blockId: BlockId,
    chunkSize: Int,
    classTag: ClassTag[T],
    memoryMode: MemoryMode,
    serializerManager: SerializerManager) extends ValuesHolder[T] {
  val allocator = memoryMode match {
    case MemoryMode.ON_HEAP => ByteBuffer.allocate _
    case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
  }

  val redirectableStream = new RedirectableOutputStream
  val bbos = new ChunkedByteBufferOutputStream(chunkSize, allocator)
  redirectableStream.setOutputStream(bbos)
  val serializationStream: SerializationStream = {
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = serializerManager.getSerializer(classTag, autoPick).newInstance()
    ser.serializeStream(serializerManager.wrapForCompression(blockId, redirectableStream))
  }

  override def storeValue(value: T): Unit = {
    serializationStream.writeObject(value)(classTag)
  }

  override def estimatedSize(): Long = {
    bbos.size
  }

  override def getBuilder(): MemoryEntryBuilder[T] = new MemoryEntryBuilder[T] {
    // We successfully unrolled the entirety of this block
    serializationStream.close()

    override def preciseSize(): Long = bbos.size

    override def build(): MemoryEntry[T] =
      SerializedMemoryEntry[T](bbos.toChunkedByteBuffer, memoryMode, classTag)
  }
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsValues()]] call.
 *
 * @param memoryStore  the memoryStore, used for freeing memory.
 * @param memoryMode   the memory mode (on- or off-heap).
 * @param unrollMemory the amount of unroll memory used by the values in `unrolled`.
 * @param unrolled     an iterator for the partially-unrolled values.
 * @param rest         the rest of the original iterator passed to
 *                     [[MemoryStore.putIteratorAsValues()]].
 */
private[storage] class PartiallyUnrolledIterator[T](
    memoryStore: MemoryStore,
    memoryMode: MemoryMode,
    unrollMemory: Long,
    private[this] var unrolled: Iterator[T],
    rest: Iterator[T])
  extends Iterator[T] {

  private def releaseUnrollMemory(): Unit = {
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    // SPARK-17503: Garbage collects the unrolling memory before the life end of
    // PartiallyUnrolledIterator.
    unrolled = null
  }

  override def hasNext: Boolean = {
    if (unrolled == null) {
      rest.hasNext
    } else if (!unrolled.hasNext) {
      releaseUnrollMemory()
      rest.hasNext
    } else {
      true
    }
  }

  override def next(): T = {
    if (unrolled == null || !unrolled.hasNext) {
      rest.next()
    } else {
      unrolled.next()
    }
  }

  /**
   * Called to dispose of this iterator and free its memory.
   */
  def close(): Unit = {
    if (unrolled != null) {
      releaseUnrollMemory()
    }
  }
}

/**
 * A wrapper which allows an open [[OutputStream]] to be redirected to a different sink.
 */
private[storage] class RedirectableOutputStream extends OutputStream {
  private[this] var os: OutputStream = _
  def setOutputStream(s: OutputStream): Unit = { os = s }
  override def write(b: Int): Unit = os.write(b)
  override def write(b: Array[Byte]): Unit = os.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
  override def flush(): Unit = os.flush()
  override def close(): Unit = os.close()
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsBytes()]] call.
 *
 * @param memoryStore the MemoryStore, used for freeing memory.
 * @param serializerManager the SerializerManager, used for deserializing values.
 * @param blockId the block id.
 * @param serializationStream a serialization stream which writes to [[redirectableOutputStream]].
 * @param redirectableOutputStream an OutputStream which can be redirected to a different sink.
 * @param unrollMemory the amount of unroll memory used by the values in `unrolled`.
 * @param memoryMode whether the unroll memory is on- or off-heap
 * @param bbos byte buffer output stream containing the partially-serialized values.
 *                     [[redirectableOutputStream]] initially points to this output stream.
 * @param rest         the rest of the original iterator passed to
 *                     [[MemoryStore.putIteratorAsValues()]].
 * @param classTag the [[ClassTag]] for the block.
 */
private[storage] class PartiallySerializedBlock[T](
    memoryStore: MemoryStore,
    serializerManager: SerializerManager,
    blockId: BlockId,
    private val serializationStream: SerializationStream,
    private val redirectableOutputStream: RedirectableOutputStream,
    val unrollMemory: Long,
    memoryMode: MemoryMode,
    bbos: ChunkedByteBufferOutputStream,
    rest: Iterator[T],
    classTag: ClassTag[T]) {

  private lazy val unrolledBuffer: ChunkedByteBuffer = {
    bbos.close()
    bbos.toChunkedByteBuffer
  }

  // If the task does not fully consume `valuesIterator` or otherwise fails to consume or dispose of
  // this PartiallySerializedBlock then we risk leaking of direct buffers, so we use a task
  // completion listener here in order to ensure that `unrolled.dispose()` is called at least once.
  // The dispose() method is idempotent, so it's safe to call it unconditionally.
  Option(TaskContext.get()).foreach { taskContext =>
    taskContext.addTaskCompletionListener[Unit] { _ =>
      // When a task completes, its unroll memory will automatically be freed. Thus we do not call
      // releaseUnrollMemoryForThisTask() here because we want to avoid double-freeing.
      unrolledBuffer.dispose()
    }
  }

  // Exposed for testing
  private[storage] def getUnrolledChunkedByteBuffer: ChunkedByteBuffer = unrolledBuffer

  private[this] var discarded = false
  private[this] var consumed = false

  private def verifyNotConsumedAndNotDiscarded(): Unit = {
    if (consumed) {
      throw new IllegalStateException(
        "Can only call one of finishWritingToStream() or valuesIterator() and can only call once.")
    }
    if (discarded) {
      throw new IllegalStateException("Cannot call methods on a discarded PartiallySerializedBlock")
    }
  }

  /**
   * Called to dispose of this block and free its memory.
   */
  def discard(): Unit = {
    if (!discarded) {
      try {
        // We want to close the output stream in order to free any resources associated with the
        // serializer itself (such as Kryo's internal buffers). close() might cause data to be
        // written, so redirect the output stream to discard that data.
        redirectableOutputStream.setOutputStream(ByteStreams.nullOutputStream())
        serializationStream.close()
      } finally {
        discarded = true
        unrolledBuffer.dispose()
        memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
      }
    }
  }

  /**
   * Finish writing this block to the given output stream by first writing the serialized values
   * and then serializing the values from the original input iterator.
   */
  def finishWritingToStream(os: OutputStream): Unit = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    ByteStreams.copy(unrolledBuffer.toInputStream(dispose = true), os)
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    redirectableOutputStream.setOutputStream(os)
    while (rest.hasNext) {
      serializationStream.writeObject(rest.next())(classTag)
    }
    serializationStream.close()
  }

  /**
   * Returns an iterator over the values in this block by first deserializing the serialized
   * values and then consuming the rest of the original input iterator.
   *
   * If the caller does not plan to fully consume the resulting iterator then they must call
   * `close()` on it to free its resources.
   */
  def valuesIterator: PartiallyUnrolledIterator[T] = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // Close the serialization stream so that the serializer's internal buffers are freed and any
    // "end-of-stream" markers can be written out so that `unrolled` is a valid serialized stream.
    serializationStream.close()
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    val unrolledIter = serializerManager.dataDeserializeStream(
      blockId, unrolledBuffer.toInputStream(dispose = true))(classTag)
    // The unroll memory will be freed once `unrolledIter` is fully consumed in
    // PartiallyUnrolledIterator. If the iterator is not consumed by the end of the task then any
    // extra unroll memory will automatically be freed by a `finally` block in `Task`.
    new PartiallyUnrolledIterator(
      memoryStore,
      memoryMode,
      unrollMemory,
      unrolled = unrolledIter,
      rest = rest)
  }
}
