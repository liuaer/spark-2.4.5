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

package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore

/**
 * Performs bookkeeping for managing an adjustable-size pool of memory that is used for storage
 * (caching).
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode memoryMode :内存模式（MemoryMode)。由此可以断定用于存储的内存池包括[[堆内存的内存池]]和
  *                   堆外内存的内存池。StorageMemoryPool的堆外内存和堆内存都是逻辑上的区分，前者是使用
  *                   sun.misc.Unsafe的API分配的系统内存，后者是系统分配给JVM堆内存的一部分。
 */
private[memory] class StorageMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap storage"
    case MemoryMode.OFF_HEAP => "off-heap storage"
  }

  @GuardedBy("lock")
  private[this] var _memoryUsed: Long = 0L

  override def memoryUsed: Long = lock.synchronized {
    _memoryUsed
  }

  private var _memoryStore: MemoryStore = _
  def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalStateException("memory store not initialized yet")
    }
    _memoryStore
  }

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
    *   获取N个字节的内存来缓存给定的块，如果有必要，则清除现有的块。<br>
    *   根据代码清单6-36,此方法首先计算要申请的内存大小numBytes与空闲空间 memoryFree的差值，
    *   如果numBytesToFree大于0，则说明需要腾出部分Block所占用的 内存空间。然后调用重载的
    *   acquireMemory方法申请获得内存。重载的acquireMemory方 法（见代码清单6-37 )用于
    *   给Blockld对应的Block获取Block所需大小（即numBytes-ToAcquire)的内存。
    *   当StorageMemoryPool内存不足时，还需要腾出其他Block占用的内存 给当前Block，
    *   腾出的大小由numBytesToFree属性指定。<br>
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireMemory(blockId: BlockId, numBytes: Long): Boolean = lock.synchronized {
    val numBytesToFree = math.max(0, numBytes - memoryFree)
    acquireMemory(blockId, numBytes, numBytesToFree)
  }

  /**
   *  1)调用 MemoryStore 的 evictBlocksToFreeSpace方法，腾出 numBytesToFree属性指定大小的空间。
    * 2) 判断内存是否充足（即numBytesToAcquire是否小于等于memoryFree)。
    * 3 )如果内存充足，则将numBytesToAcquire增加到_memoryUsed (即逻辑上获得了用 于存储Block的内存空间)。
    * 4) 返回布尔值enoughMemory,即是否成功获得了用于存储Block的内存空间。
   *
   * @param blockId the ID of the block we are acquiring storage memory for
   * @param numBytesToAcquire the size of this block
   * @param numBytesToFree the amount of space to be freed through evicting blocks
   * @return whether all N bytes were successfully granted.
   */
  def acquireMemory(
      blockId: BlockId,
      numBytesToAcquire: Long,
      numBytesToFree: Long): Boolean = lock.synchronized {
    assert(numBytesToAcquire >= 0)
    assert(numBytesToFree >= 0)
    assert(memoryUsed <= poolSize)
    if (numBytesToFree > 0) {
      //腾出 numBytesToFree属性指定大小的空间
      memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree, memoryMode)
    }
    // NOTE: If the memory store evicts blocks, then those evictions will synchronously call
    // back into this StorageMemoryPool in order to free memory. Therefore, these variables
    // should have been updated.
    //注意:如果内存存储清除blocks，那么这些blocks将同步调用因此,这些变量
    //必然已经更新了。
    val enoughMemory = numBytesToAcquire <= memoryFree
    if (enoughMemory) {
      _memoryUsed += numBytesToAcquire
    }
    // 返回布尔值enoughMemory,即是否成功获得了用于存储Block的内存空间。
    enoughMemory
  }

  def releaseMemory(size: Long): Unit = lock.synchronized {
    if (size > _memoryUsed) {
      logWarning(s"Attempted to release $size bytes of storage " +
        s"memory when we only have ${_memoryUsed} bytes")
      _memoryUsed = 0
    } else {
      _memoryUsed -= size
    }
  }

  def releaseAllMemory(): Unit = lock.synchronized {
    _memoryUsed = 0
  }

  /**
   * Free space to shrink the size of this storage memory pool by `spaceToFree` bytes.
   * Note: this method doesn't actually reduce the pool size but relies on the caller to do so.
   *
   * @return number of bytes to be removed from the pool's capacity.
   */
  def freeSpaceToShrinkPool(spaceToFree: Long): Long = lock.synchronized {
    val spaceFreedByReleasingUnusedMemory = math.min(spaceToFree, memoryFree)
    val remainingSpaceToFree = spaceToFree - spaceFreedByReleasingUnusedMemory
    if (remainingSpaceToFree > 0) {
      // If reclaiming free memory did not adequately shrink the pool, begin evicting blocks:
      // 如果回收的空闲内存没有充分缩小池，开始驱逐块:
      val spaceFreedByEviction =
        memoryStore.evictBlocksToFreeSpace(None, remainingSpaceToFree, memoryMode)
      // When a block is released, BlockManager.dropFromMemory() calls releaseMemory(), so we do
      // not need to decrement _memoryUsed here. However, we do need to decrement the pool size.
      spaceFreedByReleasingUnusedMemory + spaceFreedByEviction
    } else {
      spaceFreedByReleasingUnusedMemory
    }
  }
}
