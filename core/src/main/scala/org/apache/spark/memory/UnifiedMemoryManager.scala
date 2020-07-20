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

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

/**
  * 统一内存管理， 1.6 之后默认的内存管理器<br>
  *
  * 其中最重要的优化在于动态占用机制，其规则如下：
  * 1. 设定基本的存储内存和执行内存区域（spark.storage.storageFraction参数），
  *     该设定确定了双方各自拥有的空间的范围；
  * 2. 双方的空间都不足时，则存储到硬盘；若己方空间不足而对方空余时，可借用对方的空间;
  *   （存储空间不足是指不足以放下一个完整的 Block）
  * 3. 执行内存的空间被对方占用后，可让对方将占用的部分转存到硬盘，然后”归还”借用的空间；
  * 4. 存储内存的空间被对方占用后，无法让对方”归还”，因为需要考虑 Shuffle 过程中的很多因素，实现起来较为复杂。
  * <br>
  *
  * @param onHeapStorageRegionSize Size of the storage region, in bytes.
  *                          This region is not statically reserved; execution can borrow from
  *                          it if necessary. Cached blocks can be evicted only if actual
  *                          storage memory usage exceeds this region.
 */
private[spark] class UnifiedMemoryManager private[memory] (
    conf: SparkConf,
    val maxHeapMemory: Long,
    onHeapStorageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize) {

  private def assertInvariants(): Unit = {
    assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
    assert(
      offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
  }

  assertInvariants()

  /**
    * 堆内执行内存和堆内存储内存公用 maxHeapMemory<br>
    *  maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
    */
  override def maxOnHeapStorageMemory: Long = synchronized {
    maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
  }
  /**
    * 堆外执行内存和堆外存储内存公用 maxOffHeapMemory<br>
    * maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
    */
  override def maxOffHeapStorageMemory: Long = synchronized {
    maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
  }

  /**
    *
    *
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    /*
       executionPool （堆或堆外，二选一）
       storagePool  （堆或堆外，二选一）
       storageRegionSize 存储区域大小（堆或堆外，二选一）
       maxMemory 内存最大值
     */
    val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        onHeapStorageRegionSize,
        maxHeapMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        offHeapStorageMemory,
        maxOffHeapMemory)
    }

    /**
     *  此函数用于借用或收回存储内存<br>
      *
      * 通过驱逐缓存的块来扩展 execution pool，从而收缩 storage pool。<br>
      *
      * 在为任务获取内存时，执行池可能执行多个尝试。
      * 每次尝试必须能 evict storage，以防另一个任务跳进来并在尝试之间缓存一个大块。每次尝试调用一次。
      *
      * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
     *
     * When acquiring memory for a task, the execution pool may need to make multiple
     * attempts. Each attempt must be able to evict storage in case another task jumps in
     * and caches a large block between the attempts. This is called once per attempt.
     */
    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
      if (extraMemoryNeeded > 0) {
        // There is not enough free memory in the execution pool, so try to reclaim memory from
        // storage. We can reclaim any free memory from the storage pool. If the storage pool
        // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
        // the memory that storage has borrowed from execution.
        /*
            //execution pool 中没有足够的空闲内存，因此尝试利用 storage 。我们可以从storage pool中申请任何可用内存。
            如果storage pool已经变得比存储区域最大值更大了，我们可以驱逐块并回收存储从执行中借用的内存。
         */
        /*
           max(内存池的空闲大小, 当前内存池的大小【动态】 - 存储区域大小【固定】)
           如何理解？
           当前内存池的大小 > 存储区域大小【固定】时 ，memoryReclaimableFromStorage = 超出的部分
           当前内存池的大小 < 存储区域大小【固定】时 ， storagePool.memoryFree 为正数，后者为负数

           推论：只能利用两类内存 1、存储空闲的内存 2、存储借用的内存
                没有从 numBytes 的需求方面考虑来分配大小，为什么不驱逐存储直到申请的得到满足，IO可能更耗费时间
         */
       /*
              小贴士：根据maybeGrowExecutionPool的实现， 如果存储内存池的空闲空间大于存储
          内存池从执行内存池借用的空间大小，那么除了回收被借用的空间外，还会向存储池再借
          用一些空间【storagePool.memoryFree】；如果存储池的空闲空间小于等于存储池从执行池借用的空间大小，
          那么只需要回收被借用的空间【storagePool.poolSize - storageRegionSize】。
       */
        val memoryReclaimableFromStorage = math.max(
          storagePool.memoryFree,
          storagePool.poolSize - storageRegionSize)

        if (memoryReclaimableFromStorage > 0) {
          // Only reclaim as much space as is necessary and available:
          /*
             min(extraMemoryNeeded, memoryReclaimableFromStorage) 理解
             多了只能给memoryReclaimableFromStorage，少了就如你所愿 extraMemoryNeeded
           */
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
            math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
          storagePool.decrementPoolSize(spaceToReclaim)
          executionPool.incrementPoolSize(spaceToReclaim)
        }
      }
    }

    /**
     * The size the execution pool would have after evicting storage memory.
     *
     * The execution memory pool divides this quantity among the active tasks evenly to cap
     * the execution memory allocation for each task. It is important to keep this greater
     * than the execution pool size, which doesn't take into account potential memory that
     * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
     *
     * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
     * in execution memory allocation across tasks, Otherwise, a task may occupy more than
     * its fair share of execution memory, mistakenly thinking that other tasks can acquire
     * the portion of storage memory that cannot be evicted.
     */
    /*
        执行池在驱逐存储内存后的大小
        区分 与 maxMemory - storageRegionSize 的区别

        通俗说：如果你有多余的，那我最大可拥有本身分配给我的+你空闲的部分
               否则我最大可拥有本身分配给我的
     */
    def computeMaxExecutionPoolSize(): Long = {
      maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)
    }

    executionPool.acquireMemory(
      numBytes, taskAttemptId, maybeGrowExecutionPool, () => computeMaxExecutionPoolSize)
  }

  /**
    * 为存储 blockId 对应的 block ,从堆内存和堆外内存所获取的 numBytes 大小<br>
    * 1、根据内存模式，获取此内存模式的计算内存池、存储内存池和可以存储的最大空间。<br>
    * 2、对要获得的存储大小进行校验，即mimBytes不能大于可以存储的最大空间。<br>
    * 3、如果要获得的存储大小比存储内存池的空闲空间要大，那么就到计算内存池中去借 用空间。<br>
    *    借用的空间取mimBytes和计算内存池的空闲空 间的最小值。
    * 4、从存储内存池获得存储Blockld对应的Block所需的空间。<br>
    */
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    // 根据内存模式，获取此内存模式的计算内存池、存储内存池和可以存储的最大空间
    val (executionPool, storagePool, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        maxOnHeapStorageMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        maxOffHeapStorageMemory)
    }
    // 对要获得的存储大小进行校验，即numBytes不能大于可以存储的最大空间。
    if (numBytes > maxMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxMemory bytes)")
      return false
    }
    // 如果要获得的存储大小比存储内存池的空闲空间要大，那么就到executionPool中去借用空间。<br>
    // 借用的空间取numBytes和计算内存池的空闲空间的最小值。
    if (numBytes > storagePool.memoryFree) {
      // There is not enough free memory in the storage pool, so try to borrow free memory from
      // the execution pool.
      val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,
        numBytes - storagePool.memoryFree)
      executionPool.decrementPoolSize(memoryBorrowedFromExecution)
      storagePool.incrementPoolSize(memoryBorrowedFromExecution)
    }
    // 从存储内存池获得存储Blockld对应的Block所需的空间
    // executionPool 能释放的都给了，不够自己想办法
    // acquireMemory 会驱逐块
    storagePool.acquireMemory(blockId, numBytes)
  }

  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, memoryMode)
  }
}

object UnifiedMemoryManager {

  // Set aside a fixed amount of memory for non-storage, non-execution purposes.
  // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
  // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
  // the memory used for execution and storage will be (1024 - 300) * 0.6 = 434MB by default.
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    val maxMemory = getMaxMemory(conf)
    new UnifiedMemoryManager(
      conf,
      maxHeapMemory = maxMemory,
      onHeapStorageRegionSize =
        (maxMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong,
      numCores = numCores)
  }

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
      if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
        s"option or spark.driver.memory in Spark configuration.")
    }
    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    if (conf.contains("spark.executor.memory")) {
      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
      if (executorMemory < minSystemMemory) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$minSystemMemory. Please increase executor memory using the " +
          s"--executor-memory option or spark.executor.memory in Spark configuration.")
      }
    }
    val usableMemory = systemMemory - reservedMemory
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.6)
    (usableMemory * memoryFraction).toLong
  }
}
