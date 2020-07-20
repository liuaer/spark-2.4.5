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

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * Implements policies and bookkeeping for sharing an adjustable-sized pool of memory between tasks.
 *
 * Tries to ensure that each task gets a reasonable share of memory, instead of some task ramping up
 * to a large amount first and then causing others to spill to disk repeatedly.
 *
 * If there are N tasks, it ensures that each task can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever this
 * set changes. This is all done by synchronizing access to mutable state and using wait() and
 * notifyAll() to signal changes to callers. Prior to Spark 1.6, this arbitration of memory across
 * tasks was performed by the ShuffleMemoryManager.
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 */
private[memory] class ExecutionMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap execution"
    case MemoryMode.OFF_HEAP => "off-heap execution"
  }

  /**
   * Map from taskAttemptId -> memory consumption in bytes
   */
  @GuardedBy("lock")
  private val memoryForTask = new mutable.HashMap[Long, Long]()

  override def memoryUsed: Long = lock.synchronized {
    memoryForTask.values.sum
  }

  /**
   * Returns the memory consumption, in bytes, for the given task.
   */
  def getMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    memoryForTask.getOrElse(taskAttemptId, 0L)
  }

  /**
   * Try to acquire up to `numBytes` of memory for the given task and return the number of bytes
   * obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * @param numBytes number of bytes to acquire
   * @param taskAttemptId the task attempt acquiring memory
   * @param maybeGrowPool a callback that potentially grows the size of this pool. It takes in
   *                      one parameter (Long) that represents the desired amount of memory by
   *                      which this pool should be expanded.
   * @param computeMaxPoolSize a callback that returns the maximum allowable size of this pool
   *                           at this given moment. This is not a field because the max pool
   *                           size is variable in certain cases. For instance, in unified
   *                           memory management, the execution pool can be expanded by evicting
   *                           cached blocks, thereby shrinking the storage pool.
   *
   * @return the number of bytes granted to the task.
   */
  private[memory] def acquireMemory(
      numBytes: Long, // 要获取的内存大小
      taskAttemptId: Long, // 要获取内存的taskAttempt
      maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => Unit,
      computeMaxPoolSize: () => Long = () => poolSize): Long = lock.synchronized {
    assert(numBytes > 0, s"invalid number of bytes requested: $numBytes")

    // TODO: clean up this clunky method signature

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
    /*
       如果 memoryForTask 中还没有记录taskAttemptId， 则将taskAttemptId放入memory, 唤醒其他等待获取
       ExecutionMemoryPool 的线程
     */
    if (!memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      lock.notifyAll()
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      val numActiveTasks = memoryForTask.keys.size
      // 该任务尝试当前的内存
      val curMem = memoryForTask(taskAttemptId)

      // In every iteration of this loop, we should first try to reclaim any borrowed execution
      // space from storage. This is necessary because of the potential race condition where new
      // storage blocks may steal the free execution memory that this task was waiting for.
      /*
       maybeGrowPool

       调用函数maybeGrowPool以回收 StorageMemoryPool从当前ExecutionMemoryPool借用的内存，
       而且在StorageMemoryPool还有空闲空间时，从StorageMemoryPool借用内存， 这可能导致
       StorageMemoryPool中的一些Block被驱逐出去。

       在这个循环的每次迭代中，我们应该首先尝试回收借给 storage 的执行存储空间, This is necessary
       because of the potential race condition where new storage blocks may steal the free
       execution memory that this task was waiting for.

       numBytes - memoryFree = 要获取的内存大小 - 获取内存池的空闲大小

       有空闲的话，就不会增长
     */
      maybeGrowPool(numBytes - memoryFree)

      // Maximum size the pool would have after potentially growing the pool.
      // This is used to compute the upper bound of how much memory each task can occupy. This
      // must take into account potential free memory as well as the amount this pool currently
      // occupies. Otherwise, we may run into SPARK-12155 where, in unified memory management,
      // we did not take into account space that could have been freed by evicting cached blocks.

      //  计算内存池的最大大小
      val maxPoolSize = computeMaxPoolSize()
      //  计算每个任务尝试最大可以使用的内存大小(即 maxMemoryPerTask )
      val maxMemoryPerTask = maxPoolSize / numActiveTasks
      //  计算每个任务尝试最小保证使用的内存大小(即 minMemoryPerTask)
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)

      // How much we can grant this task; keep its share within 0 <= X <= 1 / numActiveTasks

      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))
      // Only give it as much memory as is free, which might be none if it reached 1 / numTasks

      // 计算当前任务尝试真正可以申请获取的内存大小(即 toGrant)
      val toGrant = math.min(maxToGrant, memoryFree)

      // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
      // if we can't give it this much now, wait for other tasks to free up memory
      // (this happens if older tasks allocated lots of memory before N grew)
      /*
         如果toGrant小于numBytes(即真正可以申请获取的内存超出了期望获得的内
         toGrant的和小于minMemoryPerTask， 则使得当前线程处于等待状态。这说明如果任务尝
         试要申请的内存得不到满足，甚至连每个任务尝试最基本的内存大小都得不到满足，则需
         要等待其他任务尝试释放内存。当其他任务尝试释放内存后，会进入下一次循环，直到获
         取到满意的内存大小。
       */
      /*
           当前任务尝试真正可以申请获取的内存 toGrant < 期望申请的内存 numBytes
           该任务尝试当前的内存  curMem + 当前任务尝试真正可以申请获取的内存 toGrant <
           每个任务尝试最小保证使用的内存大小 minMemoryPerTask 则使得当前线程处于等待状态。

           这说明如果任务尝试要申请的内存得不到满足，甚至连每个任务尝试最基本的内存大小都得不到满足，
           则需要等待其他任务尝试释放内存。当其他任务尝试释放内存后，会进入下一次循环，直到获取到满意的内存大小。
       */
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free")
        lock.wait()
      } else {
        memoryForTask(taskAttemptId) += toGrant
        return toGrant
      }
    }
    0L  // Never reached
    /*
         小贴士：以上对acquireMemory方法的步骤拆解， 读者可能并不容易理解。这里对它
         的处理逻辑作个简单总结：假设当前有N个线程，必须保证每个线程在溢出之前至少获得
         1/2N的内存，并且每个线程最多获得1/N的内存。由于N是动态变化的变量，所以要持续
         对这些线程进行跟踪，以便于线程的数量发生变化时，重新按照1/2N和1/N计算。
     */
  }

  /**
   * Release `numBytes` of memory acquired by the given task.
   */
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)
    var memoryToFree = if (curMem < numBytes) {
      logWarning(
        s"Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
          s"of memory from the $poolName pool")
      curMem
    } else {
      numBytes
    }
    if (memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) -= memoryToFree
      // 如果  taskAttemptId 对应的内存释放完毕，则移除
      if (memoryForTask(taskAttemptId) <= 0) {
        memoryForTask.remove(taskAttemptId)
      }
    }
    lock.notifyAll() // Notify waiters in acquireMemory() that memory has been freed
  }

  /**
    * 移除 taskAttemptId 所有的内存
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
   */
  def releaseAllMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    val numBytesToFree = getMemoryUsageForTask(taskAttemptId)
    releaseMemory(numBytesToFree, taskAttemptId)
    numBytesToFree
  }

}
