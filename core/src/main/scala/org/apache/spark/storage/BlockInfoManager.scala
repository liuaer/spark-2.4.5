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

package org.apache.spark.storage

import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

import com.google.common.collect.{ConcurrentHashMultiset, ImmutableMultiset}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging


/**
   BlockInfo用于描述块的元数据信息,包括存储级别、 Block类型、大小、锁信息等。<br>
   Block类型 ：.......
 *
 * @param level the block's storage level. This is the requested persistence level, not the
 *              effective storage level of the block (i.e. if this is MEMORY_AND_DISK, then this
 *              does not imply that the block is actually resident in memory).
 * @param classTag the block's [[ClassTag]], used to select the serializer
 * @param tellMaster whether state changes for this block should be reported to the master. This
 *                   is true for most blocks, but is false for broadcast blocks.
 */
private[storage] class BlockInfo(
    val level: StorageLevel,
    val classTag: ClassTag[_], // block的类型有哪些
    val tellMaster: Boolean) {

  /**
   * The size of the block (in bytes)
   */
  def size: Long = _size
  def size_=(s: Long): Unit = {
    _size = s
    checkInvariants()
  }
  private[this] var _size: Long = 0

  /**
   * Block被锁定读取的次数
   */
  def readerCount: Int = _readerCount
  def readerCount_=(c: Int): Unit = {
    _readerCount = c
    checkInvariants()
  }
  private[this] var _readerCount: Int = 0

  /**
    * 当前占用该写锁的任务尝试的ID<br>
    * 任务尝试在对 Block进行写操作前,首先必须获得对应 BlockInfo的
    * 写锁。 writerTask用于保存任务尝试的ID(每个任务在实际执行时,会多次尝试,
    * 每次尝试都会分配一个ID)。
    * <br><br>
   * The task attempt id of the task which currently holds the write lock for this block, or
   * [[BlockInfo.NON_TASK_WRITER]] if the write lock is held by non-task code, or
   * [[BlockInfo.NO_WRITER]] if this block is not locked for writing.
   */
  def writerTask: Long = _writerTask
  def writerTask_=(t: Long): Unit = {
    _writerTask = t
    checkInvariants()
  }
  private[this] var _writerTask: Long = BlockInfo.NO_WRITER

  private def checkInvariants(): Unit = {
    // A block's reader count must be non-negative:
    assert(_readerCount >= 0)
    // A block is either locked for reading or for writing, but not for both at the same time:
    assert(_readerCount == 0 || _writerTask == BlockInfo.NO_WRITER)
  }

  checkInvariants()
}

private[storage] object BlockInfo {

  /**
    * 用于将块的写锁标记为解锁的特殊任务尝试id常量<br>
   * Special task attempt id constant used to mark a block's write lock as being unlocked.
   */
  val NO_WRITER: Long = -1

  /**
    * 特殊的任务尝试id来标记一个 block的写锁，这个写的线程是 a driver thread or by unit test code
   * Special task attempt id constant used to mark a block's write lock as being held by
   * a non-task thread (e.g. by a driver thread or by unit test code).
   */
  val NON_TASK_WRITER: Long = -1024
}

/**
  *
  * BlockInfoManager是 BlockManager内部的子组件之一, BlockInfoManager对 Block的
  * 锁管理采用了共享锁与排他锁,其中读锁是共享锁,写锁是排他锁。
  *
  * <br><br>
  * Component of the [[BlockManager]] which tracks metadata for blocks and manages block locking.
  *
  * The locking interface exposed by this class is readers-writer lock. Every lock acquisition is
  * automatically associated with a running task and locks are automatically released upon task
  * completion or failure.
  *
  * This class is thread-safe.
 */
private[storage] class BlockInfoManager extends Logging {

  private type TaskAttemptId = Long

  /**
    * BlockId与 BlockInfo之间映射关系的缓存，所有获取了读锁、写锁的 block都将信息存于infos<br><br>
    *
   * Used to look up metadata for individual blocks. Entries are added to this map via an atomic
   * set-if-not-exists operation ([[lockNewBlockForWriting()]]) and are removed
   * by [[removeBlock()]].
   */
  @GuardedBy("this")
  private[this] val infos = new mutable.HashMap[BlockId, BlockInfo]

  /**
    * 每次任务执行尝试的标识TaskAttemptId与执行获取的 Block的写锁之间的映射关系。
    * TaskAttemptEd与写锁之间是一对多的关系,即一次任务尝试执行会获取零到多个Block的写锁
    *
    * 混入特质，value 可以放集合，可以放单个元素
   */
  @GuardedBy("this")
  private[this] val writeLocksByTask =
    new mutable.HashMap[TaskAttemptId, mutable.Set[BlockId]]
      with mutable.MultiMap[TaskAttemptId, BlockId]

  /**
    * HashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]]<br><br>
   * Tracks the set of blocks that each task has locked for reading, along with the number of times
   * that a block has been locked (since our read locks are re-entrant).
   */
  @GuardedBy("this")
  private[this] val readLocksByTask =
    new mutable.HashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]]

  // ----------------------------------------------------------------------------------------------

  // Initialization for special task attempt ids:
  registerTask(BlockInfo.NON_TASK_WRITER)

  // ----------------------------------------------------------------------------------------------

  /**
   * Called at the start of a task in order to register that task with this [[BlockInfoManager]].
   * This must be called prior to calling any other BlockInfoManager methods from that task.
   */
  def registerTask(taskAttemptId: TaskAttemptId): Unit = synchronized {
    require(!readLocksByTask.contains(taskAttemptId),
      s"Task attempt $taskAttemptId is already registered")
    readLocksByTask(taskAttemptId) = ConcurrentHashMultiset.create()
  }

  /**
    * 获取任务上下文TaskContext中当前正在执行的任务尝试的TaskAttemptld。
    * 如果任务上下文 TaskContext中没有任务尝试的TaskAttemptId,那么返回
    * BlockInfo.NON_TASK_WRITER <br>
    * Returns the current task's task attempt id (which uniquely identifies the task), or
    * [[BlockInfo.NON_TASK_WRITER]] if called by a non-task thread.
   */
  private def currentTaskAttemptId: TaskAttemptId = {
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(BlockInfo.NON_TASK_WRITER)
  }

  /**
    *
    * 锁定一个读块并返回它的元数据。<br>
    *
    * 如果另一个任务已经锁定了这个读块，那么读锁将是立即授予调用任务，并增加其锁计数。<br>
    *
    * 如果另一个任务已锁定此块用于写入，则此调用将阻塞，直到写入为止锁被释放或将 blocking = false。<br>
    *
    * 单个任务可以多次锁定一个块进行读取，在这种情况下，需要分别释放每个锁。<br>
    *
   * Lock a block for reading and return its metadata.
   *
   * If another task has already locked this block for reading, then the read lock will be
   * immediately granted to the calling task and its lock count will be incremented.
   *
   * If another task has locked this block for writing, then this call will block until the write
   * lock is released or will return immediately if `blocking = false`.
   *
   * A single task can lock a block multiple times for reading, in which case each lock will need
   * to be released separately.
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for reading).
   */
  def lockForReading(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire read lock for $blockId")
    do {
      infos.get(blockId) match {
        case None => return None
        case Some(info) =>
          if (info.writerTask == BlockInfo.NO_WRITER) {
            info.readerCount += 1
            readLocksByTask(currentTaskAttemptId).add(blockId)
            logTrace(s"Task $currentTaskAttemptId acquired read lock for $blockId")
            return Some(info)
          }
      }
      if (blocking) {
        wait()
      }
    } while (blocking)
    None
  }

  /**
   * Lock a block for writing and return its metadata.
   *
   * If another task has already locked this block for either reading or writing, then this call
   * will block until the other locks are released or will return immediately if `blocking = false`.
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for writing).
   */
  def lockForWriting(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire write lock for $blockId")
    do {
      infos.get(blockId) match {
        case None => return None
        case Some(info) =>
          if (info.writerTask == BlockInfo.NO_WRITER && info.readerCount == 0) {
            info.writerTask = currentTaskAttemptId
            writeLocksByTask.addBinding(currentTaskAttemptId, blockId)
            logTrace(s"Task $currentTaskAttemptId acquired write lock for $blockId")
            return Some(info)
          }
      }
      if (blocking) {
        wait()
      }
    } while (blocking)
    None
  }

  /**
   * Throws an exception if the current task does not hold a write lock on the given block.
   * Otherwise, returns the block's BlockInfo.
   */
  def assertBlockIsLockedForWriting(blockId: BlockId): BlockInfo = synchronized {
    infos.get(blockId) match {
      case Some(info) =>
        if (info.writerTask != currentTaskAttemptId) {
          throw new SparkException(
            s"Task $currentTaskAttemptId has not locked block $blockId for writing")
        } else {
          info
        }
      case None =>
        throw new SparkException(s"Block $blockId does not exist")
    }
  }

  /**
   * Get a block's metadata without acquiring any locks. This method is only exposed for use by
   * [[BlockManager.getStatus()]] and should not be called by other code outside of this class.
   */
  private[storage] def get(blockId: BlockId): Option[BlockInfo] = synchronized {
    infos.get(blockId)
  }

  /**
    * 将排他锁降级为可共享的读锁
   */
  def downgradeLock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId downgrading write lock for $blockId")
    // 获取blockId的blockInfo
    val info = get(blockId).get
    /*
      info.writerTask : blockId对应的写锁的尝试线程ID
      currentTaskAttemptId ：获取任务上下文TaskContext中当前正在执行的任务尝试的TaskAttemptld。
     */
    require(info.writerTask == currentTaskAttemptId,
      s"Task $currentTaskAttemptId tried to downgrade a write lock that it does not hold on" +
        s" block $blockId")
    unlock(blockId)
    val lockOutcome = lockForReading(blockId, blocking = false)
    assert(lockOutcome.isDefined)
  }

  /**
    *释放blockId对应的block上的锁<br>
   * Release a lock on the given block.
   * In case a TaskContext is not propagated properly to all child threads for the task, we fail to
   * get the TID from TaskContext, so we have to explicitly pass the TID value to release the lock.
   *
   * See SPARK-18406 for more discussion of this issue.
   */
  def unlock(blockId: BlockId, taskAttemptId: Option[TaskAttemptId] = None): Unit = synchronized {
    val taskId = taskAttemptId.getOrElse(currentTaskAttemptId)
    logTrace(s"Task $taskId releasing lock for $blockId")
    // 所有获取了读锁、写锁的 block都将信息存于infos
    // 首先判断，有没有锁，没锁就报错
    val info = get(blockId).getOrElse {
      throw new IllegalStateException(s"Block $blockId not found")
    }
    // blockid对应的block是否有写锁
    if (info.writerTask != BlockInfo.NO_WRITER) {
      info.writerTask = BlockInfo.NO_WRITER
      writeLocksByTask.removeBinding(taskId, blockId)
    } else {
      // 必然是读锁
      assert(info.readerCount > 0, s"Block $blockId is not locked for reading")
      info.readerCount -= 1
      val countsForTask = readLocksByTask(taskId)
      val newPinCountForTask: Int = countsForTask.remove(blockId, 1) - 1
      assert(newPinCountForTask >= 0,
        s"Task $taskId release lock on block $blockId more times than it acquired it")
    }
    // 通知所有正在等待该 block的线程
    notifyAll()
  }

  /**
     <p>
      试图为写入一个block获取一个合适的锁，如果我们是第一个写这个block,则直接使用，并获取锁，其他的需要等
      如果不能获取写锁会返回一个读锁
    <p/>
   * Attempt to acquire the appropriate lock for writing a new block.
   *
   * This enforces the first-writer-wins semantics. If we are the first to write the block,
   * then just go ahead and acquire the write lock. Otherwise, if another thread is already
   * writing the block, then we wait for the write to finish before acquiring the read lock.
   *
   * @return true if the block did not already exist, false otherwise. If this returns false, then
   *         a read lock on the existing block will be held. If this returns true, a write lock on
   *         the new block will be held.
   */
  def lockNewBlockForWriting(
      blockId: BlockId,
      newBlockInfo: BlockInfo): Boolean = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to put $blockId")
    lockForReading(blockId) match {
      case Some(info) =>
        // Block already exists. This could happen if another thread races with us to compute
        // the same block. In this case, just keep the read lock and return.
        false
      case None =>
        // Block does not yet exist or is removed, so we are free to acquire the write lock
        infos(blockId) = newBlockInfo
        lockForWriting(blockId)
        true
    }
  }

  /**
   * Release all lock held by the given task, clearing that task's pin bookkeeping
   * structures and updating the global pin counts. This method should be called at the
   * end of a task (either by a task completion handler or in `TaskRunner.run()`).
   *
   * @return the ids of blocks whose pins were released
   */
  def releaseAllLocksForTask(taskAttemptId: TaskAttemptId): Seq[BlockId] = synchronized {
    val blocksWithReleasedLocks = mutable.ArrayBuffer[BlockId]()

    val readLocks = readLocksByTask.remove(taskAttemptId).getOrElse(ImmutableMultiset.of[BlockId]())
    val writeLocks = writeLocksByTask.remove(taskAttemptId).getOrElse(Seq.empty)

    for (blockId <- writeLocks) {
      infos.get(blockId).foreach { info =>
        assert(info.writerTask == taskAttemptId)
        info.writerTask = BlockInfo.NO_WRITER
      }
      blocksWithReleasedLocks += blockId
    }

    readLocks.entrySet().iterator().asScala.foreach { entry =>
      val blockId = entry.getElement
      val lockCount = entry.getCount
      blocksWithReleasedLocks += blockId
      get(blockId).foreach { info =>
        info.readerCount -= lockCount
        assert(info.readerCount >= 0)
      }
    }

    notifyAll()

    blocksWithReleasedLocks
  }

  /** Returns the number of locks held by the given task.  Used only for testing. */
  private[storage] def getTaskLockCount(taskAttemptId: TaskAttemptId): Int = {
    readLocksByTask.get(taskAttemptId).map(_.size()).getOrElse(0) +
      writeLocksByTask.get(taskAttemptId).map(_.size).getOrElse(0)
  }

  /**
   * Returns the number of blocks tracked.
   */
  def size: Int = synchronized {
    infos.size
  }

  /**
   * Return the number of map entries in this pin counter's internal data structures.
   * This is used in unit tests in order to detect memory leaks.
   */
  private[storage] def getNumberOfMapEntries: Long = synchronized {
    size +
      readLocksByTask.size +
      readLocksByTask.map(_._2.size()).sum +
      writeLocksByTask.size +
      writeLocksByTask.map(_._2.size).sum
  }

  /**
   * Returns an iterator over a snapshot of all blocks' metadata. Note that the individual entries
   * in this iterator are mutable and thus may reflect blocks that are deleted while the iterator
   * is being traversed.
   */
  def entries: Iterator[(BlockId, BlockInfo)] = synchronized {
    infos.toArray.toIterator
  }

  /**
   * Removes the given block and releases the write lock on it.
   *
   * This can only be called while holding a write lock on the given block.
   */
  def removeBlock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to remove block $blockId")
    infos.get(blockId) match {
      case Some(blockInfo) =>
        if (blockInfo.writerTask != currentTaskAttemptId) {
          throw new IllegalStateException(
            s"Task $currentTaskAttemptId called remove() on block $blockId without a write lock")
        } else {
          infos.remove(blockId)
          blockInfo.readerCount = 0
          blockInfo.writerTask = BlockInfo.NO_WRITER
          writeLocksByTask.removeBinding(currentTaskAttemptId, blockId)
        }
      case None =>
        throw new IllegalArgumentException(
          s"Task $currentTaskAttemptId called remove() on non-existent block $blockId")
    }
    notifyAll()
  }

  /**
   * Delete all state. Called during shutdown.
   */
  def clear(): Unit = synchronized {
    infos.valuesIterator.foreach { blockInfo =>
      blockInfo.readerCount = 0
      blockInfo.writerTask = BlockInfo.NO_WRITER
    }
    infos.clear()
    readLocksByTask.clear()
    writeLocksByTask.clear()
    notifyAll()
  }

}
