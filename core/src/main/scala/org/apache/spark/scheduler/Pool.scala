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

package org.apache.spark.scheduler

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * A Schedulable entity that represents collection of Pools or TaskSetManagers
 */
private[spark] class Pool(
    val poolName: String,
    val schedulingMode: SchedulingMode,
    initMinShare: Int,
    initWeight: Int)
  extends Schedulable with Logging {

  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  /**
    *  ConcurrentHashMap[调度名称, Schedulable]
    */
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  val weight = initWeight
  val minShare = initMinShare
  var runningTasks = 0
  val priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1
  val name = poolName
  var parent: Pool = null

  private val taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
      case _ =>
        val msg = s"Unsupported scheduling mode: $schedulingMode. Use FAIR or FIFO instead."
        throw new IllegalArgumentException(msg)
    }
  }

  override def addSchedulable(schedulable: Schedulable) {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }

  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  /**
    * 用于当某个Executor丢失后，调用当前Pool的 schedulableQueue中的各个Schedulable
    * (可能为子调度池，也可能是TaskSetManager)的 executorLost方法。TaskSetManager的
    * executorLost方法（见代码清单7-74)进而将在此 Executor上正在运行的Task作为失败任务处理，
    * 并重新提交这些任务。<br>
    *
    * 最后执行的是 TaskSetManager覆写的executorLost
    */
  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason) {
    //递归调用
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }

  /**
    checkSpeculatableTasks方法（见代码清单7-52)用于检査当前Pool中是否有需要推断执行的任务。
    checkSpeculatableTasks实际通过迭代调用schedulableQueue中的各个子Schedulable的
    checkSpeculatableTasks方法来实现。
    Pool的checkSpeculatableTasks方法和TaskSetManager的checkSpeculatableTasks方法
   （见代码清单7-62 )，一起实现了按照深度遍历算法从调度池中査找可推断执行的任务。
    */
  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      /*
          &	按位与运算符	(a & b) 输出结果 12 ，二进制解释： 0000 1100
          |	按位或运算符	(a | b) 输出结果 61 ，二进制解释： 0011 1101
          ^	按位异或运算符	(a ^ b) 输出结果 49 ，二进制解释： 0011 0001
          |= ------> 或运算
       */
      shouldRevive |= schedulable.checkSpeculatableTasks(minTimeToSpeculation)
    }
    shouldRevive
  }

  /**
      用于对当前Pool中的所有TaskSetManager按照调度算法进行排序，并返回排序后的TaskSetManager。
      getSortedTaskSetQueue实际是通过迭代调用schedulableQueue中的各个子Schedulable的
      getSortedTaskSetQueue方法来实现。
    */
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }

  /**
    * 用于增加当前Pool及其父Pool中记录的当前正在运行的任务数量。
    */
  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
