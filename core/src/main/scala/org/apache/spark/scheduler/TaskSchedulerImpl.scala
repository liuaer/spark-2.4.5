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

import java.nio.ByteBuffer
import java.util.{Locale, Timer, TimerTask}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.Set
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.rpc.RpcEndpoint
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, SystemClock, ThreadUtils, Utils}

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a `LocalSchedulerBackend` and setting
 * isLocal to true. It handles common logic, like determining a scheduling order across jobs, waking
 * up to launch speculative tasks, etc.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * submitTasks method.
 *
 * THREADING: [[SchedulerBackend]]s and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * [[SchedulerBackend]]s synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
  extends TaskScheduler with Logging {

  import TaskSchedulerImpl._

  def this(sc: SparkContext) = {
    this(sc, sc.conf.get(config.MAX_TASK_FAILURES))
  }

  // Lazily initializing blacklistTrackerOpt to avoid getting empty ExecutorAllocationClient,
  // because ExecutorAllocationClient is created after this TaskSchedulerImpl.
  /**
    * 延迟初始化blacklistTrackerOpt以避免得到空的ExecutorAllocationClient，
    * 因为ExecutorAllocationClient是在这个TaskSchedulerImpl之后创建的。
    */
  private[scheduler] lazy val blacklistTrackerOpt = maybeCreateBlacklistTracker(sc)

  val conf = sc.conf

  // How often to check for speculative tasks
  // 任务推断执行的时间间隔
  val SPECULATION_INTERVAL_MS = conf.getTimeAsMs("spark.speculation.interval", "100ms")

  // Duplicate copies of a task will only be launched if the original copy has been running for
  // at least this amount of time. This is to avoid the overhead of launching speculative copies
  // of tasks that are very short.
  /*
   * 用于保证原始任务至少需要运行的时间。原始任 务只有超过此时间限制，才允许启动副本任务。
   * 这可以避免原始任务执行太短的时间就被推断执行副本任务。MIN_TIME_TO_SPECULATION的
   * 大小固定为100。
   */
  val MIN_TIME_TO_SPECULATION = 100
  /*
    对任务调度进行推断执行的 ScheduledThreadPoolExecutor。
    由 speculationScheduler 创建的线程以 task-scheduler-speculation 为前缀。
   */
  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // Threshold above which we warn user initial TaskSet may be starved
  /*
    判断 TaskSet饥饿的阈值,默认为15s。
   */
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  /**
    *   HashMap[Stageld, HashMap[Attempt, TaskSetManager]] <br>
    *   TaskSetManagers are not thread safe, so any access to one should be synchronized
    *   on this class.
    */
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  // Protected by `this`
  /**
    * ConcurrentHashMap[TaskId, TaskSetManager]
    */
  private[scheduler] val taskIdToTaskSetManager = new ConcurrentHashMap[Long, TaskSetManager]
  /**
    * HashMap[TaskId, String]
    */
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  // 处理饥饿的定时器
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // IDs of the tasks running on each executor
  /**
    *  HashMap[executorId, HashSet[TaskId]] <br>
    *  用于缓存 Executor与运行在此Executor上的任务之间的映射关系， 由此看出一个Executor上
    *  可以运行多个Task。
    */
  private val executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]

  def runningTasksByExecutors: Map[String, Int] = synchronized {
    executorIdToRunningTaskIds.toMap.mapValues(_.size)
  }

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  /**
    * 用于缓存机器的Host与运行在此机器上的Executor之间的映射关系， 由此可以看出机器与
    * Executor之间是一对多的关系。
    */
  protected val hostToExecutors = new HashMap[String, HashSet[String]]
  /**
    * 用于缓存机器所在的机架与机架上机器的Host之间的映射关系，
    * 由此可以看出机架与机器之间是一对多的关系
    */
  protected val hostsByRack = new HashMap[String, HashSet[String]]
  /**
    *
    */
  protected val executorIdToHost = new HashMap[String, String]

  private val abortTimer = new Timer(true)
  private val clock = new SystemClock
  // Exposed for testing
  val unschedulableTaskSetToExpiryTime = new HashMap[TaskSetManager, Long]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null
  /**
    * * SchedulerBackend是 TaskScheduler 的调度后端接口。TaskScheduler给Task分配资源
    * * 实际是通过SchedulerBackend来完成的，SchedulerBackend给Task分配完资源后将与分配给
    * * Task的Executor通信，并要求后者运行Task。
    */
  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
  // 调度次构建器
  private var schedulableBuilder: SchedulableBuilder = null
  // default scheduler is FIFO
  private val schedulingModeConf = conf.get(SCHEDULER_MODE_PROPERTY, SchedulingMode.FIFO.toString)
  val schedulingMode: SchedulingMode =
    try {
      SchedulingMode.withName(schedulingModeConf.toUpperCase(Locale.ROOT))
    } catch {
      case e: java.util.NoSuchElementException =>
        throw new SparkException(s"Unrecognized $SCHEDULER_MODE_PROPERTY: $schedulingModeConf")
    }

  val rootPool: Pool = new Pool("", schedulingMode, 0, 0)

  // This is a var so that we can reset it for testing purposes.
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  private lazy val barrierSyncTimeout = conf.get(config.BARRIER_SYNC_TIMEOUT)

  private[scheduler] var barrierCoordinator: RpcEndpoint = null

  private def maybeInitBarrierCoordinator(): Unit = {
    if (barrierCoordinator == null) {
      barrierCoordinator = new BarrierCoordinator(barrierSyncTimeout, sc.listenerBus,
        sc.env.rpcEnv)
      sc.env.rpcEnv.setupEndpoint("barrierSync", barrierCoordinator)
      logInfo("Registered BarrierCoordinator endpoint")
    }
  }

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  def initialize(backend: SchedulerBackend) {
    this.backend = backend
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported $SCHEDULER_MODE_PROPERTY: " +
          s"$schedulingMode")
      }
    }
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    // 启动后端调度接口
    backend.start()
    /*
      当应用不是在Local模式下，并且设置了推断执行(即spark.speculation属性为true)，
      那么设置一个执行间隔为SPECULATION_INTERVAL_MS(默认为100ms) 的检查可推断任
      务的定时器。此定时器通过调用checkSpeculatableTasks方法(见代码清单7-98)来检查可
      推断任务。
     */
    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          checkSpeculatableTasks()
        }
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  /**
    * 由于Task Scheduler Impl对任务资源的运行依赖于Scheduler Backend，所以为了避免
    * TaskSchedulerImpl在 SchedulerBackend准备就绪之前，就将Task交给SchedulerBackend处理，
    * 因此实现了postStartHook方法用于等待SchedulerBackend准备就绪
    */
  override def postStartHook() {
    waitBackendReady()
  }

  /**
    * DAGScheduler将Stage中各个分区的Task封装为TaskSet后，会将TaskSet交给TaskSchedulerImpl处理。
    * TaskSchedulerImpl的submitTasks方法是这一过程的入口
    *
    */
  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      //  stageTaskSets : HashMap[stageId, TaskSetManager]
      /*
         taskSetsByStageIdAndAttempt 之前可能已经存在该stage，这一次属于重新提交，
         那么需要将之前的 stage对应的tasksetmanager标记为zombie，因为一个阶段不能有
         一个以上的活动任务集管理器
       */
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])

      // Mark all the existing TaskSetManagers of this stage as zombie, as we are adding a new one.
      // This is necessary to handle a corner case. Let's say a stage has 10 partitions and has 2
      // TaskSetManagers: TSM1(zombie) and TSM2(active). TSM1 has a running task for partition 10
      // and it completes. TSM2 finishes tasks for partition 1-9, and thinks he is still active
      // because partition 10 is not completed yet. However, DAGScheduler gets task completion
      // events for all the 10 partitions and thinks the stage is finished. If it's a shuffle stage
      // and somehow it has missing map outputs, then DAGScheduler will resubmit it and create a
      // TSM3 for it. As a stage can't have more than one active task set managers, we must mark
      // TSM2 as zombie (it actually is).
      /*
        将这个阶段的所有现有tasksetmanager标记为zombie，因为我们正在添加一个新的tasksetmanager。
        这是处理紧急情况所必需的。假设一个stage有10个分区，2个TaskSetManagers: TSM1(僵死状态)和
        TSM2(活动状态)。TSM1有一个针对分区10的正在运行的任务它完成。TSM2完成了分区1-9的任务，并且认为
        他仍然处于活动状态因为分区10还没有完成。然而，DAGScheduler获得任务完成所有10个分区的事件，并
        认为该阶段已经完成。如果他是一个shufflestage，它有丢失的map输出，然后DAGScheduler将重新提交它，
        并创建一个TSM3。由于一个阶段不能有一个以上的活动任务集管理器，我们必须标记TSM2是僵尸(它确实是)。
       */
      stageTaskSets.foreach { case (_, ts) =>
        ts.isZombie = true
      }
      // stageTaskSets = HashMap[stageAttemptId, manager]
      stageTaskSets(taskSet.stageAttemptId) = manager
      // 调度池中添加该TaskSetManager
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
      /*
        如果当前应用程序不是Local模式并且TaskSchedulerImpl还没有接收到Task，那
        么设置一个定时器按照STARVATION_TIMEOUT_MS指定的时间间隔检查TaskSchedulerImpl的饥饿状况，
         当TaskSchedulerImpl已经运行Task后，取消此定时器。
       */
      if (!isLocal && !hasReceivedTask) {
        //周期性地执行
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    // 给Task分配资源并运行Task
    backend.reviveOffers()
  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures, blacklistTrackerOpt)
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    // Kill all running tasks for the stage.
    killAllTaskAttempts(stageId, interruptThread, reason = "Stage cancelled")
    // Cancel all attempts for the stage.
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  override def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    logInfo(s"Killing task $taskId: $reason")
    val execId = taskIdToExecutorId.get(taskId)
    if (execId.isDefined) {
      backend.killTask(taskId, execId.get, interruptThread, reason)
      true
    } else {
      logWarning(s"Could not kill task $taskId because no task with that ID was found.")
      false
    }
  }

  override def killAllTaskAttempts(
      stageId: Int,
      interruptThread: Boolean,
      reason: String): Unit = synchronized {
    logInfo(s"Killing all running tasks in stage $stageId: $reason")
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // There are two possible cases here:
        // 1. The task set manager has been created and some tasks have been scheduled.
        //    In this case, send a kill signal to the executors to kill the task.
        // 2. The task set manager has been created but no tasks have been scheduled. In this case,
        //    simply continue.
        tsm.runningTasksSet.foreach { tid =>
          taskIdToExecutorId.get(tid).foreach { execId =>
            backend.killTask(tid, execId, interruptThread, reason)
          }
        }
      }
    }
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    manager.parent.removeSchedulable(manager)
    logInfo(s"Removed TaskSet ${manager.taskSet.id}, whose tasks have all completed, from pool" +
      s" ${manager.parent.name}")
  }

  /**
    * 给【单个】taskSet中的task分配资源，不同于resourceOffers从调度池中给所有的taskSet分配
    *
    * private[spark] class TaskDescription(
    * val taskId: Long,
    * val attemptNumber: Int,
    * val executorId: String,
    * val name: String,
    * val index: Int,
    * val partitionId: Int,
    * val addedFiles: Map[String, Long],
    * val addedJars: Map[String, Long],
    * val properties: Properties,
    * val serializedTask: ByteBuffer)
    *
    * @param taskSet  被分配资源的单个taskSet
    * @param maxLocality 最大的本地性
    * @param shuffledOffers 打乱之后的 workOffer
    * @param availableCpus  每个Excutor中可用的Cpu个数 ，availableCpus = shuffledOffers.map(o => o.cores).toArray
    * @param tasks  类型 Indexseq[ArrayBuffer[TaskDescription]]
    *               tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))
    * @param addressesWithDescs
    * @return
    */
  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: IndexedSeq[ArrayBuffer[TaskDescription]],
      addressesWithDescs: ArrayBuffer[(String, TaskDescription)]) : Boolean = {
    var launchedTask = false
    // nodes and executors that are blacklisted for the entire application have already been
    // filtered out by this point
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      // 第i个WorkerOffer【可用Executor代替】的可用的CPU核数大于等于CPUS_PER_TASK
      if (availableCpus(i) >= CPUS_PER_TASK) {
        try {
          //
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            //
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetManager.put(tid, taskSet)
            taskIdToExecutorId(tid) = execId
            executorIdToRunningTaskIds(execId).add(tid)
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            // Only update hosts for a barrier task.
            if (taskSet.isBarrier) {
              // The executor address is expected to be non empty.
              addressesWithDescs += (shuffledOffers(i).address.get -> task)
            }
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    return launchedTask
  }

  /**
    *
    * 由 cluster manager调用，以 offer resources on slaves。We respond by asking our active task
    * sets for tasks in order of priority。
    * 我们以 round-robin 的方式向每个节点填充任务,任务在集群中是平衡的。
    *
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: IndexedSeq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    /*
        WorkerOffer  Represents free resources available on an 【executor】
     */
    /*
         更新Host与Executor的各种映射关系
     */
    for (o <- offers) {
      if (!hostToExecutors.contains(o.host)) {
        hostToExecutors(o.host) = new HashSet[String]()
      }
      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
        hostToExecutors(o.host) += o.executorId
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        newExecAvail = true
      }
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Before making any offers, remove any nodes from the blacklist whose blacklist has expired. Do
    // this here to avoid a separate thread and added synchronization overhead, and also because
    // updating the blacklist is only relevant when task offers are being made.

    //在提供任何 offers 之前，请从黑名单中删除黑名单已过期的节点。做这在这里避免了单独的线程
    //和增加的同步开销，也是因为更新黑名单只在提供任务时才有意义。
    /*
       黑名单中删除黑名单已过期的节点,即不再存于黑名单
       为啥要在这里做？
       1、avoid a separate thread and added synchronization overhead
       2、只有在分配任务时，更新黑名单才有意义
     */
    blacklistTrackerOpt.foreach(_.applyBlacklistTimeout())
    // 过滤掉在和名单中的 host 和  executor ，NodeBlacklist 中的 node指的是 host
    val filteredOffers = blacklistTrackerOpt.map { blacklistTracker =>
      offers.filter { offer =>
        !blacklistTracker.isNodeBlacklisted(offer.host) &&
          !blacklistTracker.isExecutorBlacklisted(offer.executorId)
      }
    }.getOrElse(offers)

    // 打乱过滤之后的 workOffer, 避免将任务总是分配给同样一组Worker
    val shuffledOffers = shuffleOffers(filteredOffers)
    // Build a list of tasks to assign to each worker.
    // 根据每个WorkerOffer的可用的CPU核数创建同等尺寸的任务描述(TaskDescription)
    // 为啥这么做？
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))
    // 可用的CPU核数统计到 availableCpus
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    // 根据调度池选用的排序方式（FiFO Fire）排序，获取调度池中所有的 TaskSet
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      /*
           标记是否有新的Executor增加，重新计算本地性
            def executorAdded() {
              recomputeLocality()
            }
       */
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    /*
       按我们的调度顺序获取每个TaskSet，然后按递增顺序提供每个节点这样它就有机会在所有的位置级别上启动本地任务。
       注意:preferredLocality顺序:PROCESS_LOCAL、NODE_LOCAL、NO_PREF、RACK_LOCAL、ANY
     */
    for (taskSet <- sortedTaskSets) {
      /*
          WorkerOffer ： Represents free resources available on an 【executor】
          将每个 executor 上的 cpu 划分为多份，CPUS_PER_TASK=1，对应了我们 一个 executor中2cpu,可以分配2个Task
       */
      val availableSlots = availableCpus.map(c => c / CPUS_PER_TASK).sum
      // Skip the barrier taskSet if the available slots are less than the number of pending tasks.
      // 如果可用的slots小于待处理任务的数量，则跳过barrier taskSet。
      if (taskSet.isBarrier && availableSlots < taskSet.numTasks) {
        // Skip the launch process.
        // TODO SPARK-24819 If the job requires more slots than available (both busy and free
        // slots), fail the job on submit.
        logInfo(s"Skip current round of resource offers for barrier stage ${taskSet.stageId} " +
          s"because the barrier taskSet requires ${taskSet.numTasks} slots, while the total " +
          s"number of available slots is $availableSlots.")
      } else {
        var launchedAnyTask = false
        // Record all the executor IDs assigned barrier tasks on.
        val addressesWithDescs = ArrayBuffer[(String, TaskDescription)]()
        /*
           遍历当前taskSet的本地性级别，然后从高到低调用 resourceOfferSingleTaskSet 分配资源
           若launchedTaskAtCurrentMaxLocality = true ，当前等级可以满足分配资源的要求
         */
        for (currentMaxLocality <- taskSet.myLocalityLevels) {
          var launchedTaskAtCurrentMaxLocality = false
          do {
            // 给单个 taskSet分配资源
            launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(taskSet,
              currentMaxLocality, shuffledOffers, availableCpus, tasks, addressesWithDescs)
            launchedAnyTask |= launchedTaskAtCurrentMaxLocality
          } while (launchedTaskAtCurrentMaxLocality)
        }
        /*
            launchedAnyTask |= launchedTaskAtCurrentMaxLocality只要有一个level满足了某个task满足
            launchedAnyTask 即为 true

            与调度逻辑偏离较远
         */
        if (!launchedAnyTask) {
          taskSet.getCompletelyBlacklistedTaskIfAny(hostToExecutors).foreach { taskIndex =>
              // If the taskSet is unschedulable we try to find an existing idle blacklisted
              // executor. If we cannot find one, we abort immediately. Else we kill the idle
              // executor and kick off an abortTimer which if it doesn't schedule a task within the
              // the timeout will abort the taskSet if we were unable to schedule any task from the
              // taskSet.
              // Note 1: We keep track of schedulability on a per taskSet basis rather than on a per
              // task basis.
              // Note 2: The taskSet can still be aborted when there are more than one idle
              // blacklisted executors and dynamic allocation is on. This can happen when a killed
              // idle executor isn't replaced in time by ExecutorAllocationManager as it relies on
              // pending tasks and doesn't kill executors on idle timeouts, resulting in the abort
              // timer to expire and abort the taskSet.
              executorIdToRunningTaskIds.find(x => !isExecutorBusy(x._1)) match {
                case Some ((executorId, _)) =>
                  if (!unschedulableTaskSetToExpiryTime.contains(taskSet)) {
                    blacklistTrackerOpt.foreach(blt => blt.killBlacklistedIdleExecutor(executorId))

                    val timeout = conf.get(config.UNSCHEDULABLE_TASKSET_TIMEOUT) * 1000
                    unschedulableTaskSetToExpiryTime(taskSet) = clock.getTimeMillis() + timeout
                    logInfo(s"Waiting for $timeout ms for completely "
                      + s"blacklisted task to be schedulable again before aborting $taskSet.")
                    abortTimer.schedule(
                      createUnschedulableTaskSetAbortTimer(taskSet, taskIndex), timeout)
                  }
                case None => // Abort Immediately
                  logInfo("Cannot schedule any task because of complete blacklisting. No idle" +
                    s" executors can be found to kill. Aborting $taskSet." )
                  taskSet.abortSinceCompletelyBlacklisted(taskIndex)
              }
          }
        } else {
          // We want to defer killing any taskSets as long as we have a non blacklisted executor
          // which can be used to schedule a task from any active taskSets. This ensures that the
          // job can make progress.
          // Note: It is theoretically possible that a taskSet never gets scheduled on a
          // non-blacklisted executor and the abort timer doesn't kick in because of a constant
          // submission of new TaskSets. See the PR for more details.
          if (unschedulableTaskSetToExpiryTime.nonEmpty) {
            logInfo("Clearing the expiry times for all unschedulable taskSets as a task was " +
              "recently scheduled.")
            unschedulableTaskSetToExpiryTime.clear()
          }
        }

        if (launchedAnyTask && taskSet.isBarrier) {
          // Check whether the barrier tasks are partially launched.
          // TODO SPARK-24818 handle the assert failure case (that can happen when some locality
          // requirements are not fulfilled, and we should revert the launched tasks).
          require(addressesWithDescs.size == taskSet.numTasks,
            s"Skip current round of resource offers for barrier stage ${taskSet.stageId} " +
              s"because only ${addressesWithDescs.size} out of a total number of " +
              s"${taskSet.numTasks} tasks got resource offers. The resource offers may have " +
              "been blacklisted or cannot fulfill task locality requirements.")

          // materialize the barrier coordinator.
          maybeInitBarrierCoordinator()

          // Update the taskInfos into all the barrier task properties.
          val addressesStr = addressesWithDescs
            // Addresses ordered by partitionId
            .sortBy(_._2.partitionId)
            .map(_._1)
            .mkString(",")
          addressesWithDescs.foreach(_._2.properties.setProperty("addresses", addressesStr))

          logInfo(s"Successfully scheduled all the ${addressesWithDescs.size} tasks for barrier " +
            s"stage ${taskSet.stageId}.")
        }
      }
    }

    // TODO SPARK-24823 Cancel a job that contains barrier stage(s) if the barrier tasks don't get
    // launched within a configured time.
    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

  private def createUnschedulableTaskSetAbortTimer(
      taskSet: TaskSetManager,
      taskIndex: Int): TimerTask = {
    new TimerTask() {
      override def run() {
        if (unschedulableTaskSetToExpiryTime.contains(taskSet) &&
            unschedulableTaskSetToExpiryTime(taskSet) <= clock.getTimeMillis()) {
          logInfo("Cannot schedule any task because of complete blacklisting. " +
            s"Wait time for scheduling expired. Aborting $taskSet.")
          taskSet.abortSinceCompletelyBlacklisted(taskIndex)
        } else {
          this.cancel()
        }
      }
    }
  }

  /**
   * Shuffle offers around to avoid always placing tasks on the same workers.  Exposed to allow
   * overriding in tests, so it can be deterministic.
   */
  protected def shuffleOffers(offers: IndexedSeq[WorkerOffer]): IndexedSeq[WorkerOffer] = {
    Random.shuffle(offers)
  }

  /**
    * Task SchedulerImpl对执行结果的处理
    *
    * Task在执行的时候会不断发送StatusUpdate消息， 在Local模式下， LocalEndpoint接收
    * statusUpdate消息会先匹配执行TaskSchedulerImpl的statusUpdate方法，然后调用reviveOffers方法给
    * 其他Task分配资源。
    *
    * TaskSchedulerImpl的statusUpdate方法用于更新Task的状态，状态包括
    * 状态包括：运行中(RUNNING) 、已完成(FINISHED) 、失败(FAILED) 、被杀死(KILLED)
    * 丢失(LOST) 五种。会从taskIdToTaskSetId、taskIdToExecutorId中移除此任务， 并且调用
    * taskResultGetter的enqueueSuccessfulTask方法。
    *
    * @param tid
    * @param state
    * @param serializedData
    */
  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None
    var reason: Option[ExecutorLossReason] = None
    synchronized {
      try {
        // 获取 TaskId 对应的 TaskSetManager
        Option(taskIdToTaskSetManager.get(tid)) match {
          case Some(taskSet) =>
            /*
                如果要更新的任务状态是LOST， 那么从 taskIdToExecutorId 中获取Task对应的
                Executor的身份标识。如果此Executor上正在运行Task， 那么调用removeExecutor方法移
                除Executor， 移除的原因是SlaveLost。
             */
            if (state == TaskState.LOST) {
              // TaskState.LOST is only used by the deprecated Mesos fine-grained scheduling mode,
              // where each executor corresponds to a single task, so mark the executor as failed.
              val execId = taskIdToExecutorId.getOrElse(tid, throw new IllegalStateException(
                "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)"))
              if (executorIdToRunningTaskIds.contains(execId)) {
                reason = Some(
                  SlaveLost(s"Task $tid was lost, so marking the executor as lost as well."))
                removeExecutor(execId, reason.get)
                failedExecutor = Some(execId)
              }
            }
            /*
                除了Lost，就是剩下这些状态
             */
            if (TaskState.isFinished(state)) {
              // 移除Task的状态
              cleanupTaskState(tid)
              taskSet.removeRunningTask(tid)
              // 调用 taskResultGetter 处理结果
              if (state == TaskState.FINISHED) {
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates) or its " +
                "executor has been marked as failed.")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      assert(reason.isDefined)
      dagScheduler.executorLost(failedExecutor.get, reason.get)
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  override def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = {
      accumUpdates.flatMap { case (id, updates) =>
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        Option(taskIdToTaskSetManager.get(id)).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskFailedReason): Unit = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && !taskSetManager.someAttemptSucceeded(tid)) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
      }
    }
  }

  override def stop() {
    speculationScheduler.shutdown()
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    if (barrierCoordinator != null) {
      barrierCoordinator.stop()
    }
    starvationTimer.cancel()
    abortTimer.cancel()
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  /**
    * check Specula table Tasks方法实际依赖于root Pool的check Specula table
    * Tasks方法(见代码清单7-52) 。如果检查到有可以推断执行的任务， 则调用Scheduler
    * Backend的revive Offers方法。以Local模式下的LocalSchedulerBackend为例，LocalSchedulerBackend
    * 的revive Offers方法(见代码清单7-92) 将向Local Endpoint发送ReviveOffers消息，
    * LocalEndpoint接收到Revive Offers消息后(见代码清单7-88),将调用LocalEndpoint
    * 的reviveOffers方法(见代码清单7-89)分配资源并运行Task。
    */

  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      /*
         如果 executorIdToRunningTaskIds 中包含指定的Executor的身份标识，这说明
         此时在此Executor上已经有Task正在运行， 那么调用removeExecutor方法移除Executor，
         并将failedExecutor设置为此Executor
       */
      if (executorIdToRunningTaskIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        removeExecutor(executorId, reason)
        failedExecutor = Some(executorId)
      } else {
        /*
           如果executorIdToRunningTaskIds中不包含指定的Executor的身份标识，这说明此
           时在此Executor上没有Task正在运行，那么从executorIdToHost中获取此Executor对应的
           Host，并调用removeExecutor方法移除Executor
         */
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            // If the host mapping still exists, it means we don't know the loss reason for the
            // executor. So call removeExecutor() to update tasks running on that executor when
            // the real loss reason is finally known.
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)

          case None =>
            // We may get multiple executorLost() calls with different loss reasons. For example,
            // one may be triggered by a dropped connection from the slave while another may be a
            // report of executor termination from Mesos. We produce log messages for both so we
            // eventually report the termination reason.
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    /*
        如果failed Executor设置了Executor的身份标识， 这说明此Executor已经被移
        除， 那么此Executor上正在运行的Task需要得到妥善的安置。
        Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
     */
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get, reason)
      backend.reviveOffers()
    }
  }

  override def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logInfo(s"Handle removed worker $workerId: $message")
    dagScheduler.workerRemoved(workerId, host, message)
  }

  /**
    *  Executor log日志
    */
  private def logExecutorLoss(
      executorId: String,
      hostPort: String,
      reason: ExecutorLossReason): Unit = reason match {
    case LossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  /**
   * Cleans up the TaskScheduler's state for tracking the given task.
   */
  private def cleanupTaskState(tid: Long): Unit = {
    taskIdToTaskSetManager.remove(tid)
    taskIdToExecutorId.remove(tid).foreach { executorId =>
      executorIdToRunningTaskIds.get(executorId).foreach { _.remove(tid) }
    }
  }

  /**
   * Remove an executor from all our data structures and mark it as lost. If the executor's loss
   * reason is not yet known, do not yet remove its association with its host nor update the status
   * of any running tasks, since the loss reason defines whether we'll fail those tasks.
   */
  private def removeExecutor(executorId: String, reason: ExecutorLossReason) {
    // The tasks on the lost executor may not send any more status updates (because the executor
    // has been lost), so they should be cleaned up here.
    /*
        ExecutorLoss Reason有四个子类,分别代表四种不同的原因
          Slavelost: Worker丢失。
          LossReason Pending:未知的原因导致的 Executor退出。
          ExecutorkKilled: Executor被“杀死”了。
          ExecutorExited: Executor退出了。
     */
    // cleanupTaskState 清除在此 Executor上正在运行的Task在 taskIdToTaskSetManager、
    //taskIdToExecutorld等缓存中的数据。
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up TaskScheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")
      // We do not notify the TaskSetManager of the task failures because that will
      // happen below in the rootPool.executorLost() call.
      taskIds.foreach(cleanupTaskState)
    }
    /*
        从 hostToExecutors 中移除此 Executor 的信息。
        如果 hostToexecutors 中此 Executor所在的Host主机已经没有任何 Executor了,那
        么从 hosts ByRack中移除此Host的信息。如果 hostsByRack中此 Executor所在的机架
        已经没有任何Host了,那么从 hostsByRack 中移除此机架的信息。
     */
    val host = executorIdToHost(executorId)
    val execs = hostToExecutors.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      hostToExecutors -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }
    /*
      如果移除 Executor的原因不是 LossReasonPending 【未知的原因导致的 Executor退出】,
      那么首先从 executorldToHost 中移除此 Executor的缓存,然后调用根调度池 rootpool的
      executorLost方法,将在此Executor上正在运行的Task作为失败任务处理,最后重新提交这些任务。
     */
    if (reason != LossReasonPending) {
      executorIdToHost -= executorId
      rootPool.executorLost(executorId, host, reason)
    }
    // 从黑名单中删除 executorId
    blacklistTrackerOpt.foreach(_.handleRemovedExecutor(executorId))
  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    hostToExecutors.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    hostToExecutors.contains(host)
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.contains(execId)
  }

  def isExecutorBusy(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.get(execId).exists(_.nonEmpty)
}

  /**
   * Get a snapshot of the currently blacklisted nodes for the entire application.  This is
   * thread-safe -- it can be called without a lock on the TaskScheduler.
   */
  def nodeBlacklist(): scala.collection.immutable.Set[String] = {
    blacklistTrackerOpt.map(_.nodeBlacklist()).getOrElse(scala.collection.immutable.Set())
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      // Might take a while for backend to be ready if it is waiting on resources.
      if (sc.stopped.get) {
        // For example: the master removes the application for some reason
        throw new IllegalStateException("Spark context stopped while waiting for backend")
      }
      synchronized {
        this.wait(100)
      }
    }
  }

  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  private[scheduler] def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager] = {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }

  /**
   * Marks the task has completed in all TaskSetManagers for the given stage.
   *
   * After stage failure and retry, there may be multiple TaskSetManagers for the stage.
   * If an earlier attempt of a stage completes a task, we should ensure that the later attempts
   * do not also submit those same tasks.  That also means that a task completion from an earlier
   * attempt can lead to the entire stage getting marked as successful.
   */
  private[scheduler] def markPartitionCompletedInAllTaskSets(
      stageId: Int,
      partitionId: Int,
      taskInfo: TaskInfo) = {
    taskSetsByStageIdAndAttempt.getOrElse(stageId, Map()).values.foreach { tsm =>
      tsm.markPartitionCompleted(partitionId, taskInfo)
    }
  }

}


private[spark] object TaskSchedulerImpl {

  val SCHEDULER_MODE_PROPERTY = "spark.scheduler.mode"

  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used. The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given {@literal <h1, [o1, o2, o3]>}, {@literal <h2, [o4]>} and
   * {@literal <h3, [o5, o6]>}, returns {@literal [o1, o5, o4, o2, o6, o3]}.
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.getOrElse(key, null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size) {
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

  private def maybeCreateBlacklistTracker(sc: SparkContext): Option[BlacklistTracker] = {
    if (BlacklistTracker.isBlacklistEnabled(sc.conf)) {
      val executorAllocClient: Option[ExecutorAllocationClient] = sc.schedulerBackend match {
        case b: ExecutorAllocationClient => Some(b)
        case _ => None
      }
      Some(new BlacklistTracker(sc, executorAllocClient))
    } else {
      None
    }
  }

}
