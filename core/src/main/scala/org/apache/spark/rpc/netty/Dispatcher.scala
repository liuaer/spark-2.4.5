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

package org.apache.spark.rpc.netty

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

/**
 * A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
 *
 * @param numUsableCores Number of CPU cores allocated to the process, for sizing the thread pool.
 *                       If 0, will consider the available CPUs on the host.
 */
/*
    创建消息调度器Dispatcher是有效提高NettyRpcEnv对消息异步处理并最大提升并行处理能力的前提。
    Dispatcher负责将RPC消息路由到要该对此消息处理的RpcEndpoint（RPC端点）。
 */
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv, numUsableCores: Int) extends Logging {
  /*
      RPC端点数据，它包括了RpcEndpoint、NettyRpcEndpointRef及Inbox等属于同一个端点的实例。
      Inbox与RpcEndpoint、NettyRpcEndpointRef通过此EndpointData相关联。
   */
  private class EndpointData(
      val name: String,
      val endpoint: RpcEndpoint,
      val ref: NettyRpcEndpointRef) {
    val inbox = new Inbox(ref, endpoint)
  }

  /**
   * 端点实例名称与端点数据EndpointData之间映射关系的缓存。有了这个缓存，
   * 就可以使用端点名称从中快速获取或删除EndpointData了。
   */
  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]
  /**
   *  端点实例RpcEndpoint与端点实例引用RpcEndpointRef之间映射关系的缓存。
   *  有了这个缓存，就可以使用端点实例从中快速获取或删除端点实例引用了。
   */
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  /**
   * receivers：存储端点数据EndpointData的阻塞队列。只有Inbox中有消息的EndpointData
   * 才会被放入此阻塞队列。
   */
  // Track the receivers whose inboxes may contain messages.
  private val receivers = new LinkedBlockingQueue[EndpointData]

  /**
   * True if the dispatcher has been stopped. Once stopped, all messages posted will be bounced
   * immediately.
   * Dispatcher是否停止的状态
   */
  @GuardedBy("this")
  private var stopped = false
  /*
     Dispatcher的registerRpcEndpoint方法用于注册RpcEndpoint，这个方法的副作用便
     可以将EndpointData放入receivers，其实现如代码清单5-9所示。

     注册RpcEndpoint的步骤如下。
     1）使用当前RpcEndpoint所在NettyRpcEnv的地址和RpcEndpoint的名称创建RpcEndpointAddress对象。
     2）创建RpcEndpoint的引用对象——NettyRpcEndpointRef。
     3）创建EndpointData，并放入endpoints缓存。
     4）将RpcEndpoint与NettyRpcEndpointRef的映射关系放入endpointRefs缓存。
     5）将EndpointData放入阻塞队列receivers的队尾。MessageLoop线程异步获取到此EndpointData，
        并处理其Inbox中刚刚放入的OnStart消息，最终调用RpcEndpoint的OnStart方法在RpcEndpoint开始
        处理消息之前做一些准备工作。
     6）返回NettyRpcEndpointRef。
   */
  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }
      val data = endpoints.get(name)
      endpointRefs.put(data.endpoint, data.ref)
      receivers.offer(data)  // for the OnStart message
    }
    endpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  // Should be idempotent
  private def unregisterRpcEndpoint(name: String): Unit = {
    val data = endpoints.remove(name)
    if (data != null) {
      /*
        可是奇怪的是为什么EndpointData从endpoints中移除后，最后还要放入receivers?
        EndpointData虽然移除了，但是对应的RpcEndpointRef并没有从endpointRefs缓存中移除。
        -----------
        当要移除一个EndpointData时，其Inbox可能正在对消息进行处理，所以不能贸然停止。
        这里采用了更平滑的停止方式，即调用了Inbox的stop方法来平滑过渡
       */
      data.inbox.stop()
      receivers.offer(data)  // for the OnStop message
    }
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.
  }

  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      if (stopped) {
        // This endpoint will be stopped by Dispatcher.stop() method.
        return
      }
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
   * Send a message to all registered [[RpcEndpoint]]s in this process.
   *
   * This can be used to make network events known to all end points (e.g. "a new node connected").
   */
  def postToAll(message: InboxMessage): Unit = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
        postMessage(name, message, (e) => { e match {
          case e: RpcEnvStoppedException => logDebug (s"Message $message dropped. ${e.getMessage}")
          case e: Throwable => logWarning(s"Message $message dropped. ${e.getMessage}")
        }}
      )}
  }

  /** Posts a message sent by a remote endpoint. */
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    // RpcCallContext 是用于回调客户端的上下文
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  /** Posts a message sent by a local endpoint. */
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  /** Posts a one-way message. */
  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  /**
   * Posts a message to a specific endpoint.
   * Dispatcher 的 postMessage 用于将消息提交给指定的RpcEndpoint
   * @param endpointName name of the endpoint.
   * @param message the message to post
   * @param callbackIfStopped callback function if the endpoint is stopped.
   */
    /*
       postMessage方法的处理步骤如下。
       1）根据端点名称 endpointName 从缓存 endpoints 中获取EndpointData。
       2）如果当前Dispatcher没有停止并且缓存endpoints中确实存在名为endpointName的EndpointData，那么
          将调用EndpointData对应Inbox的post方法将消息加入Inbox的消息列表中，因此还需要将EndpointData推入
          receivers，以便MessageLoop处理此Inbox中的消息。Inbox的post方法的实现如代码清单5-14所示，其逻辑
          为Inbox未停止时向messages列表加入消息。
     */
  private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      // 根据端点名称endpointName从缓存endpoints中获取EndpointData。
      val data = endpoints.get(endpointName)
      if (stopped) {
        Some(new RpcEnvStoppedException())
      } else if (data == null) {
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        data.inbox.post(message)
        receivers.offer(data)
        None
      }
    }
    // We don't need to call `onStop` in the `synchronized` block
    error.foreach(callbackIfStopped)
  }
  /*
    1）如果Dispatcher还未停止，则将自身状态修改为已停止。
    2）对endpoints中的所有EndpointData去注册。这里通过调用unregisterRpcEndpoint方法（见代码清单5-11），
       将向endpoints中的每个EndpointData的Inbox里放置OnStop消息。
    3）向receivers中投放“毒药”。提示由于Dispatcher都停止了，所以Dispatcher中的所有MessageLoop线程也
       就没有存在的必要了。
    4）关闭threadpool线程池。
   */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    // Stop all endpoints. This will queue all endpoints for processing by the message loops.
    endpoints.keySet().asScala.foreach(unregisterRpcEndpoint)
    // Enqueue a message that tells the message loops to stop.
    receivers.offer(PoisonPill)
    threadpool.shutdown()
  }

  def awaitTermination(): Unit = {
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
   * Return if the endpoint exists
   */
  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }
  /*
      用于对消息进行调度的线程池。此线程池运行的任务都是 MessageLoop
   */
  /** Thread pool used for dispatching messages. */
  private val threadpool: ThreadPoolExecutor = {
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
    val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
      math.max(2, availableCores))
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }
  /*
    1）从receivers中获取EndpointData。receivers中的EndpointData，其Inbox的messages列表中肯定有了新的消息。
    换言之，只有Inbox的messages列表中有了新的消息，此EndpointData才会被放入receivers中。由于receivers是个
    阻塞队列，所以当receivers中没有EndpointData时，MessageLoop线程会被阻塞。2）如果取到的EndpointData是
    “毒药”（PoisonPill），那么此MessageLoop线程将退出（通过return语句）。这里有个动作就是将PoisonPill重新
    放入到receivers中，这是因为threadpool线程池极有可能不止一个MessageLoop线程，为了让大家都“毒发身亡”，还
    需要把“毒药”放回到receivers中，这样其他“活着”的线程就会再次误食“毒药”，达到所有MessageLoop线程都结束的
    效果。3）如果取到的EndpointData不是“毒药”，那么调用EndpointData中Inbox的process方法对消息进行处理。上
    文的MessageLoop任务实际是将消息交给EndpointData中Inbox的process方法处理的
   */
  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            val data = receivers.take()
            if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              receivers.offer(PoisonPill)
              return
            }
            data.inbox.process(Dispatcher.this)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case _: InterruptedException => // exit
        case t: Throwable =>
          try {
            // Re-submit a MessageLoop so that Dispatcher will still work if
            // UncaughtExceptionHandler decides to not kill JVM.
            threadpool.execute(new MessageLoop)
          } finally {
            throw t
          }
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = new EndpointData(null, null, null)
}
