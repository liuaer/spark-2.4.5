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

import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}

/**
 * InboxMessage的多种实现类，其作用分别如下。
 * ➢OneWayMessage:RpcEndpoint处理此类型的消息后不需要向客户端回复信息。
 * ➢RpcMessage:RPC消息，RpcEndpoint处理完此消息后需要向客户端回复信息。
 * ➢OnStart：用于Inbox实例化后，再通知与此Inbox相关联的RpcEndpoint启动。
 * ➢OnStop：用于Inbox停止后，通知与此Inbox相关联的RpcEndpoint停止。
 * ➢RemoteProcessConnected：此消息用于告诉所有的RpcEndpoint，有远端的进程已经与当前RPC服务建立了连接。
 * ➢RemoteProcessDisconnected：此消息用于告诉所有的RpcEndpoint，有远端的进程已经与当前RPC服务断开了连接。
 * ➢RemoteProcessConnectionError：此消息用于告诉所有的RpcEndpoint，与远端某个地址之间的连接发生了错误。
 */
private[netty] sealed trait InboxMessage

private[netty] case class OneWayMessage(
    senderAddress: RpcAddress,
    content: Any) extends InboxMessage

private[netty] case class RpcMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext) extends InboxMessage

private[netty] case object OnStart extends InboxMessage

private[netty] case object OnStop extends InboxMessage

/** A message to tell all endpoints that a remote process has connected. */
private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a remote process has disconnected. */
private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a network error has happened. */
private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

/**
 * An inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
 */
/*
    端点内的盒子。每个RpcEndpoint都有一个对应的盒子，这个盒子里有个存储 InboxMessage 消息的列表
    messages。所有的消息将缓存在messages列表里面，并由RpcEndpoint异步处理这些消息。
 */
private[netty] class Inbox(
    val endpointRef: NettyRpcEndpointRef,
    val endpoint: RpcEndpoint)
  extends Logging {

  inbox =>  // Give this an alias so we can use it more clearly in closures.
  /*
    用于缓存需要由对应 RpcEndpoint 处理的消息，即与 Inbox 在同一 EndpointData中的 RpcEndpoint。
   */
  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()
  /*
      stopped:Inbox的停止状态。
      enableConcurrent：是否允许多个线程同时处理messages中的消息。
      numActiveThreads：激活线程的数量，即正在处理messages中消息的线程数量。
   */
  /** True if the inbox (and its associated endpoint) is stopped. */
  @GuardedBy("this")
  private var stopped = false

  /** Allow multiple threads to process messages at the same time. */
  @GuardedBy("this")
  private var enableConcurrent = false

  /** The number of threads processing messages for this inbox. */
  @GuardedBy("this")
  private var numActiveThreads = 0
  // Inbox向自身的messages列表中放入OnStart消息
  // OnStart should be the first message to process
  inbox.synchronized {
    messages.add(OnStart)
  }

  /**
   * Process stored messages.
   */
    /*
        注意第1）步、第2）步及第4）步位于Inbox的锁保护之下，是因为messages是普通的
        java.util.LinkedList, LinkedList本身不是线程安全的，所以为了增加并发安全性，需要通过同步保护。
     */
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    /*
        1）进行线程并发检查。具体是，如果不允许多个线程同时处理messages中的消息
          （enableConcurrent为false），并且当前激活线程数（numActiveThreads）不为0，
           这说明已经有线程在处理消息，所以当前线程不允许再去处理消息（使用return返回）。
     */
    inbox.synchronized {
      //  这说明已经有线程在处理消息
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      /*
          2）从messages中获取消息。如果有消息未处理，则当前线程需要处理此消息，因而算是
          一个新的激活线程（需要将numActiveThreads加1）。如果messages中没有消息了（一般
          发生在多线程情况下），则直接返回。
       */
      message = messages.poll()
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }
    /*
      3）根据消息类型进行匹配，并执行对应的逻辑。这里有个小技巧值得借鉴，那就是匹配执行的过程中
         也许会发生错误，当发生错误的时候，我们希望当前Inbox所对应RpcEndpoint的错误处理方法onError
         可以接收到这些错误信息。Inbox的safelyCall方法给我们提供了这方面的实现，如代码清单5-8所示。
     */
    while (true) {
      //当前Inbox所对应 RpcEndpoint 的错误处理方法 onError 可以接收到这些错误信息
      safelyCall(endpoint) {
        message match {
          case RpcMessage(_sender, content, context) =>
            try {
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case e: Throwable =>
                context.sendFailure(e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          case OneWayMessage(_sender, content) =>
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new SparkException(s"Unsupported message $message from ${_sender}")
            })

          case OnStart =>
            endpoint.onStart()
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }

          case OnStop =>
            val activeThreads = inbox.synchronized { inbox.numActiveThreads }
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            dispatcher.removeRpcEndpointRef(endpoint)
            endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
            endpoint.onConnected(remoteAddress)

          case RemoteProcessDisconnected(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)

          case RemoteProcessConnectionError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }
      /*
        4）对激活线程数量进行控制。当第3）步对消息处理完毕后，当前线程作为之前已经激活的
           线程是否还有存在的必要呢？这里有两个判断：如果不允许多个线程同时处理messages中的
           消息并且当前激活的线程数多于1个，那么需要当前线程退出并将numActiveThreads减1；如果
           messages已经没有消息要处理了，这说明当前线程无论如何也该返回并将numActiveThreads减1。
       */
      inbox.synchronized {
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          numActiveThreads -= 1
          return
        }
        message = messages.poll()
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    }
  }

  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      // We already put "OnStop" into "messages", so we should drop further messages
      onDrop(message)
    } else {
      messages.add(message)
      false
    }
  }

  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    // 1) 根据之前的分析，MessageLoop有“允许并发运行”和“不允许并发运行”两种情况。对于允许并发的情况，
    //为了确保安全，应该将enableConcurrent设置为false。
    if (!stopped) {
      // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
      // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
      // safely.
      enableConcurrent = false
      // 2）设置当前Inbox为停止状态。
      stopped = true
      /*
      3）向messages中添加OnStop消息。根据代码清单5-5中对MessageLoop的分析，为了能够处理OnStop消息，
         只有Inbox所属的EndpointData放入receivers中，其messages列表中的消息才会被处理，这回答了刚才
         提出的第一个问题。为了实现平滑停止，OnStop消息最终将匹配代码清单5-7，调用Dispatcher的
         removeRpcEndpointRef方法，将RpcEndpoint与RpcEndpointRef的映射从缓存endpointRefs中移除，
         这回答了刚才的第二个问题。在匹配执行OnStop消息的最后，将调用RpcEndpoint的onStop方法对RpcEndpoint停止。
       */
      //为了能够处理OnStop消息，只有Inbox所属的EndpointData放入receivers中，其messages列表中的消息才会被处理
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }

  /**
   * Called when we are dropping a message. Test cases override this to test message dropping.
   * Exposed for testing.
   */
  protected def onDrop(message: InboxMessage): Unit = {
    logWarning(s"Drop $message because $endpointRef is stopped")
  }

  /**
   * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
   */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try action catch {
          /*
             使用case NonFatal(ex) => ... 子句使用scala.util.control.NonFatal 匹配了所有的非致命性异常
           */
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) =>
            if (stopped) {
              logDebug("Ignoring error", ee)
            } else {
              logError("Ignoring error", ee)
            }
        }
    }
  }

}
