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

import java.nio.ByteBuffer
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.rpc.{RpcAddress, RpcEnvStoppedException}
/*
    消息列表messages中的消息类型为OutboxMessage，所有将要向远端发送的消息都会被封装成
    OutboxMessage类型。OutboxMessage作为一个特质，定义了所有向外发送消息的规范，

    根据OutboxMessage的名称，我们很容易与Dispatcher中Inbox里的InboxMessage类型的消息关联起来。
    OutboxMessage在客户端使用，是对外发送消息的封装。
    InboxMessage在服务端使用，是对所接收消息的封装。
 */
private[netty] sealed trait OutboxMessage {

  def sendWith(client: TransportClient): Unit

  def onFailure(e: Throwable): Unit

}

private[netty] case class OneWayOutboxMessage(content: ByteBuffer) extends OutboxMessage
  with Logging {

  override def sendWith(client: TransportClient): Unit = {
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case e1: RpcEnvStoppedException => logDebug(e1.getMessage)
      case e1: Throwable => logWarning(s"Failed to send one-way RPC.", e1)
    }
  }

}

private[netty] case class RpcOutboxMessage(
    content: ByteBuffer,
    _onFailure: (Throwable) => Unit,
    _onSuccess: (TransportClient, ByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback with Logging {

  private var client: TransportClient = _
  private var requestId: Long = _

  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    this.requestId = client.sendRpc(content, this)
  }

  def onTimeout(): Unit = {
    if (client != null) {
      client.removeRpcRequest(requestId)
    } else {
      logError("Ask timeout before connecting successfully")
    }
  }

  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }

  override def onSuccess(response: ByteBuffer): Unit = {
    _onSuccess(client, response)
  }

}

private[netty] class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {
  //  nettyEnv 当前 Outbox 所在节点上的 NettyRpcEnv
  //  address  Outbox所对应的远端 NettyRpcEnv的地址
  outbox => // Give this an alias so we can use it more clearly in closures.
  //  向其他远端 NettyRpcEnv 上的所有 RpcEndpoint 发送的消息列表。
  @GuardedBy("this")
  private val messages = new java.util.LinkedList[OutboxMessage]

  //  当前Outbox内的TransportClient。消息的发送都依赖于此传输客户端。
  //  @GuardedBy( "this" ) 受对象内部锁保护
  @GuardedBy("this")
  private var client: TransportClient = null

  /**
   * connectFuture points to the connect task. If there is no connect task, connectFuture will be
   * null.
   */
  // 指向当前Outbox内连接任务的java.util.concurrent.Future引用。如果当前没有连接任务，
  // 则connectFuture为null。
  @GuardedBy("this")
  private var connectFuture: java.util.concurrent.Future[Unit] = null
  // 当前Outbox是否停止的状态
  @GuardedBy("this")
  private var stopped = false

  /**
   * If there is any thread draining the message queue
   */
  //  表示当前 Outbox 内正有线程在处理 messages 列表中消息的状态。
  @GuardedBy("this")
  private var draining = false

  /**
   * Send a message. If there is no active connection, cache it and launch a new connection. If
   * [[Outbox]] is stopped, the sender will be notified with a [[SparkException]].
   */
  def send(message: OutboxMessage): Unit = {
    // 1）判断当前Outbox的状态是否已经停止。
    val dropped = synchronized {
      if (stopped) {
        true
      } else {
        messages.add(message)
        false
      }
    }
    // 2）如果Outbox已经停止，则向发送者发送SparkException异常。如果Outbox还未停止，
    // 则将OutboxMessage添加到messages列表中，并且调用drainOutbox方法处理messages中的消息
    if (dropped) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
    } else {
      drainOutbox()
    }
  }

  /**
   * Drain the message queue. If there is other draining thread, just exit. If the connection has
   * not been established, launch a task in the `nettyEnv.clientConnectionExecutor` to setup the
   * connection.
   */
    /*
    1）如果当前Outbox已经停止或者正在连接远端服务，则返回。
    2）如果Outbox中的TransportClient为null，这说明还未连接远端服务。此时需要调用launchConnectTask
       方法运行连接远端服务的任务，然后返回。
    3）如果正有线程在处理（即发送）messages列表中的消息，则返回。
    4）如果messages列表中没有消息要处理，则返回。否则取出其中的一条消息，并将draining状态置为true。
    5）循环处理messages列表中的消息，即不断从messages列表中取出消息并调用OutboxMessage的sendWith方法
    发送消息。在drainOutbox方法中调用launchConnectTask方法，运行连接远端服务的任务，
     */
  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
    synchronized {
      // 如果当前Outbox已经停止或者正在连接远端服务，则返回。
      if (stopped) {
        return
      }
      if (connectFuture != null) {
        // We are connecting to the remote address, so just exit
        return
      }
      // 如果Outbox中的TransportClient为null，这说明还未连接远端服务。此时需要调用
      // launchConnectTask 方法运行连接远端服务的任务，然后返回。
      if (client == null) {
        // There is no connect task but client is null, so we need to launch the connect task.
        launchConnectTask()
        return
      }
      // 如果正有线程在处理（即发送）messages列表中的消息，则返回。
      if (draining) {
        // There is some thread draining, so just exit
        return
      }
      // 如果messages列表中没有消息要处理，则返回。否则取出其中的一条消息，并将draining状态置为true。
      message = messages.poll()
      if (message == null) {
        return
      }
      draining = true
    }
      //  循环处理 messages 列表中的消息，即不断从messages列表中取出消息并调用 OutboxMessage 的
      //  sendWith 方法发送消息
    while (true) {
      try {
        val _client = synchronized { client }
        if (_client != null) {
          message.sendWith(_client)
        } else {
          assert(stopped == true)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if (stopped) {
          return
        }
        message = messages.poll()
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }
  //  运行连接远端服务的任务
  private def launchConnectTask(): Unit = {
    connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {
      /*
        1）构造Callable的匿名内部类，此匿名类将调用 NettyRpcEnv 的createClient
           方法创建TransportClient，然后调用drainOutbox方法处理Outbox中的消息。
       */
      override def call(): Unit = {
        try {
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            client = _client
            if (stopped) {
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            // exit
            return
          case NonFatal(e) =>
            outbox.synchronized { connectFuture = null }
            handleNetworkFailure(e)
            return
        }
        outbox.synchronized { connectFuture = null }
        // It's possible that no thread is draining now. If we don't drain here, we cannot send the
        // messages until the next message arrives.
        drainOutbox()
      }
    })
  }

  /**
   * Stop [[Inbox]] and notify the waiting messages with the cause.
   */
  private def handleNetworkFailure(e: Throwable): Unit = {
    synchronized {
      assert(connectFuture == null)
      if (stopped) {
        return
      }
      stopped = true
      closeClient()
    }
    // Remove this Outbox from nettyEnv so that the further messages will create a new Outbox along
    // with a new connection
    nettyEnv.removeOutbox(address)

    // Notify the connection failure for the remaining messages
    //
    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(e)
      message = messages.poll()
    }
    assert(messages.isEmpty)
  }

  private def closeClient(): Unit = synchronized {
    // Just set client to null. Don't close it in order to reuse the connection.
    client = null
  }

  /**
   * Stop [[Outbox]]. The remaining messages in the [[Outbox]] will be notified with a
   * [[SparkException]].
   */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
      if (connectFuture != null) {
        connectFuture.cancel(true)
      }
      closeClient()
    }

    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
      message = messages.poll()
    }
  }
}
