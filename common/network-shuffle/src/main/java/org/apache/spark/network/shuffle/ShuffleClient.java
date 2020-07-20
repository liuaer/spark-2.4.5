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

package org.apache.spark.network.shuffle;

import java.io.Closeable;
import java.util.Collections;

import com.codahale.metrics.MetricSet;

/**
 * 6.9.3 Shuffle客户端
 * 根据前面的介绍,如果没有部署外部的 Shuffle服务,即 spark.shuffle.service.enabled
 * 属性为 false时, Netty BlockTransferService不但通过 OneForOneStreamManager与Nety
 * BlockRpc Server对外提供Blck上传与下载的服务,也将作为默认的Shuffer客户端。
 * Netty Block Transfer Service作为 Shuffle客户端,具有发起上传和下载请求并接收服务端响
 * 应的能力。 Netty BlockTransfer Service的两个方法— -fetchBlocks和 uploadBlock将帮我们
 * 达到目的。
 *
 *  Provides an interface for reading shuffle files, either from an Executor or external service.
 * */
public abstract class ShuffleClient implements Closeable {

  /**
   * Initializes the ShuffleClient, specifying this Executor's appId.
   * Must be called before any other method on the ShuffleClient.
   */
  public void init(String appId) { }

  /**
   * Fetch a sequence of blocks from a remote node asynchronously,
   *
   * Note that this API takes a sequence so the implementation can batch requests, and does not
   * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
   * the data of a block is fetched, rather than waiting for all blocks to be fetched.
   *
   * @param host the host of the remote node.
   * @param port the port of the remote node.
   * @param execId the executor id.
   * @param blockIds block ids to fetch.
   * @param listener the listener to receive block fetching status.
   * @param downloadFileManager DownloadFileManager to create and clean temp files.
   *                        If it's not <code>null</code>, the remote blocks will be streamed
   *                        into temp shuffle files to reduce the memory usage, otherwise,
   *                        they will be kept in memory.
   */
  public abstract void fetchBlocks(
      String host,
      int port,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener,
      DownloadFileManager downloadFileManager);

  /**
   * Get the shuffle MetricsSet from ShuffleClient, this will be used in MetricsSystem to
   * get the Shuffle related metrics.
   */
  public MetricSet shuffleMetrics() {
    // Return an empty MetricSet by default.
    return () -> Collections.emptyMap();
  }
}
