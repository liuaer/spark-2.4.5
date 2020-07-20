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

import org.apache.spark.SparkEnv
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.config._
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.util.Utils

/**
  *
  * @param id
  * @param name
  * @param numPartitions
  * @param storageLevel
  * @param parentIds  RDD的父RDD的id序列。这说明一个RDD会有零到多个父RDD
  * @param callSite  RDD的用户调用栈信息
  * @param scope    RDD的操作范围。scope的类型为RDDOperationScope,每一个RDD都有 一个 RDDOperationScope。
  *                 RDDOperationScope与Stage或 Job 之间并无特殊关系， —个RDDOperationScope可以存在
  *                 于一个Stage内，也可以跨越多个Job。
  */
@DeveloperApi
class RDDInfo(
    val id: Int,
    var name: String,
    val numPartitions: Int,
    var storageLevel: StorageLevel,
    val parentIds: Seq[Int],
    val callSite: String = "",
    val scope: Option[RDDOperationScope] = None)
  extends Ordered[RDDInfo] {

  var numCachedPartitions = 0
  /**
    * 使用的内存大小
    */
  var memSize = 0L
  /**
    * 使用的磁盘大小
    */
  var diskSize = 0L
  /**
    * Block存储在外部的大小
    */
  var externalBlockStoreSize = 0L
  /**
    * 是否已缓存
    */
  def isCached: Boolean = (memSize + diskSize > 0) && numCachedPartitions > 0

  override def toString: String = {
    import Utils.bytesToString
    ("RDD \"%s\" (%d) StorageLevel: %s; CachedPartitions: %d; TotalPartitions: %d; " +
      "MemorySize: %s; DiskSize: %s").format(
        name, id, storageLevel.toString, numCachedPartitions, numPartitions,
        bytesToString(memSize), bytesToString(diskSize))
  }

  override def compare(that: RDDInfo): Int = {
    this.id - that.id
  }
}

private[spark] object RDDInfo {
  /**
    * 构建 RDDInfo
    * @param rdd
    * @return
    */
  def fromRdd(rdd: RDD[_]): RDDInfo = {
    val rddName = Option(rdd.name).getOrElse(Utils.getFormattedClassName(rdd))
    val parentIds = rdd.dependencies.map(_.rdd.id)
    val callsiteLongForm = Option(SparkEnv.get)
      .map(_.conf.get(EVENT_LOG_CALLSITE_LONG_FORM))
      .getOrElse(false)

    val callSite = if (callsiteLongForm) {
      rdd.creationSite.longForm
    } else {
      rdd.creationSite.shortForm
    }
    new RDDInfo(rdd.id, rddName, rdd.partitions.length,
      rdd.getStorageLevel, parentIds, callSite, rdd.scope)
  }
}
