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

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

/*

    SortShuffleWriter 使用 ExternalSorter作为排序器， 由于ExternalSorter底层使用了Partitioned
    AppendOnlyMap和 PartitionedPairBuffer两种缓存， 因此SortShuffleWriter 还支持对Shuffle
    数据的聚合功能。

 */
private[spark] class SortShuffleWriter[K, V, C](
   /*
     特质ShuffleBlockResolver定义了对ShuffleBlock进行解析的规范， 包括获取Shuffe数
     据文件、获取Shuffle索引文件、删除指定的Shuffle数据文件和索引文件、生成Shuffe索
     引文件、获取Shuffle块的数据等
    */
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C], // 使用它向Task传递 shuffle 信息
    mapId: Int,  // Map 任务
    context: TaskContext) // TaskContextImp
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  // 是否正在停止
  private var stopping = false
  // Map 任务的状态
  private var mapStatus: MapStatus = null

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /**
    *  SortShuffleWriter 的核心实现在于将map任务的输出结果写到磁盘的write方法
    *
    *  Write a bunch of records to this task's output
    *
    * */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    /*
        使用 BaseShuffleHandle.mapSideCombine 确定是否在map 端合并

        那么创建 ExternalSorter 时， 将ShuffleDependency的 aggregator和 keyOrdering 传递
        给ExternalSorter 的 aggregator 和 ordering属性， 否则不进行传递。这也间接决定了External
        Sorter选择Partitioned Append Only Map还是PartitionedPairBuffer。

     */
    sorter = if (dep.mapSideCombine) {
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      //  如果不用聚合，那么就没有必要排序
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    // map 任务的输出插入到缓存中
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    //  获取 shuffle 数据文件
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    // 和 output 同目录的临时文件
    val tmp = Utils.tempFileWith(output)
    try {
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      // 数据仅仅存放在内存中， 必然存在着丢失的风险。External Sorter的 write Partitioned File
      // 方法用于持久化计算结果，写在了临时文件 tmp 中
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      /*
         partitionLengths :  索引文件中各个partition的长度数据

         writeIndexFileAndCommit-->生成Block文件对应的索引文件。此索引文件用于记录各个分区在Block文件中
         对应的偏移量， 以便于reduce任务拉取时使用。
       */
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  /**
    *  是否绕开合并排序
    */
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    // 设置了 合并排序，必定绕不开
    if (dep.mapSideCombine) {
      false
    } else {
      val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
      // 判断分区数是否> 200(默认)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
