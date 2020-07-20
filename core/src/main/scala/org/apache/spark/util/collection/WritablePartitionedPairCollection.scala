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

package org.apache.spark.util.collection

import java.util.Comparator

import org.apache.spark.storage.DiskBlockObjectWriter

/**
  *
  * WritablePartitionedPairCollection是对由键值对构成的集合进行大小跟踪的通用接口。
  * 这里的每个键值对都有相关联的分区， 例如， key为(0， #) ， value为1的键值对， 真正的
  * 键实际是#， 而0则是键#的分区ID。WritablePartitionedPairCollection支持基于内存进行
  * 有效排序， 并可以创建将集合内容按照字节写入磁盘的 WritablePartitionedIterator。
  *
  * <br>
  *
 * A common interface for size-tracking collections of key-value pairs that
 *
 *  - Have an associated partition for each key-value pair.
 *  - Support a memory-efficient sorted iterator
 *  - Support a WritablePartitionedIterator for writing the contents directly as bytes.
 */
private[spark] trait WritablePartitionedPairCollection[K, V] {
  /**
   * Insert a key-value pair with a partition into the collection
   */
  def insert(partition: Int, key: K, value: V): Unit

  /**
   * Iterate through the data in order of partition ID and then the given comparator. This may
   * destroy the underlying collection.
   */
  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)]

  /**
   * Iterate through the data and write out the elements instead of returning them. Records are
   * returned in order of their partition ID and then the given comparator.
   * This may destroy the underlying collection.
   */
  def destructiveSortedWritablePartitionedIterator(keyComparator: Option[Comparator[K]])
    : WritablePartitionedIterator = {
    // 根据排序器排完序
    val it = partitionedDestructiveSortedIterator(keyComparator)
    new WritablePartitionedIterator {
      private[this] var cur = if (it.hasNext) it.next() else null

      def writeNext(writer: DiskBlockObjectWriter): Unit = {
        //  cur._1._2 = key  cur._2 = value
        writer.write(cur._1._2, cur._2)
        cur = if (it.hasNext) it.next() else null
      }

      def hasNext(): Boolean = cur != null
      //  cur._1._1 对应分区
      def nextPartition(): Int = cur._1._1
    }
  }
}

private[spark] object WritablePartitionedPairCollection {
  /**
    * 生成对由partition id和key构成的两个对偶对象按照partition id进行排序的比较器
    * <br>
   * A comparator for (Int, K) pairs that orders them by only their partition ID.
   */
  def partitionComparator[K]: Comparator[(Int, K)] = new Comparator[(Int, K)] {
    override def compare(a: (Int, K), b: (Int, K)): Int = {
      a._1 - b._1
    }
  }

  /**
    * 生成对由partition id和key构成的两个对偶对象先按照
    * partition id进行比较， 再根据指定的键比较器按照key进行第二级比较的比较器。
    * <br>
   * A comparator for (Int, K) pairs that orders them both by their partition ID and a key ordering.
   */
  def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)] = {
    new Comparator[(Int, K)] {
      override def compare(a: (Int, K), b: (Int, K)): Int = {
        val partitionDiff = a._1 - b._1
        if (partitionDiff != 0) {
          partitionDiff
        } else {
          keyComparator.compare(a._2, b._2)
        }
      }
    }
  }
}

/**
 * Iterator that writes elements to a DiskBlockObjectWriter instead of returning them. Each element
 * has an associated partition.
 */
private[spark] trait WritablePartitionedIterator {
  def writeNext(writer: DiskBlockObjectWriter): Unit

  def hasNext(): Boolean

  def nextPartition(): Int
}
