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

import scala.collection.mutable.PriorityQueue

/**
  *
  * * MedianHeap被设计用来快速追踪一组数字的中位数，可能包含重复项。插入新数字的时间复杂度为O(logn)
  * 确定中位数的时间复杂度为O(1)。基本的想法是保持两个堆:smallerHalf和largerHalf。的smallerHalf
  * 存储所有数字的较小的一半，而较大的一半存储较大的一半。每次插入新数字时，需要平衡两个堆的大小它们的大小不会
  * 相差超过1。因此每次调用find中值()，检查两个堆的大小是否相同。如果相等，我们应该
  * 返回两个最高的堆值的平均值。否则我们返回多1个元素的那个堆的值。
  *
 * MedianHeap is designed to be used to quickly track the median of a group of numbers
 * that may contain duplicates. Inserting a new number has O(log n) time complexity and
 * determining the median has O(1) time complexity.
 * The basic idea is to maintain two heaps: a smallerHalf and a largerHalf. The smallerHalf
 * stores the smaller half of all numbers while the largerHalf stores the larger half.
 * The sizes of two heaps need to be balanced each time when a new number is inserted so
 * that their sizes will not be different by more than 1. Therefore each time when
 * findMedian() is called we check if two heaps have the same size. If they do, we should
 * return the average of the two top values of heaps. Otherwise we return the top of the
 * heap which has one more element.
 */
private[spark] class MedianHeap(implicit val ord: Ordering[Double]) {

  /**
   * Stores all the numbers less than the current median in a smallerHalf,
   * i.e median is the maximum, at the root.
   */
  private[this] var smallerHalf = PriorityQueue.empty[Double](ord)

  /**
   * Stores all the numbers greater than the current median in a largerHalf,
   * i.e median is the minimum, at the root.
   */
  private[this] var largerHalf = PriorityQueue.empty[Double](ord.reverse)

  def isEmpty(): Boolean = {
    smallerHalf.isEmpty && largerHalf.isEmpty
  }

  def size(): Int = {
    smallerHalf.size + largerHalf.size
  }

  def insert(x: Double): Unit = {
    // If both heaps are empty, we arbitrarily insert it into a heap, let's say, the largerHalf.
    if (isEmpty) {
      largerHalf.enqueue(x)
    } else {
      // If the number is larger than current median, it should be inserted into largerHalf,
      // otherwise smallerHalf.
      if (x > median) {
        largerHalf.enqueue(x)
      } else {
        smallerHalf.enqueue(x)
      }
    }
    rebalance()
  }

  private[this] def rebalance(): Unit = {
    if (largerHalf.size - smallerHalf.size > 1) {
      smallerHalf.enqueue(largerHalf.dequeue())
    }
    if (smallerHalf.size - largerHalf.size > 1) {
      largerHalf.enqueue(smallerHalf.dequeue)
    }
  }

  def median: Double = {
    if (isEmpty) {
      throw new NoSuchElementException("MedianHeap is empty.")
    }
    if (largerHalf.size == smallerHalf.size) {
      (largerHalf.head + smallerHalf.head) / 2.0
    } else if (largerHalf.size > smallerHalf.size) {
      largerHalf.head
    } else {
      smallerHalf.head
    }
  }
}
