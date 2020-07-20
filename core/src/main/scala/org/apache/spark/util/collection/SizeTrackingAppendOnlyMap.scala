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

/**
  *
  * Size Tracking Append Only Map的确继承了Append Only Map和SizeTracker。
  * Size Tracking Append Only Map采用代理模式重写了Append Only Map的三个方法
  * (包括update、change Value及grow Table) 。Size Tracking Append Only Map确保对data数组
  * 进行数据更新、缓存聚合等操作后， 调用Size Tracker的after Update方法完成采样； 在扩
  * 充data数组的容量后， 调用Size Tracker的reset Samples方法对样本进行重置， 以便于对
  * Append Only Map的大小估算更加准确。
  *
  *
 * An append-only map that keeps track of its estimated size in bytes.
 */
private[spark] class SizeTrackingAppendOnlyMap[K, V]
  extends AppendOnlyMap[K, V] with SizeTracker
{
  override def update(key: K, value: V): Unit = {
    super.update(key, value)
    super.afterUpdate()
  }

  override def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    val newValue = super.changeValue(key, updateFunc)
    super.afterUpdate()
    newValue
  }

  override protected def growTable(): Unit = {
    super.growTable()
    resetSamples()
  }
}
