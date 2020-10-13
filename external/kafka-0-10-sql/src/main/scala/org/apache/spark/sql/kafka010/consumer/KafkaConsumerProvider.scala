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

package org.apache.spark.sql.kafka010.consumer

import java.{util => ju}

import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.spark.util.Utils

private[kafka010] object KafkaConsumerProvider {

  private val CONSUMER_SUPPLIER_OVERRIDE : String = "consumer.supplier.override"

  /**
   * Create Kafka Consumer from Kafka parameters
   */
  def createKafkaConsumer(kafkaParams: ju.Map[String, Object]):
  Consumer[Array[Byte], Array[Byte]] = {
    var consumer: Consumer[Array[Byte], Array[Byte]] = null
    if (kafkaParams.containsKey(CONSUMER_SUPPLIER_OVERRIDE)) {
      consumer = Utils.classForName(kafkaParams.get(CONSUMER_SUPPLIER_OVERRIDE)
        .asInstanceOf[String]).getDeclaredConstructor(classOf[ju.Map[String, Object]])
        .newInstance(kafkaParams).asInstanceOf[Consumer[Array[Byte], Array[Byte]]];
    } else {
      consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
    }
    consumer
  }
}

