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

package org.apache.spark.sql.kafka010.avro

import java.util.concurrent.atomic.AtomicBoolean
import java.{util => ju}

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.types.{BinaryType, StringType}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.Utils

/**
  * The [[KafkaWriter]] class is used to write data from a batch query
  * or structured streaming query, given by a [[QueryExecution]], to Kafka.
  * The data is assumed to have a value column, and an optional topic and key
  * columns. If the topic column is missing, then the topic must come from
  * the 'topic' configuration option. If the key column is missing, then a
  * null valued key field will be added to the
  * [[org.apache.kafka.clients.producer.ProducerRecord]].
  */
private[kafka010] object KafkaWriter extends Logging {
  val TOPIC_ATTRIBUTE_NAME: String = "topic"
  val KEY_ATTRIBUTE_NAME: String = "key"
  val VALUE_ATTRIBUTE_NAME: String = "value"
  val FIRST_SCHEMA_INIT: AtomicBoolean = new AtomicBoolean(true)

  override def toString: String = "KafkaWriter"

  def validateQuery(
                     queryExecution: QueryExecution,
                     kafkaParameters: ju.Map[String, Object],
                     topic: Option[String] = None): Unit = {
    val schema = queryExecution.analyzed.output
    schema.find(_.name == TOPIC_ATTRIBUTE_NAME).getOrElse(
      if (topic.isEmpty) {
        throw new AnalysisException(s"topic option required when no " +
          s"'$TOPIC_ATTRIBUTE_NAME' attribute is present. Use the " +
          s"${KafkaSourceProvider.TOPIC_OPTION_KEY} option for setting a topic.")
      } else {
        Literal(topic.get, StringType)
      }
    ).dataType match {
      case StringType => // good
      case _ =>
        throw new AnalysisException(s"Topic type must be a String")
    }
    schema.find(_.name == KEY_ATTRIBUTE_NAME).getOrElse(
      Literal(null, StringType)
    ).dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new AnalysisException(s"$KEY_ATTRIBUTE_NAME attribute type " +
          s"must be a String or BinaryType")
    }
    schema.find(_.name == VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new AnalysisException(s"Required attribute '$VALUE_ATTRIBUTE_NAME' not found")
    ).dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new AnalysisException(s"$VALUE_ATTRIBUTE_NAME attribute type " +
          s"must be a String or BinaryType")
    }
  }

  def write(
             sparkSession: SparkSession,
             queryExecution: QueryExecution,
             kafkaParameters: ju.Map[String, Object],
             topic: Option[String],
             recordNamespace: String,
             schemaRegistry: Option[String],
             packageSize:Int ): Unit = {
    val structType = queryExecution.analyzed.schema
    //第一次运行，包含schemaRegistry地址时，自动注册schema
    if (FIRST_SCHEMA_INIT.getAndSet(false)) {
      val builder = SchemaBuilder.record(topic.get).namespace(recordNamespace)
      val schema: Schema = SchemaConverters.convertStructToAvro(
        structType, builder, recordNamespace)
      logInfo("Kafka_Avro Sink output schema: "+System.getProperty("line.separator") + schema.toString(true))

      if (schemaRegistry.isDefined) {
        try {
          val url = schemaRegistry.get
          val client = new CachedSchemaRegistryClient(url, 100)
          client.register(topic.get, schema)
        } catch {
          case e: Exception => logError("Kafka_Avro Sink register schema failed", e)
        }
      }
    }
    //    validateQuery(queryExecution, kafkaParameters, topic)
    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      queryExecution.toRdd.foreachPartition { iter =>
        val writeTask = new KafkaWriteTask(kafkaParameters, structType, topic.get, recordNamespace,packageSize)
        Utils.tryWithSafeFinally(block = writeTask.execute(iter))(
          finallyBlock = writeTask.close())
      }
    }
  }
}
