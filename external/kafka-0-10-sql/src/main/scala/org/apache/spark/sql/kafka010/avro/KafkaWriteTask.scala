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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.{ util => ju}

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._

/**
  * A simple trait for writing out data in a single Spark task, without any concerns about how
  * to commit or abort tasks. Exceptions thrown by the implementation of this class will
  * automatically trigger task aborts.
  */
private[kafka010] class KafkaWriteTask(
                                        producerConfiguration: ju.Map[String, Object],
                                        schema: StructType,
                                        topic: String,
                                        recordNamespace: String) {
  // used to synchronize with Kafka callbacks
  @volatile private var failedWrite: Exception = null
  //  private val projection = createProjection
  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _
  private val builder = SchemaBuilder.record(topic).namespace(recordNamespace)
  private val avroSchema: Schema = SchemaConverters.convertStructToAvro(
    schema, builder, recordNamespace)
  private lazy val converter = createConverterToAvro(schema, topic, recordNamespace)
  private lazy val internalRowConverter =
    CatalystTypeConverters.createToScalaConverter(schema).asInstanceOf[InternalRow => Row]
  private lazy val writer: SpecificDatumWriter[GenericRecord] = new SpecificDatumWriter[GenericRecord](avroSchema)
  private lazy val out: ByteArrayOutputStream = new ByteArrayOutputStream(1024 * 1024 * 10)
  private lazy val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)


  /**
    * Writes key value data out to topics.
    */
  def execute(iterator: Iterator[InternalRow]): Unit = {
    producer = CachedKafkaProducer.getOrCreate(producerConfiguration)
    while (iterator.hasNext && failedWrite == null) {
      out.reset()
      val currentRow: InternalRow = iterator.next()
      val avroRecord = converter(internalRowConverter(currentRow)).asInstanceOf[GenericData.Record]
      writer.write(avroRecord, encoder)
      encoder.flush()
      out.flush()
      //      val projectedRow = projection(currentRow)
      //      val topic = projectedRow.getUTF8String(0)
      //      val key = projectedRow.getBinary(1)
      //      val value = projectedRow.getBinary(2)
      //      if (topic == null) {
      //        throw new NullPointerException(s"null topic present in the data. Use the " +
      //          s"${KafkaSourceProvider.TOPIC_OPTION_KEY} option for setting a default topic.")
      //      }
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, null, out.toByteArray)
      val callback = new Callback() {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if (failedWrite == null && e != null) {
            failedWrite = e
          }
        }
      }
      producer.send(record, callback)
    }
  }

  def close(): Unit = {
    checkForErrors()
    if (producer != null) {
      producer.flush()
      checkForErrors()
      producer = null
    }
  }

  //  private def createProjection: UnsafeProjection = {
  //    val topicExpression = topic.map(Literal(_)).orElse {
  //      inputSchema.find(_.name == KafkaWriter.TOPIC_ATTRIBUTE_NAME)
  //    }.getOrElse {
  //      throw new IllegalStateException(s"topic option required when no " +
  //        s"'${KafkaWriter.TOPIC_ATTRIBUTE_NAME}' attribute is present")
  //    }
  //    topicExpression.dataType match {
  //      case StringType => // good
  //      case t =>
  //        throw new IllegalStateException(s"${KafkaWriter.TOPIC_ATTRIBUTE_NAME} " +
  //          s"attribute unsupported type $t. ${KafkaWriter.TOPIC_ATTRIBUTE_NAME} " +
  //          "must be a StringType")
  //    }
  //    val keyExpression = inputSchema.find(_.name == KafkaWriter.KEY_ATTRIBUTE_NAME)
  //      .getOrElse(Literal(null, BinaryType))
  //    keyExpression.dataType match {
  //      case StringType | BinaryType => // good
  //      case t =>
  //        throw new IllegalStateException(s"${KafkaWriter.KEY_ATTRIBUTE_NAME} " +
  //          s"attribute unsupported type $t")
  //    }
  //    val valueExpression = inputSchema
  //      .find(_.name == KafkaWriter.VALUE_ATTRIBUTE_NAME).getOrElse(
  //      throw new IllegalStateException("Required attribute " +
  //        s"'${KafkaWriter.VALUE_ATTRIBUTE_NAME}' not found")
  //    )
  //    valueExpression.dataType match {
  //      case StringType | BinaryType => // good
  //      case t =>
  //        throw new IllegalStateException(s"${KafkaWriter.VALUE_ATTRIBUTE_NAME} " +
  //          s"attribute unsupported type $t")
  //    }
  //    UnsafeProjection.create(
  //      Seq(topicExpression, Cast(keyExpression, BinaryType),
  //        Cast(valueExpression, BinaryType)), inputSchema)
  //  }

  private def checkForErrors(): Unit = {
    if (failedWrite != null) {
      throw failedWrite
    }
  }

  /**
    * This function constructs converter function for a given sparkSQL datatype. This is used in
    * writing Avro records out to disk
    */
  private def createConverterToAvro(
                                     dataType: DataType,
                                     structName: String,
                                     recordNamespace: String): (Any) => Any = {
    dataType match {
      case BinaryType => (item: Any) =>
        item match {
          case null => null
          case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
        }
      case ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | StringType | BooleanType => identity
      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      case TimestampType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Timestamp].getTime
      case DateType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Date].getTime
      case ArrayType(elementType, _) =>
        val elementConverter = createConverterToAvro(
          elementType,
          structName,
          SchemaConverters.getNewRecordNamespace(elementType, recordNamespace, structName))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val sourceArray = item.asInstanceOf[Seq[Any]]
            val targetArray = new ju.ArrayList[Any]()
            for (source <- sourceArray) {
              targetArray.add(elementConverter(source))
            }
            targetArray
          }
        }
      case MapType(StringType, valueType, _) =>
        val valueConverter = createConverterToAvro(
          valueType,
          structName,
          SchemaConverters.getNewRecordNamespace(valueType, recordNamespace, structName))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val javaMap = new ju.HashMap[String, Any]()
            item.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
              javaMap.put(key, valueConverter(value))
            }
            javaMap
          }
        }
      case structType: StructType =>
        val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
        val schema: Schema = SchemaConverters.convertStructToAvro(
          structType, builder, recordNamespace)
        val fieldConverters = structType.fields.map(field =>
          createConverterToAvro(
            field.dataType,
            field.name,
            SchemaConverters.getNewRecordNamespace(field.dataType, recordNamespace, field.name)))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = new GenericData.Record(schema)
            val convertersIterator = fieldConverters.iterator
            val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
            val rowIterator = item.asInstanceOf[Row].toSeq.iterator

            while (convertersIterator.hasNext) {
              val converter = convertersIterator.next()
              record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
            }
            record
          }
        }
    }
  }

}

