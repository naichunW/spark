package org.apache.spark.sql.kafka010

import java.nio.ByteBuffer
import java.util

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object AvroDeserialize {
  def decoder = (schemaString: String, consumerRecords: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]]) => {
    val data = ListBuffer[InternalRow]()
    val schema = new Schema.Parser().parse(schemaString)
    val size = schema.getFields.size()
    val reader = new SpecificDatumReader[GenericRecord](schema)
    val decoderFactory = DecoderFactory.get()
    var decoder: BinaryDecoder = null
    val record: GenericData.Record = new GenericData.Record(schema)
    val rowData = ListBuffer[AnyRef]()

    for (consumerRecord <- consumerRecords) {
      val bytes: Array[Byte] = consumerRecord.value
      decoder = decoderFactory.binaryDecoder(bytes, decoder)
      while (!decoder.isEnd) {
        reader.read(record, decoder)
        rowData.clear()
        for (index <- 0 until size) {
          val value: AnyRef = record.get(index)
          // 不支持
          if (value.isInstanceOf[Utf8]) {
            rowData.append(value.toString)
          } else if (value.isInstanceOf[ByteBuffer]) {
            rowData.append(value.asInstanceOf[ByteBuffer].array())
          } else if (value.isInstanceOf[util.HashMap[String,Object]]) {
            rowData.append(value.asInstanceOf[util.HashMap[String,Object]].toMap)
          } else if (value.isInstanceOf[GenericData.Array[Object]]) {
            rowData.append(value.asInstanceOf[GenericData.Array[Object]].toSeq)
          } else {
            rowData.append(value)
          }
        }
        data.append(InternalRow.fromSeq(rowData))
      }
    }
    data.iterator
  }


  /**
    * 将avro schema转换为DDL语句中用的字符串
    * eg
    * {"type":"record","name":"test","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}
    * 转换为
    * name string,age int
    *
    * @return
    */
  def convert(schema: Schema): String = {
    val struct: String = schema.getType match {
      case Schema.Type.NULL => null
      case Schema.Type.STRING => "string"
      case Schema.Type.INT => "int"
      case Schema.Type.LONG => "long"
      case Schema.Type.BOOLEAN => "boolean"
      case Schema.Type.BYTES => "binary"
      case Schema.Type.FLOAT => "float"
      case Schema.Type.DOUBLE => "double"
      case Schema.Type.UNION => {
        var ss: String = null
        schema.getTypes.toSeq.foreach(s => {
          val sss = convert(s)
          if (null != sss) {
            ss = sss
          }
        })
        ss
      }
      case Schema.Type.ARRAY => {
        val s = convert(schema.getElementType)
        s"array<$s>"
      }
      case Schema.Type.MAP => {
        val s = convert(schema.getValueType)
        s"map<string,$s>"
      }
      case _ => "string"
    }
    struct
  }
}
