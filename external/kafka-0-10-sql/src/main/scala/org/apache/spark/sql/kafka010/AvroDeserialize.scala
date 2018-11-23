package org.apache.spark.sql.kafka010

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable.ListBuffer

object AvroDeserialize {
  def decoder=(schemaString: String,consumerRecords: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]])=>{
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
          if (value.isInstanceOf[Utf8]) {
            rowData.append(value.toString)
          } else {
            rowData.append(value)
          }
        }
        data.append(InternalRow.fromSeq(rowData))
      }
    }
    data.iterator
  }
}
