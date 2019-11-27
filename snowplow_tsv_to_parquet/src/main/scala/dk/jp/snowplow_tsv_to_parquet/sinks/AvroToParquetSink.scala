package dk.jp.snowplow_tsv_to_parquet.sinks

import java.time.LocalDateTime

import dk.jp.snowplow_tsv_to_parquet.util.OutputPathPartitions
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.collection.mutable

class AvroToParquetSink(schema: Schema) {

  // For each partition (such as event type), we construct a ParquetWriter. Because we don't know which partition values
  // we'll see beforehand, we'll dynamically construct the writers using newWriter the first time we see a partition
  // value. This array then maps from a partition to its ParquetWriter which can be used to write a single Avro record.
  private val writers = mutable.Map[OutputPathPartitions, ParquetWriter[GenericData.Record]]()
  private def newWriter(parts: OutputPathPartitions): ParquetWriter[GenericData.Record] = {
    val pathForParts = new Path(s"/tmp/${parts.getSavePath}")
    AvroParquetWriter
      .builder[GenericData.Record](pathForParts)
      .withSchema(schema)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withDictionaryEncoding(true)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .build()
  }

  def write(out: GenericData.Record, dt: LocalDateTime): Unit = {
    val event = out.get("event_type").asInstanceOf[String]
    val parts = OutputPathPartitions(event, dt)
    // Find or create a new ParquetWriter for the record's partition.
    val writer = writers.getOrElseUpdate(parts, newWriter(parts))
    try {
      writer.write(out)
    } catch {
      case e: Throwable => ???
    }
  }

  /**
    * Closes the sink and returns the partitions that we ended up using.
    */
  def close(): Seq[OutputPathPartitions] = {
    writers.values.foreach(_.close())
    writers.keysIterator.toSeq
  }
}
