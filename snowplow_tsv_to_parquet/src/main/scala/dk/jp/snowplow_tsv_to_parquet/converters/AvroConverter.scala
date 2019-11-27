package dk.jp.snowplow_tsv_to_parquet.converters

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

object AvroConverter {
  /**
    * Convert a sequence of rows to Avro.
    */
  def toAvro(in: Iterator[Array[Any]], schema: Schema): Iterator[GenericData.Record] = {
    in.map(toAvro(_, schema))
  }

  /**
    * Convert an individual row to Avro.
    */
  private def toAvro(in: Array[Any], schema: Schema): GenericData.Record = {
    val r = new GenericData.Record(schema)
    in.zipWithIndex.foreach { case (v, i) => r.put(i, v) }
    r
  }
}
