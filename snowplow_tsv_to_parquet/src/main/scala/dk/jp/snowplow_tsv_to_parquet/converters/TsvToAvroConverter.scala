package dk.jp.snowplow_tsv_to_parquet.converters

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

object TsvToAvroConverter {
  def convert(in: Iterator[Array[Any]], schema: Schema): Iterator[GenericData.Record] = {
    in.map(convert(_, schema))
  }

  private def convert(in: Array[Any], schema: Schema): GenericData.Record = {
    val r = new GenericData.Record(schema)
    in.zipWithIndex.foreach { case (v, i) => r.put(i, v) }
    r
  }
}
