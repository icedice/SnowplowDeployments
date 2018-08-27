package dk.jp.snowplow_tsv_to_parquet.util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
  * The possible partition keys. For example, event = "page_view" will result in a partition called "event=page_view"
  * when storing the data.
  */
case class OutputPathPartitions(event: String, dt: LocalDateTime) {
  def getSavePath: String = {
    val dtSuffix = dt.format(DateTimeFormatter.ofPattern("/'year'=yyyy/'month'=MM/'day'=dd/'hour'=HH/yyyyMMdd_HH.'pq'"))
    s"event=$event$dtSuffix"
  }
}
