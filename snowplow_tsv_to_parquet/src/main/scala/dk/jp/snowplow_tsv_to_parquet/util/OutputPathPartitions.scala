package dk.jp.snowplow_tsv_to_parquet.util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
  * The possible partition keys. For example, event = "page_view" will result in a partition called "event=page_view"
  * when storing the data.
  */
case class OutputPathPartitions(event: String, dt: LocalDateTime) {
  private def getSaveDirectory: String = {
    val dtSuffix = dt.format(DateTimeFormatter.ofPattern("/'date'=yyyy-MM-dd/'hour'=HH"))
    s"event=$event$dtSuffix"
  }

  private def getSaveFileName: String = {
    dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH.'pq'"))
  }

  def getLocalSavePath: String = s"/tmp/snowplow_tsv_to_parquet/$getSaveDirectory/$getSaveFileName"
  def getRemoteSaveDirectory: String = s"snowplow/$getSaveDirectory"
  def getRemoteSavePath: String = s"$getRemoteSaveDirectory/$getSaveFileName"
}
