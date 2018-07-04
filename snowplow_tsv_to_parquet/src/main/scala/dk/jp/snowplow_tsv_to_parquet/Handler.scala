package dk.jp.snowplow_tsv_to_parquet

import java.time.LocalDateTime

import util.LambdaEvent

class Handler {

  def handler(event: LambdaEvent): Unit = {
    val dt = LocalDateTime.of(event.getYear, event.getMonth, event.getDay, event.getHour, 0)
    Main.run(dt)
  }

}
