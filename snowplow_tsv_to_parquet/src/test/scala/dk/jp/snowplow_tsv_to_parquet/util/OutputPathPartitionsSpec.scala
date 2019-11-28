package dk.jp.snowplow_tsv_to_parquet.util

import java.time.LocalDateTime

import org.scalatest.{MustMatchers, WordSpec}

class OutputPathPartitionsSpec extends WordSpec with MustMatchers {
  "OutputPathPartitions instances" should {
    "have correct local save path" in {
      val p = OutputPathPartitions("an event", LocalDateTime.of(2020, 2, 27, 12, 34, 56))
      p.getLocalSavePath mustBe "/tmp/snowplow_tsv_to_parquet/event=an event/date=2020-02-27/hour=12/2020-02-27_12.pq"
    }
    
    "have correct remote save directory" in {
      val p = OutputPathPartitions("an event", LocalDateTime.of(2020, 2, 27, 8, 34, 56))
      p.getRemoteSaveDirectory mustBe "snowplow/event=an event/date=2020-02-27/hour=08"
    }
    
    "have correct remote save path" in {
      val p = OutputPathPartitions("an event", LocalDateTime.of(2020, 5, 27, 12, 34, 56))
      p.getRemoteSavePath mustBe "snowplow/event=an event/date=2020-05-27/hour=12/2020-05-27_12.pq"
    }
  }
}
