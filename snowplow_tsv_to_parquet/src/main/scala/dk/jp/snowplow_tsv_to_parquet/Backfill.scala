//package dk.jp.snowplow_tsv_to_parquet
//
//import java.time.LocalDateTime
//
//import dk.jp.snowplow_tsv_to_parquet.Main.run
//import org.slf4j.LoggerFactory
//
//// See README if the parquet files should be backfilled.
//object Backfill {
//
//  private val logger = LoggerFactory.getLogger("Backfill")
//
//  def main(args: Array[String]): Unit = {
//
//    val year = 2018
//    val month = 11
//    val startDay = 22
//    val endDay = 22
//
//    val hours = List.range(0,24)
//    val days = List.range(startDay, endDay+1)
//
//    val inBucket = sys.env.getOrElse("IN_BUCKET", throw new IllegalArgumentException("IN_BUCKET environment variable not set."))
//    logger.info(s"Working with input bucket $inBucket.")
//    val outBucket = sys.env.getOrElse("OUT_BUCKET", throw new IllegalArgumentException("OUT_BUCKET environment variable not set."))
//    logger.info(s"Working with output bucket $outBucket.")
//    val partitionDatabase = sys.env.getOrElse("PARTITION_DATABASE", throw new IllegalArgumentException("PARTITION_DATABASE environment variable not set."))
//    logger.info(s"Working with partition database $partitionDatabase.")
//    val athenaOutputLocation = sys.env.getOrElse("ATHENA_OUTPUT_LOCATION", throw new IllegalArgumentException("ATHENA_OUTPUT_LOCATION environment variable not set."))
//    logger.info(s"Working with Athena output location $athenaOutputLocation.")
//
//    for (day <- days) {
//      for (hour <- hours) {
//        val dates = LocalDateTime.of(year, month, day, hour, 0)
//        run(inBucket, outBucket, dates, partitionDatabase, athenaOutputLocation)
//      }
//    }
//  }
//
//}
