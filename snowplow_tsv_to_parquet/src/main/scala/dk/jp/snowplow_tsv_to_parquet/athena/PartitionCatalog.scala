package dk.jp.snowplow_tsv_to_parquet.athena

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.amazonaws.services.athena.model.{QueryExecutionContext, ResultConfiguration, StartQueryExecutionRequest}
import com.amazonaws.services.athena.{AmazonAthenaAsync, AmazonAthenaAsyncClientBuilder}
import dk.jp.snowplow_tsv_to_parquet.util.OutputPathPartitions
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object PartitionCatalog {
  lazy val client: AmazonAthenaAsync = AmazonAthenaAsyncClientBuilder.defaultClient()
  private val logger = LoggerFactory.getLogger("PartitionCatalog")

  /**
    * Use Athena to add partitions rather than a Glue Crawler. The Crawler will take a long time to run and we know
    * exactly which partitions we would like to add so we don't need the features of a Glue Crawler.
    * If the partitions gets messed up for some reason, we can run a Glue Crawler to get back on track.
    */
  def addPartitions(parts: Seq[OutputPathPartitions], bucket: String, partitionDatabase: String, athenaOutputLocation: String): Unit = {
    if (parts.isEmpty) {
      return
    }

    // We don't use the output for anything, but we have to provide a location so just generate a more-or-less unique
    // location within the given bucket and prefix.
    val fullOutputLocation = s"s3://$athenaOutputLocation/dt=${LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}"

    val qs = getQueryString(parts, bucket)
    logger.info(s"Executing query $qs...")

    val req = new StartQueryExecutionRequest()
      .withQueryString(qs)
      .withQueryExecutionContext(new QueryExecutionContext().withDatabase(partitionDatabase))
      .withResultConfiguration(new ResultConfiguration().withOutputLocation(fullOutputLocation))

    val res = client.startQueryExecutionAsync(req)

    Try(res.get()) match {
      case Success(_) => ()
      case Failure(t) =>
        logger.error("Unexpected error while updating partitions", t)
        // For some reason, the program does not stop if res.get() fails. Force stop it.
        sys.exit(1)
    }
  }

  private def getQueryString(parts: Seq[OutputPathPartitions], bucket: String): String = {
    val partQueries = parts.map(getPartitionQuery(_, bucket))
    s"ALTER TABLE snowplow ADD IF NOT EXISTS\n${partQueries.mkString("\n")}"
  }

  private def getPartitionQuery(part: OutputPathPartitions, bucket: String): String = {
    val location = s"s3://$bucket/snowplow/${part.getSavePath}"
    val dt = part.dt
    s"PARTITION (event = '${part.event}', year = ${dt.getYear}, month = ${dt.getMonthValue}, day = ${dt.getDayOfMonth}, hour = ${dt.getHour}) LOCATION '$location'"
  }

}
