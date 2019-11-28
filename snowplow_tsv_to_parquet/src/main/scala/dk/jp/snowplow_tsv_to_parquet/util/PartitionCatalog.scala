package dk.jp.snowplow_tsv_to_parquet.util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.amazonaws.services.athena.AmazonAthena
import com.amazonaws.services.athena.model._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

trait PartitionCatalog {
  def addPartitions(parts: Seq[OutputPathPartitions], bucket: String): Unit
}

class AthenaPartitionCatalog(partitionDatabase: String, athenaOutputLocation: String, client: AmazonAthena) extends PartitionCatalog {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Use Athena to add partitions rather than a Glue Crawler. The Crawler will take a long time to run and we know
    * exactly which partitions we would like to add so we don't need the features of a Glue Crawler.
    * If the partitions gets messed up for some reason, we can run a Glue Crawler to get back on track.
    */
  def addPartitions(parts: Seq[OutputPathPartitions], bucket: String): Unit = {
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

    val res = client.startQueryExecution(req)

    waitForQueryToComplete(client, res)
  }

  private def getQueryString(parts: Seq[OutputPathPartitions], bucket: String): String = {
    val partQueries = parts.map(getPartitionQuery(_, bucket))
    s"ALTER TABLE snowplow ADD IF NOT EXISTS\n${partQueries.mkString("\n")}"
  }

  private def getPartitionQuery(part: OutputPathPartitions, bucket: String): String = {
    val location = s"s3://$bucket/${part.getRemoteSavePath}"
    val dt = part.dt
    f"PARTITION (event = '${part.event}', `date` = '${dt.toLocalDate.toString}', hour = ${dt.getHour}%02d) LOCATION '$location'"
  }

  private def waitForQueryToComplete(client: AmazonAthena, res: StartQueryExecutionResult): Unit = {
    val getQueryExecutionRequest = new GetQueryExecutionRequest().withQueryExecutionId(res.getQueryExecutionId)
    
    val sleepBetweenChecks = 2000L
    var numChecks = 0
    while (true) {
      if ((numChecks * sleepBetweenChecks).millis > 5.minutes) {
        throw new RuntimeException("Timeout while waiting for Athena query to finish.")
      }

      val getQueryExecutionResult = client.getQueryExecution(getQueryExecutionRequest)
      val queryState = getQueryExecutionResult.getQueryExecution.getStatus.getState

      queryState match {
        case "FAILED" => throw new RuntimeException("Query failed to with error message: " + getQueryExecutionResult.getQueryExecution.getStatus.getStateChangeReason)
        case "CANCELLED" => throw new RuntimeException("Query was cancelled.")
        case "SUCCEEDED" =>
          // Break out of the infinite loop.
          return
        case _ =>
          // Sleep an amount of time before retrying again.
          Thread.sleep(sleepBetweenChecks)
      }

      numChecks += 1
      logger.debug("Current state for the Athena query is: " + queryState)
    }
  }

}
