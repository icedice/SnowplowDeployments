package dk.jp.snowplow_tsv_to_parquet

import java.time.LocalDateTime

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider
import dk.jp.snowplow_tsv_to_parquet.Main.run
import dk.jp.snowplow_tsv_to_parquet.athena.AthenaPartitionCatalog
import dk.jp.snowplow_tsv_to_parquet.factory.AmazonClientFactory
import dk.jp.snowplow_tsv_to_parquet.util.S3ObjectStorage
import org.slf4j.LoggerFactory

/**
  * Main method for running on a local machine.
  * Can be switched from running dev/test to prod by setting the AWS_ASSUME_ROLE_ARN and AWS_ROLE_SESSION_NAME
  * environment variables.
  */
object LocalMain {

  private val logger = LoggerFactory.getLogger("LocalMain")

  def main(args: Array[String]): Unit = {
    val year = 2019
    val month = 11
    val day = 26
    val hour = 7

    val inBucket = sys.env.getOrElse("IN_BUCKET", throw new IllegalArgumentException("IN_BUCKET environment variable not set."))
    logger.info(s"Working with input bucket $inBucket.")
    val outBucket = sys.env.getOrElse("OUT_BUCKET", throw new IllegalArgumentException("OUT_BUCKET environment variable not set."))
    logger.info(s"Working with output bucket $outBucket.")
    val partitionDatabase = sys.env.getOrElse("PARTITION_DATABASE", throw new IllegalArgumentException("PARTITION_DATABASE environment variable not set."))
    logger.info(s"Working with partition database $partitionDatabase.")
    val athenaOutputLocation = sys.env.getOrElse("ATHENA_OUTPUT_LOCATION", throw new IllegalArgumentException("ATHENA_OUTPUT_LOCATION environment variable not set."))
    logger.info(s"Working with Athena output location $athenaOutputLocation.")

    val (s3, athena) = sys.env.get("AWS_ASSUME_ROLE_ARN") match {
      case Some(roleArn) =>
        logger.info(s"Working with role ARN: '$roleArn'")
        val roleSessionName = sys.env.getOrElse("AWS_ROLE_SESSION_NAME", throw new IllegalArgumentException("AWS_ROLE_SESSION_NAME environment variable not set."))
        logger.info(s"Working with session name: '$roleSessionName'")
        val assumeRoleProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName)
          .build()

        (AmazonClientFactory.createS3Client(assumeRoleProvider), AmazonClientFactory.createAthenaClient(assumeRoleProvider))
      case None =>
        (AmazonClientFactory.createS3Client(), AmazonClientFactory.createAthenaClient())
    }

    val dt = LocalDateTime.of(year, month, day, hour, 0)
    logger.info(s"Working with date $dt.")

    val objStorage = new S3ObjectStorage(s3)
    val partitionCatalog = new AthenaPartitionCatalog(partitionDatabase, athenaOutputLocation, athena)
    run(inBucket, outBucket, dt, objStorage, partitionCatalog)
  }

}
