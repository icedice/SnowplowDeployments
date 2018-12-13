package dk.jp.snowplow_tsv_to_parquet

import java.time.LocalDateTime

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider
import dk.jp.snowplow_tsv_to_parquet.Main.run
import dk.jp.snowplow_tsv_to_parquet.factory.AmazonClientFactory
import org.slf4j.LoggerFactory

/**
  * Object / Main method for running on a local machine.
  * Can be switched from running dev/test to prod, with... TBA
  */
object LocalMain {

  private val logger = LoggerFactory.getLogger("LocalMain")

  def main(args: Array[String]): Unit = {
    val year = 2018
    val month = 12
    val day = 3
    val hour = 2

    val inBucket = sys.env.getOrElse("IN_BUCKET", throw new IllegalArgumentException("IN_BUCKET environment variable not set."))
    logger.info(s"Working with input bucket $inBucket.")
    val outBucket = sys.env.getOrElse("OUT_BUCKET", throw new IllegalArgumentException("OUT_BUCKET environment variable not set."))
    logger.info(s"Working with output bucket $outBucket.")
    val partitionDatabase = sys.env.getOrElse("PARTITION_DATABASE", throw new IllegalArgumentException("PARTITION_DATABASE environment variable not set."))
    logger.info(s"Working with partition database $partitionDatabase.")
    val athenaOutputLocation = sys.env.getOrElse("ATHENA_OUTPUT_LOCATION", throw new IllegalArgumentException("ATHENA_OUTPUT_LOCATION environment variable not set."))
    logger.info(s"Working with Athena output location $athenaOutputLocation.")

    val roleArn = sys.env.getOrElse("AWS_ASSUME_ROLE_ARN", throw new IllegalArgumentException("AWS_ASSUME_ROLE_ARN environment variable not set."))
    logger.info(s"Working with rolearn: '$roleArn'")
    val roleSessionName = sys.env.getOrElse("AWS_ROLE_SESSION_NAME", throw new IllegalArgumentException("AWS_ROLE_SESSION_NAME environment variable not set."))
    logger.info(s"Working with session name: '$roleSessionName'")
    val assumeRoleProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName)
      .build()

    val s3 = AmazonClientFactory.createS3Client(assumeRoleProvider)
    val athena = AmazonClientFactory.createProdAssumeAthenaClient(assumeRoleProvider)

    //    (0 to 23).foreach { hour =>
    val dt = LocalDateTime.of(year, month, day, hour, 0)
    logger.info(s"Working with date $dt.")
    run(inBucket, outBucket, dt, partitionDatabase, athenaOutputLocation, s3, athena)
    //    }
  }

}
