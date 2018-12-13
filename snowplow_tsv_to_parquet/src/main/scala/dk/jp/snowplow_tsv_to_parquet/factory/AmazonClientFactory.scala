package dk.jp.snowplow_tsv_to_parquet.factory

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider
import com.amazonaws.services.athena.{AmazonAthena, AmazonAthenaClientBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

object AmazonClientFactory {

  // Up the socket timeout to avoid "Connection reset" errors due to long living connections.
  private val socketTimeout = 15 * 60 * 1000 // 15 minutes.
  private val s3ClientConfig = new ClientConfiguration().withSocketTimeout(socketTimeout)

  def createAthenaClient(): AmazonAthena = {
    AmazonAthenaClientBuilder.defaultClient()
  }

  def createProdAssumeAthenaClient(assumeRoleProvider: STSAssumeRoleSessionCredentialsProvider): AmazonAthena = {
    AmazonAthenaClientBuilder.standard().withCredentials(assumeRoleProvider).build()
  }

  def createS3Client(): AmazonS3 = {
    AmazonS3ClientBuilder
      .standard()
      .withClientConfiguration(s3ClientConfig)
      .build()
  }

  def createS3Client(assumeRoleProvider: STSAssumeRoleSessionCredentialsProvider): AmazonS3 = {
    AmazonS3ClientBuilder
      .standard()
      .withCredentials(assumeRoleProvider)
      .withClientConfiguration(s3ClientConfig)
      .withRegion("eu-west-1")
      .build()
  }

}
