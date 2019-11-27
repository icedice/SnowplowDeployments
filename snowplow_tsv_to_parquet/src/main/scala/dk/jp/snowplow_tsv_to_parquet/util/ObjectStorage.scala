package dk.jp.snowplow_tsv_to_parquet.util

import java.io.{File, InputStream}
import java.util.zip.GZIPInputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsRequest
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

trait ObjectStorage {
  def getContent(bucket: String, prefix: String, batchSize: Int): Iterator[Seq[InputStream]]

  def putObject(bucket: String, parts: OutputPathPartitions): Unit
}

class S3ObjectStorage(s3: AmazonS3) extends ObjectStorage {

  private val logger = LoggerFactory.getLogger("S3Extension")

  private def getAllKeys(bucket: String, prefix: String): Seq[String] = {
    var listObjects = s3.listObjects(bucket, prefix)
    val keys = mutable.Buffer(listObjects.getObjectSummaries.asScala.map(_.getKey): _*)

    while (listObjects.isTruncated) {
      val req = new ListObjectsRequest()
        .withBucketName(bucket)
        .withMarker(listObjects.getNextMarker)
      listObjects = s3.listObjects(req)
      keys ++= listObjects.getObjectSummaries.asScala.map(_.getKey)
    }

    keys
  }

  private def getObjContent(bucket: String, key: String): InputStream = {
    logger.info(s"About to download $bucket/$key")
    val obj = s3.getObject(bucket, key)
    new GZIPInputStream(obj.getObjectContent)
  }

  /**
    * Get all objects as [[InputStream]]s from bucket's prefix. If we have files open for too long, we end up with
    * [[java.net.SocketException]]s from the S3 SDK. Therefore, we return a iterator of iterators allowing us to only
    * keep connections to the current 'batch' of streams open while still allowing us to process the 'batch' in parallel.
    */
  def getContent(bucket: String, prefix: String, batchSize: Int): Iterator[Seq[InputStream]] = {
    val allKeys = getAllKeys(bucket, prefix)
    logger.info(s"All keys are: $allKeys")
    allKeys.grouped(batchSize)
      .map(_.map(getObjContent(bucket, _)))
  }

  def putObject(bucket: String, parts: OutputPathPartitions): Unit = {
    s3.putObject(bucket, parts.getRemoteSavePath, new File(parts.getLocalSavePath))
  }

}
