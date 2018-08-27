package dk.jp.snowplow_tsv_to_parquet.util

import java.io.{File, InputStream}
import java.util.zip.GZIPInputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsRequest

import scala.collection.JavaConverters._
import scala.collection.mutable

class S3Extension(s3: AmazonS3) {

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
    val obj = s3.getObject(bucket, key)
    new GZIPInputStream(obj.getObjectContent)
  }

  /**
    * Get all objects as [[InputStream]]s from bucket's prefix. If we have files open for too long, we end up with
    * [[java.net.SocketException]]s from the S3 SDK. Therefore, we return a iterator of iterators allowing us to only
    * keep connections to the current 'batch' of streams open while still allowing us to process the 'batch' in parallel.
    */
  def getContent(bucket: String, prefix: String, batchSize: Int): Iterator[Seq[InputStream]] = {
    getAllKeys(bucket, prefix)
      .grouped(batchSize)
      .map(_.map(getObjContent(bucket, _)))
  }

  def putObject(bucket: String, parts: OutputPathPartitions): Unit = {
    s3.putObject(bucket, s"snowplow/${parts.getSavePath}", new File(s"/tmp/${parts.getSavePath}"))
  }

}
