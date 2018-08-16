package dk.jp.snowplow_tsv_to_parquet

import java.io._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import dk.jp.snowplow_tsv_to_parquet.converters.{ContextExploder, TsvToAvroConverter}
import dk.jp.snowplow_tsv_to_parquet.sinks.AvroToParquetSink
import dk.jp.snowplow_tsv_to_parquet.sources.TsvSource
import dk.jp.snowplow_tsv_to_parquet.util.{S3Extension, Schemas}
import org.apache.avro.generic.GenericData
import org.slf4j.LoggerFactory

import scala.collection.parallel.ForkJoinTaskSupport
import scala.io.Source
import scala.language.implicitConversions

object Main {

  private val logger = LoggerFactory.getLogger("Main")

  // The level of parallelism to use when converting event files from S3 to Avro objects.
  private val parallelism = 4
  // The number of files to keep a connection in S3 open to at a time.
  private val inStreamBatchSize = parallelism * 3

  // Up the socket timeout to avoid "Connection reset" errors due to long living connections.
  private val s3ClientConfig = new ClientConfiguration()
  private val socketTimeout = 15 * 60 * 1000 // 15 minutes.
  s3ClientConfig.setSocketTimeout(socketTimeout)

  private implicit val s3: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withClientConfiguration(s3ClientConfig)
    .build()
  private implicit def s3ToS3Extension(s3: AmazonS3): S3Extension = new S3Extension(s3)

  def run(inBucket: String, outBucket: String, dtToProcess: LocalDateTime): Unit = {
    val prefix = getInputPrefix(dtToProcess)
    logger.info(s"Processing prefix $prefix...")

    val inStreamBatches = s3.getContent(inBucket, prefix, inStreamBatchSize)
    val sink = new AvroToParquetSink(Schemas.out)

    inStreamBatches.foreach { inStreams =>
      logger.info(s"Found ${inStreams.size} keys in prefix $prefix...")
      processBatch(inStreams, sink, dtToProcess)
    }

    logger.info("Closing sink and finalizing writing files to local storage...")
    val partsWritten = sink.close()

    logger.info("Uploading file(s) to S3...")
    partsWritten.par.foreach(s3.putObject(outBucket, _))

    logger.info("Done.")
  }

  private def processBatch(inStreams: Seq[InputStream], sink: AvroToParquetSink, dtToProcess: LocalDateTime): Unit = {
    val inStreamsPar = inStreams.par
    inStreamsPar.tasksupport = new ForkJoinTaskSupport(new java.util.concurrent.ForkJoinPool(parallelism))

    val records = inStreamsPar
      .flatMap(getRecords)
      .seq

    // ParquetWriter is not thread safe, so we can only write to it sequentially.
    logger.info("Writing batch to sink...")
    records.foreach(sink.write(_, dtToProcess))
  }

  private def getRecords(in: InputStream): Iterator[GenericData.Record] = {
    val ls = Source.fromInputStream(in).getLines()

    logger.info(s"Reading raw event file..")
    val input = TsvSource.read(ls, Schemas.in)

    logger.info(s"Exploding contexts..")
    val explodedInput = ContextExploder.explodeContexts(input)

    logger.info(s"Converting events to Avro..")
    TsvToAvroConverter.convert(explodedInput, Schemas.out)
  }

  private def getInputPrefix(dt: LocalDateTime): String = {
    val dtPrefix = dt.format(DateTimeFormatter.ofPattern("yyyy/MM/dd/HH"))
    s"snowplow/$dtPrefix/"
  }

  def main(args: Array[String]): Unit = {
    assert(args.length == 6, "Run with parameters {in bucket} {out bucket} {year} {month} {day} {hour}.")

    val inBucket = args(0)
    val outBucket = args(1)
    val year = args(2).toInt
    val month = args(3).toInt
    val day = args(4).toInt
    val hour = args(5).toInt
    val dt = LocalDateTime.of(year, month, day, hour, 0)

    logger.info(s"Working with input bucket $inBucket.")
    logger.info(s"Working with output bucket $outBucket.")
    logger.info(s"Working with date $dt.")

    run(inBucket, outBucket, dt)
  }

}
