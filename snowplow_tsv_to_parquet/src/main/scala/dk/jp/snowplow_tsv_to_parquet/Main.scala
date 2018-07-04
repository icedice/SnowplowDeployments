package dk.jp.snowplow_tsv_to_parquet

import java.io._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import dk.jp.snowplow_tsv_to_parquet.converters.{ContextExploder, TsvToAvroConverter}
import dk.jp.snowplow_tsv_to_parquet.sinks.AvroToParquetSink
import dk.jp.snowplow_tsv_to_parquet.sources.TsvSource
import dk.jp.snowplow_tsv_to_parquet.util.{S3Extension, Schemas}
import org.apache.avro.generic.GenericData
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.language.implicitConversions

object Main {

  private val logger = LoggerFactory.getLogger("Main")

  private val inBucket = sys.env.getOrElse("S3_INPUT_BUCKET", "jpmedier-datalake-dev")
  logger.info(s"Working with input bucket $inBucket.")
  private val outBucket = sys.env.getOrElse("S3_OUTPUT_BUCKET", "behavior-datalake-dev")
  logger.info(s"Working with output bucket $outBucket.")

  private implicit val s3: AmazonS3 = AmazonS3ClientBuilder.defaultClient()
  private implicit def s3ToS3Extension(s3: AmazonS3): S3Extension = new S3Extension(s3)

  def run(dtToProcess: LocalDateTime): Unit = {
    logger.info(s"Working with date $dtToProcess.")

    val prefix = getInputPrefix(dtToProcess)
    logger.info(s"Processing prefix $prefix...")

    val inStreams = s3.getContent(inBucket, prefix)
    logger.info(s"Found ${inStreams.size} keys in prefix $prefix...")

    val records = inStreams
      .par
      .flatMap(getRecords)
      .seq

    // ParquetWriter is not thread safe, so we can only write to it sequentially.
    logger.info("Writing Parquet file(s)...")
    val sink = new AvroToParquetSink(Schemas.out)
    records.foreach(sink.write(_, dtToProcess))
    val partsWritten = sink.close()

    logger.info("Uploading file(s) to S3...")
    partsWritten.par.foreach(s3.putObject(outBucket, _))

    logger.info("Done.")
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
    run(LocalDateTime.of(2018, 7, 4, 12, 0))
  }

}
