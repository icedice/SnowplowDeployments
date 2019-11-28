package dk.jp.snowplow_tsv_to_parquet

import java.io.InputStream
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.{ArrayList => JavaArrayList}

import dk.jp.snowplow_tsv_to_parquet.util.{ObjectStorage, OutputPathPartitions, PartitionCatalog}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.scalamock.scalatest.MockFactory
import org.scalatest.{MustMatchers, WordSpec}

import scala.collection.mutable
import scala.collection.JavaConverters._

class MainSpec extends WordSpec with MockFactory with MustMatchers {

  private def resourceToInputStream(f: String): InputStream = getClass.getResource(s"/$f").openStream()

  /**
    * Ignore the ObjectStorage and PartitionCatalog calls and only focus on the Parquet files. The other parts are 
    * tested elsewhere.
    */
  private def runAndGenerateParquetFile(resourceFileName: String, dtToProcess: LocalDateTime = LocalDateTime.of(2011, 10, 9, 5, 0, 0)): Unit = {
    val events = resourceToInputStream(resourceFileName)

    val objStorage = mock[ObjectStorage]
    (objStorage.getContent _)
      .expects(*, *, *)
      .returns(Seq(Seq(events)).iterator)

    (objStorage.putObject _).expects(*, *).anyNumberOfTimes()

    val partitionCatalog = mock[PartitionCatalog]
    (partitionCatalog.addPartitions _).expects(*, *).anyNumberOfTimes()

    Main.run("in bucket", "out bucket", dtToProcess, objStorage, partitionCatalog)
  }
  
  private def readParquetFile(event: String, dt: LocalDateTime = LocalDateTime.of(2011, 10, 9, 5, 0, 0)): Seq[GenericRecord] = {
    val path = new Path(s"/tmp/snowplow_tsv_to_parquet/event=$event/${dt.format(DateTimeFormatter.ofPattern("/'date'=yyyy-MM-dd/'hour'=HH/yyyy-MM-dd_HH'.pq'"))}")
    val inputFile = HadoopInputFile.fromPath(path, new Configuration())
    val reader = AvroParquetReader.builder[GenericRecord](inputFile).build
    
    val result = mutable.Buffer[GenericRecord]()
    var next = reader.read()
    while (next != null) {
      result += next
      next = reader.read()
    }
    
    result
  }
  
  // More-or-less end-to-end tests of the system with external dependencies mocked.
  "Main.run" should {
    "not do anything if the input is empty" in {
      val objStorage = mock[ObjectStorage]
      (objStorage.getContent _)
        .expects("in bucket", "snowplow/2019/11/27/16/", 12 /* Batch size hardcoded in Main. */)
        .returns(Seq(Seq()).iterator)
        .once()

      val partitionCatalog = mock[PartitionCatalog]
      (partitionCatalog.addPartitions _)
        .expects(Seq(), "out bucket")
        .once()

      // All other calls to mocks are unexpected.

      val dtToProcess = LocalDateTime.of(2019, 11, 27, 16, 26, 0)

      Main.run("in bucket", "out bucket", dtToProcess, objStorage, partitionCatalog)
    }

    "convert tsv from object storage and update partitions" in {
      val dtToProcess = LocalDateTime.of(2019, 11, 27, 16, 26, 0)

      val events = resourceToInputStream("MainSpec_end_to_end_3_events.tsv")

      // There's page_pings and a page_view in the events. 
      val expectedPartition1 = OutputPathPartitions("page_view", dtToProcess)
      val expectedPartition2 = OutputPathPartitions("page_ping", dtToProcess)

      val objStorage = mock[ObjectStorage]
      (objStorage.getContent _)
        .expects("in bucket", "snowplow/2019/11/27/16/", 12 /* Batch size hardcoded in Main. */)
        .returns(Seq(Seq(events)).iterator)

      (objStorage.putObject _).expects("out bucket", expectedPartition1).once()
      (objStorage.putObject _).expects("out bucket", expectedPartition2).once()

      val partitionCatalog = mock[PartitionCatalog]
      (partitionCatalog.addPartitions _)
        .expects(Seq(expectedPartition1, expectedPartition2), "out bucket")
        .once()

      Main.run("in bucket", "out bucket", dtToProcess, objStorage, partitionCatalog)
      // In this test we only verify the mocks and not the actual Parquet output. There's other tests for that part.
    }

    "convert tsv to parquet and have correct number of columns" in {
      // This test ensures that you don't add new output columns without looking at the tests.
      val numExpectedColumns = 148
      
      val dtToProcess = LocalDateTime.of(2019, 11, 27, 16, 26, 0)
      runAndGenerateParquetFile("MainSpec_end_to_end_3_events.tsv", dtToProcess)
      
      val pvs = readParquetFile("page_view", dtToProcess)
      pvs.head.getSchema.getFields.size() mustBe numExpectedColumns 

      val pps = readParquetFile("page_ping", dtToProcess)
      pps.head.getSchema.getFields.size() mustBe numExpectedColumns
      pps(1).getSchema.getFields.size() mustBe numExpectedColumns
    }

    "convert tsv to pq and extract access agreement columns" in {
      runAndGenerateParquetFile("MainSpec_end_to_end_access_agreements.tsv")

      val pvs = readParquetFile("page_view")
      // The first event has a valid AA context with 2 entries (with different accounts but same ids).
      // The properties are stored with schema array<string> in the files making it cumbersome to extract and compare to
      // Seq[String].
      pvs.head.get("access_agreement_accounts").asInstanceOf[JavaArrayList[Utf8]].asScala.map(_.toString) mustBe Seq("K00370461", "K00370462")
      pvs.head.get("access_agreement_ids").asInstanceOf[JavaArrayList[Utf8]].asScala.map(_.toString) mustBe Seq("1000013", "1000013")
      // The second has no AA context.
      pvs(1).get("access_agreement_accounts") mustBe null
      pvs(1).get("access_agreement_ids") mustBe null
    }
    
    "not include events by CookieInformationScanner" in {
      runAndGenerateParquetFile("MainSpec_end_to_end_cookieinformationscanner.tsv")

      val pvs = readParquetFile("page_view")
      // There's 4 pageviews but 2 of them are made by CookieInformationScanner.
      pvs.size == 2
      pvs.head.get("useragent").toString mustNot include("CookieInformationScanner")
      pvs(1).get("useragent").toString mustNot include("CookieInformationScanner")
    }
    
    "extract anon_id and user_id correctly" in {
      runAndGenerateParquetFile("MainSpec_end_to_end_anon_id_user_id.tsv")

      val pvs = readParquetFile("page_ping")
      // There's 4 pageviews but 2 of them are made by CookieInformationScanner.
      pvs.size == 2
      pvs.head.get("anon_id").toString mustBe "2e4fae65-fdde-4d3d-911a-3175f3670f0e"
      pvs.head.get("user_id") mustBe null // "anon" is converted to null.
      
      pvs(1).get("anon_id").toString mustBe "2e4fae65-fdde-4d3d-911a-3175f3670f0e"
      pvs(1).get("user_id").toString mustBe "4c11def2-7b18-4951-9a5d-9f2732ad2204"
      
      pvs(2).get("anon_id").toString mustBe "NOTSET"
      pvs(2).get("user_id").toString mustBe "d5230891-1736-42bb-abb4-7e40b4b3cb67"
    }
    
    "output section_path_id as an underscore separated string" in {
      // section_path_id is an underscore separated string for legacy reasons.
      runAndGenerateParquetFile("MainSpec_end_to_end_3_events.tsv")
      val pvs = readParquetFile("page_view")
      pvs.head.get("section_path_id").toString mustBe "340_2016_2025_2164"
    }
    
    "set some timestamps to null if they are not parseable" in {
      // Some timestamps may not be parseable because they are sent by the client. For example with year 16517.
      // dvce_sent_tstamp and refr_device_tstamp and dvce_created_tstamp are invalid for the first event but that's OK.
      runAndGenerateParquetFile("MainSpec_end_to_end_invalid_device_tstamp.tsv")
      
      val pvs = readParquetFile("page_view")
      pvs.head.get("dvce_created_tstamp") mustBe null
      pvs.head.get("dvce_sent_tstamp") mustBe null
      pvs.head.get("refr_device_tstamp") mustBe null

      pvs(1).get("dvce_created_tstamp") mustBe timestampToEpochMilli("2019-11-27 10:05:07.706")
      pvs(1).get("dvce_sent_tstamp") mustBe timestampToEpochMilli("2019-11-27 10:05:07.804")
      pvs(1).get("refr_device_tstamp") mustBe timestampToEpochMilli("2021-11-27 10:05:07.804")
    }
    
    "crash if certain timestamps are not parseable" in {
      // Unlike the device timestamps which are not always parseable, the collector timestamp must be valid as it is set
      // server side.
      intercept[IllegalArgumentException] {
        // collector_tstamp has year 16517 and we expect to crash on this.
        runAndGenerateParquetFile("MainSpec_end_to_end_invalid_collector_tstamp.tsv")
      }.getMessage must startWith("16517-11-27 10:04:46.602 does not match any of")
    }
  }

  private def timestampToEpochMilli(s: String) = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    val parsedDate = LocalDateTime.parse(s, formatter)
    parsedDate.toInstant(ZoneOffset.UTC).toEpochMilli
  }

}
