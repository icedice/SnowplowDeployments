package dk.jp.snowplow_tsv_to_parquet

import java.io.InputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{ArrayList => JavaArrayList}

import dk.jp.snowplow_tsv_to_parquet.athena.PartitionCatalog
import dk.jp.snowplow_tsv_to_parquet.util.{ObjectStorage, OutputPathPartitions}
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
  private def runAndGenerateParquetFile(resourceFileName: String, dtToProcess: LocalDateTime): Unit = {
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
  
  private def readParquetFile(event: String, dt: LocalDateTime): Seq[GenericRecord] = {
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
      val dtToProcess = LocalDateTime.of(2011, 10, 9, 5, 0, 0)
      runAndGenerateParquetFile("MainSpec_end_to_end_access_agreements.tsv", dtToProcess)

      val pvs = readParquetFile("page_view", dtToProcess)
      // The first event has a valid AA context with 2 entries (with different accounts but same ids).
      // The properties are stored with schema array<string> in the files making it cumbersome to extract and compare to
      // Seq[String].
      pvs.head.get("access_agreement_accounts").asInstanceOf[JavaArrayList[Utf8]].asScala.map(_.toString) mustBe Seq("K00370461", "K00370462")
      pvs.head.get("access_agreement_ids").asInstanceOf[JavaArrayList[Utf8]].asScala.map(_.toString) mustBe Seq("1000013", "1000013")
      // The second has no AA context.
      pvs(1).get("access_agreement_accounts") mustBe null
      pvs(1).get("access_agreement_ids") mustBe null
    }
  }

}
