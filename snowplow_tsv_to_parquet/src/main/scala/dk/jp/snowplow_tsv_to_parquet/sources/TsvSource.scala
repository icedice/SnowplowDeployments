package dk.jp.snowplow_tsv_to_parquet.sources

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.avro.Schema.Type
import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object TsvSource {
  private val logger = LoggerFactory.getLogger("TsvSource")

  def read(ls: Iterator[String], schema: Schema): Iterator[Array[Any]] = {
    val lsWithFields = ls
      .map(_.split("\t", -1))
      .map(_.zip(schema.getFields.asScala))

    lsWithFields
      .map(_.zipWithIndex.map {
        case ((rawValue, field), idx) => makeValue(rawValue, field.schema(), idx)
      })
  }

  private def makeValue(raw: String, schema: Schema, idx: Int): Any = {
    // All our input fields are unions because they are optional so we can safely call getTypes().
    val ts = schema.getTypes.asScala

    // For each possible type, attempt to parse the raw value as that type. Find the first that succeeds or throw an
    // error since that value does not conform to the schema.
    // The input schema only have a few primitive types, so we don't need to support e.g. Type.RECORD.
    // The iterator makes it lazy and find() stops after the first Success() if any so it should improve performance.
    ts.iterator.map { s =>
      parseValue(raw, s.getType, s.getLogicalType, s.getObjectProps.asScala.toMap)
    } find(_.isSuccess) match {
      case Some(Success(h)) => h
      case _ => throw new IllegalArgumentException(s"$raw does not match any of [${ts.mkString(",")}] in column ${idx + 1}")
    }
  }

  private def parseValue(raw: String, t: Type, logicalType: LogicalType, props: Map[String, AnyRef]): Try[Any] = t match {
    case Type.STRING => Success(raw)
    case Type.BOOLEAN => Try(parseBoolean(raw))
    case Type.INT => Try(raw.toInt)
    case Type.LONG if logicalType.isInstanceOf[LogicalTypes.TimestampMillis] =>
      Try(parseTimestamp(raw)) match {
        case Failure(_) if shouldSetNullIfNotParsable(props) =>
          logger.info(s"Encountered invalid timestamp $raw. Type allows us to set the value to null.")
          Success(null)
        case x => x
      }
    case Type.LONG => Try(raw.toLong)
    case Type.DOUBLE => Try(raw.toDouble)
    case Type.NULL => if (raw.isEmpty) Success(null) else Failure(new IllegalArgumentException)
    case x => throw new UnsupportedOperationException(s"unsupported schema type $x")
  }

  private def shouldSetNullIfNotParsable(props: Map[String, AnyRef]) = props.get("setNullIfNotParsable".intern()).exists(_.asInstanceOf[Boolean] == true)

  private def parseBoolean(s: String) = {
    if (s == "1") true
    else if (s == "0") false
    else s.toBoolean
  }

  private def parseTimestamp(s: String) = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    val parsedDate = LocalDateTime.parse(s, formatter)
    parsedDate.toInstant(ZoneOffset.UTC).toEpochMilli
  }
}
