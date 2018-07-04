package dk.jp.snowplow_tsv_to_parquet.util

import org.apache.avro.SchemaBuilder.FieldAssembler
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import scala.language.implicitConversions

object Schemas {
  /**
    * Most fields are shared between the input and output schema, however, you cannot copy or reuse an Avro schema. As
    * an alternative, we declare a helper method to define a schema from a list of field types and names.
    */
  val in: Schema = SchemaHelper.buildSchema("Event", SchemaHelper.inFields)
  val out: Schema = SchemaHelper.buildSchema("Event", SchemaHelper.inFields ++ SchemaHelper.additionalOutFields)
}

private object SchemaHelper {
  val inFields: Seq[(FieldType, String)] = Seq(
    (StringFieldType(), "app_id"),
    (StringFieldType(), "platform"),
    (TimestampFieldType(), "etl_tstamp"),
    (TimestampFieldType(), "collector_tstamp"),
    (TimestampFieldType(), "dvce_created_tstamp"),
    // This is actually called "event", however, we use it as a partition column and to avoid a "duplicate whatever"
    // warning, we simply rename the version of the column that is stored in the file. If using e.g. Spark to save the
    // data it will automatically remove the partition column from the file, however, when using parquet-mr like we do,
    // there is no easy way to remove the column from the output while keeping it in the schema.
    (StringFieldType(), "event_type"),
    (StringFieldType(), "event_id"),
    (StringFieldType(), "txn_id"),
    (StringFieldType(), "name_tracker"),
    (StringFieldType(), "v_tracker"),
    (StringFieldType(), "v_collector"),
    (StringFieldType(), "v_etl"),
    (StringFieldType(), "snowplow_user_id"),
    (StringFieldType(), "user_ipaddress"),
    (LongFieldType(), "user_fingerprint"),
    (StringFieldType(), "domain_userid"),
    (IntFieldType(), "domain_sessionidx"),
    (StringFieldType(), "network_userid"),
    (StringFieldType(), "geo_country"),
    (StringFieldType(), "geo_region"),
    (StringFieldType(), "geo_city"),
    (StringFieldType(), "geo_zipcode"),
    (DoubleFieldType(), "geo_latitude"),
    (DoubleFieldType(), "geo_longitude"),
    (StringFieldType(), "geo_region_name"),
    (StringFieldType(), "ip_isp"),
    (StringFieldType(), "ip_organization"),
    (StringFieldType(), "ip_domain"),
    (StringFieldType(), "ip_netspeed"),
    (StringFieldType(), "page_url"),
    (StringFieldType(), "page_title"),
    (StringFieldType(), "page_referrer"),
    (StringFieldType(), "page_urlscheme"),
    (StringFieldType(), "page_urlhost"),
    (IntFieldType(), "page_urlport"),
    (StringFieldType(), "page_urlpath"),
    (StringFieldType(), "page_urlquery"),
    (StringFieldType(), "page_urlfragment"),
    (StringFieldType(), "refr_urlscheme"),
    (StringFieldType(), "refr_urlhost"),
    (IntFieldType(), "refr_urlport"),
    (StringFieldType(), "refr_urlpath"),
    (StringFieldType(), "refr_urlquery"),
    (StringFieldType(), "refr_urlfragment"),
    (StringFieldType(), "refr_medium"),
    (StringFieldType(), "refr_source"),
    (StringFieldType(), "refr_term"),
    (StringFieldType(), "mkt_medium"),
    (StringFieldType(), "mkt_source"),
    (StringFieldType(), "mkt_term"),
    (StringFieldType(), "mkt_content"),
    (StringFieldType(), "mkt_campaign"),
    (StringFieldType(), "contexts"),
    (StringFieldType(), "se_category"),
    (StringFieldType(), "se_action"),
    (StringFieldType(), "se_label"),
    (StringFieldType(), "se_property"),
    (StringFieldType(), "se_value"),
    (StringFieldType(), "unstruct_event"),
    (StringFieldType(), "tr_orderid"),
    (StringFieldType(), "tr_affiliation"),
    (StringFieldType(), "tr_total"),
    (StringFieldType(), "tr_tax"),
    (StringFieldType(), "tr_shipping"),
    (StringFieldType(), "tr_city"),
    (StringFieldType(), "tr_state"),
    (StringFieldType(), "tr_country"),
    (StringFieldType(), "ti_orderid"),
    (StringFieldType(), "ti_sku"),
    (StringFieldType(), "ti_name"),
    (StringFieldType(), "ti_category"),
    (StringFieldType(), "ti_price"),
    (StringFieldType(), "ti_quantity"),
    (IntFieldType(), "pp_xoffset_min"),
    (IntFieldType(), "pp_xoffset_max"),
    (IntFieldType(), "pp_yoffset_min"),
    (IntFieldType(), "pp_yoffset_max"),
    (StringFieldType(), "useragent"),
    (StringFieldType(), "br_name"),
    (StringFieldType(), "br_family"),
    (StringFieldType(), "br_version"),
    (StringFieldType(), "br_type"),
    (StringFieldType(), "br_renderengine"),
    (StringFieldType(), "br_lang"),
    (BooleanFieldType(), "br_features_pdf"),
    (BooleanFieldType(), "br_features_flash"),
    (BooleanFieldType(), "br_features_java"),
    (BooleanFieldType(), "br_features_director"),
    (BooleanFieldType(), "br_features_quicktime"),
    (BooleanFieldType(), "br_features_realplayer"),
    (BooleanFieldType(), "br_features_windowsmedia"),
    (BooleanFieldType(), "br_features_gears"),
    (BooleanFieldType(), "br_features_silverlight"),
    (BooleanFieldType(), "br_cookies"),
    (IntFieldType(), "br_colordepth"),
    (IntFieldType(), "br_viewwidth"),
    (IntFieldType(), "br_viewheight"),
    (StringFieldType(), "os_name"),
    (StringFieldType(), "os_family"),
    (StringFieldType(), "os_manufacturer"),
    (StringFieldType(), "os_timezone"),
    (StringFieldType(), "dvce_type"),
    (BooleanFieldType(), "dvce_ismobile"),
    (IntFieldType(), "dvce_screenwidth"),
    (IntFieldType(), "dvce_screenheight"),
    (StringFieldType(), "doc_charset"),
    (IntFieldType(), "doc_width"),
    (IntFieldType(), "doc_height"),
    (StringFieldType(), "tr_currency"),
    (StringFieldType(), "tr_total_base"),
    (StringFieldType(), "tr_tax_base"),
    (StringFieldType(), "tr_shipping_base"),
    (StringFieldType(), "ti_currency"),
    (StringFieldType(), "ti_price_base"),
    (StringFieldType(), "base_currency"),
    (StringFieldType(), "geo_timezone"),
    (StringFieldType(), "mkt_clickid"),
    (StringFieldType(), "mkt_network"),
    (StringFieldType(), "etl_tags"),
    (TimestampFieldType(), "dvce_sent_tstamp"),
    (StringFieldType(), "refr_domain_userid"),
    (TimestampFieldType(), "refr_device_tstamp"),
    (StringFieldType(), "derived_contexts"),
    (StringFieldType(), "domain_sessionid"),
    (TimestampFieldType(), "derived_tstamp"),
    (StringFieldType(), "event_vendor"),
    (StringFieldType(), "event_name"),
    (StringFieldType(), "event_format"),
    (StringFieldType(), "event_version"),
    (StringFieldType(), "event_fingerprint"),
    (TimestampFieldType(), "true_tstamp")
  )

  val additionalOutFields: Seq[(FieldType, String)] = Seq(
    (StringFieldType(), "anon_id"),
    (StringFieldType(), "user_id"),
    (BooleanFieldType(), "user_authorized"),
    (StringFieldType(), "site"),
    (IntFieldType(), "content_id"),
    (IntFieldType(), "section_id"),
    (BooleanFieldType(), "page_restricted"),
    (StringFieldType(), "web_page_id")
  )

  /**
    * This is more-or-less equivalent to calling SchemaBuilder.record("rec").fields().optionalString("field1").optionalInt("field2").endRecord()
    * But with a variable number of fields.
    */
  def buildSchema(name: String, fields: Seq[(FieldType, String)]): Schema = {
    val x = SchemaBuilder
      .record(name)
      .fields()

    fields.foldLeft(x) { case (acc, (t, fieldName)) =>
      t match {
        case StringFieldType() => acc.optionalString(fieldName)
        case IntFieldType() => acc.optionalInt(fieldName)
        case BooleanFieldType() => acc.optionalBoolean(fieldName)
        case LongFieldType() => acc.optionalLong(fieldName)
        case DoubleFieldType() => acc.optionalDouble(fieldName)
        case TimestampFieldType() => acc.optionalTsMillis(fieldName)
      }
    }

    x.endRecord()
  }

  private implicit def extendFieldAssembler(x: FieldAssembler[Schema]): FieldAssemblerTsMillis = new FieldAssemblerTsMillis(x)
}

/**
  * Extends FieldAssembler[R] with an optionalTsMillis() method.
  */
private class FieldAssemblerTsMillis(assembler: FieldAssembler[Schema]) {
  private val tsMillis = LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG))
  private val optionalTsMillisType = SchemaBuilder unionOf() nullType() and() `type` tsMillis endUnion()

  def optionalTsMillis(name: String): FieldAssembler[Schema] = assembler.name(name).`type`(optionalTsMillisType).withDefault(null)
}

/**
  * The various field types supported by SchemaHelper.buildSchema().
  */
private sealed trait FieldType
private case class StringFieldType() extends FieldType
private case class IntFieldType() extends FieldType
private case class LongFieldType() extends FieldType
private case class BooleanFieldType() extends FieldType
private case class DoubleFieldType() extends FieldType
private case class TimestampFieldType() extends FieldType
