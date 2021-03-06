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

  val inFieldNameToIdx: Map[String, Int] = SchemaHelper.inFields.map(_._2).zipWithIndex.toMap
}

private object SchemaHelper {
  val inFields: Seq[(FieldType, String)] = Seq(
    (StringFieldType(), "app_id"),
    (StringFieldType(), "platform"),
    (TimestampFieldType(), "etl_tstamp"),
    (TimestampFieldType(), "collector_tstamp"),
    // We can't trust the client's time stamps so if it is not a valid date (for example, we have seen events from year
    // 16517), set it to null.
    (TimestampFieldType(setNullIfNotParsable = true), "dvce_created_tstamp"),
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
    // As with dvce_created_tstamp.
    (TimestampFieldType(setNullIfNotParsable = true), "dvce_sent_tstamp"),
    (StringFieldType(), "refr_domain_userid"),
    // As with dvce_created_tstamp.
    (TimestampFieldType(setNullIfNotParsable = true), "refr_device_tstamp"),
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

  /**
    * Additional output columns extracted from the events' "context" JSON blob.
    */
  val additionalOutFields: Seq[(FieldType, String)] = Seq(
    (StringFieldType(), "anon_id"),
    (StringFieldType(), "user_id"),
    (BooleanFieldType(), "user_authenticated"),
    (BooleanFieldType(), "user_authorized"),
    (BooleanFieldType(), "grp_authenticated"),
    (BooleanFieldType(), "grp_authorized"),
    (StringFieldType(), "corp_id"),
    (StringFieldType(), "site"),
    (IntFieldType(), "content_id"),
    (IntFieldType(), "section_id"),
    (StringFieldType(), "section_name"),
    (StringFieldType(), "section_path_id"),
    (BooleanFieldType(), "page_restricted"),
    (StringFieldType(), "page_restricted_type"),
    (StringFieldType(), "web_page_id"),
    (StringArrayFieldType(), "access_agreement_accounts"),
    (StringArrayFieldType(), "access_agreement_ids")
  )

  /**
    * Construct an Avro schema from our internal [[FieldType]]s.
    */
  def buildSchema(name: String, fields: Seq[(FieldType, String)]): Schema = {
    val x = SchemaBuilder
      .record(name)
      .fields()

    // This is more-or-less equivalent to calling
    //   SchemaBuilder.record("rec").fields().optionalString("field1").optionalInt("field2").endRecord()
    // with a variable number of fields.
    fields.foldLeft(x) { case (acc, (t, fieldName)) =>
      t match {
        case StringFieldType() => acc.optionalString(fieldName)
        case IntFieldType() => acc.optionalInt(fieldName)
        case BooleanFieldType() => acc.optionalBoolean(fieldName)
        case LongFieldType() => acc.optionalLong(fieldName)
        case DoubleFieldType() => acc.optionalDouble(fieldName)
        case TimestampFieldType(allowNullIfNotParsable) =>
          if (allowNullIfNotParsable) {
            acc.optionalTsMillisSetNullIfNotParsable(fieldName)
          } else {
            acc.optionalTsMillis(fieldName)
          }
        case StringArrayFieldType() => acc.optionalStringArray(fieldName)
      }
    }

    x.endRecord()
  }

  private implicit def extendFieldAssemblerTsMillis(x: FieldAssembler[Schema]): FieldAssemblerTsMillis = new FieldAssemblerTsMillis(x)
  private implicit def extendFieldAssemblerStringArray(x: FieldAssembler[Schema]): FieldAssemblerStringArray = new FieldAssemblerStringArray(x)
}

/**
  * Extends FieldAssembler[R] with an optionalTsMillis() and optionalTsMillisSetNullIfNotParsable() methods.
  */
private class FieldAssemblerTsMillis(assembler: FieldAssembler[Schema]) {
  private val tsMillis = LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG))
  private val optionalTsMillisType = SchemaBuilder unionOf() nullType() and() `type` tsMillis endUnion()

  def optionalTsMillis(name: String): FieldAssembler[Schema] = assembler.name(name).`type`(optionalTsMillisType).withDefault(null)

  private val tsMillisSetNullIfNotParsable = LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG))
  // Add a flag to the schema's properties to let the parser know that the timestamp can be set to null if not valid.
  tsMillisSetNullIfNotParsable.addProp("setNullIfNotParsable".intern(), true)
  private val optionalTsMillisSetNullIfNotParsableType = SchemaBuilder unionOf() nullType() and() `type` tsMillisSetNullIfNotParsable endUnion()

  /**
    * Allow the timestamp to be unparsable and in that case, set it to null. Can be used for client information that we
    * can't fully control such as client timestamps which can be changed by the client and can't be trusted anyway.
    */
  def optionalTsMillisSetNullIfNotParsable(name: String): FieldAssembler[Schema] = assembler.name(name).`type`(optionalTsMillisSetNullIfNotParsableType).withDefault(null)
}

/**
  * Extends FieldAssembler[R] with an optionalStringArray() method.
  */
private class FieldAssemblerStringArray(assembler: FieldAssembler[Schema]) {
  /**
    * An optional array of non-nullable strings.
    */
  def optionalStringArray(name: String): FieldAssembler[Schema] = assembler.name(name).`type`().optional().array().items().nullable().stringType()
}

/**
  * The various field types supported by [[SchemaHelper.buildSchema]].
  */
private sealed trait FieldType
private case class StringFieldType() extends FieldType
private case class IntFieldType() extends FieldType
private case class LongFieldType() extends FieldType
private case class BooleanFieldType() extends FieldType
private case class DoubleFieldType() extends FieldType
/**
  * @param setNullIfNotParsable If the timestamp is not valid, set the field's value to null.
  */
private case class TimestampFieldType(setNullIfNotParsable: Boolean = false) extends FieldType

/**
  * Array of strings. If we need to support more types, consider adding a parameter of type FieldType, however, then we
  * need to handle the general case in [[SchemaHelper.buildSchema]].
  */
private case class StringArrayFieldType() extends FieldType
