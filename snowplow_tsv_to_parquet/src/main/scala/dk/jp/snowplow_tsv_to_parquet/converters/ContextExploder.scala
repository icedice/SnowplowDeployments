package dk.jp.snowplow_tsv_to_parquet.converters

import dk.jp.snowplow_tsv_to_parquet.util.Schemas
import org.json4s.DefaultReaders._
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.Try

private case class Pageview(site: Option[String], contentId: Option[Int], sectionId: Option[Int], sectionName: Option[String], sectionPathId: Option[String], pageRestricted: Option[Boolean], pageRestrictedType: Option[String])
private case class NativeAppScreenview(site: Option[String], contentId: Option[Int], sectionId: Option[Int], pageRestricted: Option[Boolean], pageRestrictedType: Option[String])
private case class Group(authenticated: Option[Boolean], authorized: Option[Boolean], corpId: Option[String])
private case class User(anonId: Option[String], userId: Option[String], authenticated: Option[Boolean], authorized: Option[Boolean], group: Group)
private case class AccessAgreements(accounts: Seq[String], ids: Seq[String])
private case class WebPage(id: Option[String])

object ContextExploder {
  def explodeContexts(in: Iterator[Array[Any]]): Iterator[Array[Any]] = {
    in.map(explodeContexts)
  }

  private def explodeContexts(event: Array[Any]): Array[Any] = {
    val ctxsField = event(Schemas.inFieldNameToIdx("contexts")).asInstanceOf[String]
    val wrappers = Try((parse(ctxsField) \ "data").asInstanceOf[JArray].arr).getOrElse(List[JValue]())

    // Find the page_view/native_app_screen_view, user and web_page contexts. There should only ever be one of each per page view event.
    val pv = findContext(wrappers, "iglu:dk.jyllands-posten/page_view/") map extractPageview getOrElse Pageview(None, None, None, None, None, None, None)
    val nasv = findContext(wrappers, "iglu:dk.jyllands-posten/native_app_screen_view/") map extractNativeAppScreenView getOrElse NativeAppScreenview(None, None, None, None, None)

    val user = findContext(wrappers, "iglu:dk.jyllands-posten/user/") map extractUser getOrElse User(None, None, None, None, Group(None, None, None))
    // If the AA context is there, we know that it contains a list of (account, id) pairs so there's no reason to use
    // getOrElse() here but rather let accessAgreements be an Option.
    val accessAgreements = findContext(wrappers, "iglu:dk.jyllands-posten/access_agreements/") map extractAccessAgreements
    val webPage = findContext(wrappers, "iglu:com.snowplowanalytics.snowplow/web_page/") map extractWebPage getOrElse WebPage(None)

    // The order here must match the order in SchemaHelper.additionalOutFields.
    val additionalFields: Seq[_ >: AnyRef] = Seq(
      user.anonId.orNull,
      user.userId.orNull,
      user.authenticated.orNull,
      user.authorized.orNull,
      user.group.authenticated.orNull,
      user.group.authorized.orNull,
      user.group.corpId.orNull,

      // Use page view first, if that has empty options, use native app screen view to populate the additional fields.
      pv.site.orElse(nasv.site).orNull,
      pv.contentId.orElse(nasv.contentId).orNull,
      pv.sectionId.orElse(nasv.sectionId).orNull,
      pv.sectionName.orNull,
      pv.sectionPathId.orNull,
      pv.pageRestricted.orElse(nasv.pageRestricted).orNull,
      pv.pageRestrictedType.orElse(nasv.pageRestrictedType).orNull,

      webPage.id.orNull,

      accessAgreements.map(_.accounts.toArray).orNull,
      accessAgreements.map(_.ids.toArray).orNull
    )

    event ++ additionalFields
  }

  private def findContext(wrappers: Seq[JValue], prefix: String): Option[JValue] = {
    val ctxWrapper = wrappers.find { contextWrapper =>
      (contextWrapper \ "schema").asInstanceOf[JString].s.startsWith(prefix)
    }

    ctxWrapper.map(_ \ "data")
  }

  private def extractPageview(pvCtx: JValue): Pageview = {
    val site = (pvCtx \ "site").getAs[String]
    val contentId = (pvCtx \ "content_id").getAs[Int]
    val sectionId = (pvCtx \ "section_id").getAs[Int]
    val sectionName = (pvCtx \ "section_name").getAs[String]
    // For legacy reasons we output the section path id as a "_" separated list of ids. For example, [1, 2, 3] becomes
    // "1_2_3".
    val sectionPathId = (pvCtx \ "section_path_id").getAs[Seq[Int]].map(_.mkString("_"))
    val pageRestricted = (pvCtx \ "page_restricted").getAs[String].map(_ == "yes")
    val pageRestrictedType = (pvCtx \ "page_restricted_type").getAs[String]
    Pageview(site, contentId, sectionId, sectionName, sectionPathId, pageRestricted, pageRestrictedType)
  }

  private def extractNativeAppScreenView(nasvCtx: JValue): NativeAppScreenview = {
    val site = (nasvCtx \ "site").getAs[String]
    val contentId = (nasvCtx \ "content_id").getAs[Int]
    val sectionId = (nasvCtx \ "section_id").getAs[Int]
    val pageRestricted = (nasvCtx \ "page_restricted").getAs[String].map(_ == "yes")
    val pageRestrictedType = (nasvCtx \ "page_restricted_type").getAs[String]
    NativeAppScreenview(site, contentId, sectionId, pageRestricted, pageRestrictedType)
  }

  private def extractUser(userCtx: JValue): User = {
    val anonId = (userCtx \ "anon_id").getAs[String]
    val userId = (userCtx \ "user_id").getAs[String].filter(_ != "anon")
    val userAuthenticated = (userCtx \ "user_authenticated").getAs[String].map(_ == "yes")
    val userAuthorized = (userCtx \ "user_authorized").getAs[String].map(_ == "yes")

    val grpAuthenticated = (userCtx \ "grp_authenticated").getAs[String].map(_ == "yes")
    val grpAuthorized = (userCtx \ "grp_authorized").getAs[String].map(_ == "yes")
    val corpId = (userCtx \ "corp_id").getAs[String].filter(_ != "NOTSET")
    val group = Group(grpAuthenticated, grpAuthorized, corpId)

    User(anonId, userId, userAuthenticated, userAuthorized, group)
  }

  private def extractAccessAgreements(aaCtx: JValue): AccessAgreements = {
    val aas = (aaCtx \ "access_agreements").asInstanceOf[JArray]
    val accountAndIdPairs = aas.arr.map { aa =>
      val account = (aa \ "account_number").as[String]
      val id = (aa \ "id").as[String]
      (account, id)
    }
    val (accounts, ids) = accountAndIdPairs.unzip

    AccessAgreements(accounts, ids)
  }

  private def extractWebPage(userCtx: JValue): WebPage = WebPage((userCtx \ "id").getAs[String])
}
