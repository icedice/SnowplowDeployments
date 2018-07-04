package dk.jp.snowplow_tsv_to_parquet.converters

import org.json4s.DefaultReaders._
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.Try

private case class Pageview(site: Option[String], contentId: Option[Int], sectionId: Option[Int], pageRestricted: Option[Boolean])
private case class User(anonId: Option[String], userId: Option[String], authorized: Option[Boolean])
private case class WebPage(id: Option[String])

object ContextExploder {
  def explodeContexts(in: Iterator[Array[Any]]): Iterator[Array[Any]] = {
    in.map(explodeContexts)
  }

  private def explodeContexts(event: Array[Any]): Array[Any] = {
    val ctxsField = event(52).asInstanceOf[String]
    val wrappers = Try((parse(ctxsField) \ "data").asInstanceOf[JArray].arr).getOrElse(List[JValue]())

    // Find the page_view, user and web_page contexts. There should only ever be one of each per page view event.
    val pv = findContext(wrappers, "iglu:dk.jyllands-posten/page_view/") map extractPageview getOrElse Pageview(None, None, None, None)
    val user = findContext(wrappers, "iglu:dk.jyllands-posten/user/") map extractUser getOrElse User(None, None, None)
    val webPage = findContext(wrappers, "iglu:com.snowplowanalytics.snowplow/web_page/") map extractWebPage getOrElse WebPage(None)

    val additionalFields: Seq[_ >: AnyRef] = Seq(
      user.anonId.orNull,
      user.userId.orNull,
      user.authorized.orNull,
      pv.site.orNull,
      pv.contentId.orNull,
      pv.sectionId.orNull,
      pv.pageRestricted.orNull,
      webPage.id.orNull
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
    val pageRestricted = (pvCtx \ "page_restricted").getAs[String].map(_ == "yes")
    Pageview(site, contentId, sectionId, pageRestricted)
  }

  private def extractUser(userCtx: JValue): User = {
    val anonId = (userCtx \ "anon_id").getAs[String]
    val userId = (userCtx \ "user_id").getAs[String]
    val authorized = (userCtx \ "user_authorized").getAs[String].map(_ == "yes")
    User(anonId, userId, authorized)
  }

  private def extractWebPage(userCtx: JValue): WebPage = WebPage((userCtx \ "id").getAs[String])
}
