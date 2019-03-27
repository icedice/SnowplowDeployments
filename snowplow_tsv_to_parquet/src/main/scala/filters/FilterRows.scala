package filters

import dk.jp.snowplow_tsv_to_parquet.util.Schemas

/***
  Object to filter rows for the events. Currently, rows with user agent containing "CookieInformationScanner" is removed.
  ***/

object FilterRows {

  def filterRows(in: Iterator[Array[Any]]): Iterator[Array[Any]] = {
    in.filterNot(shouldRemoveBasedOnUserAgent)
  }

  private def shouldRemoveBasedOnUserAgent(event: Array[Any]): Boolean = {
    val x: Any = Option(event(Schemas.inFieldNameToIdx("useragent")))
    x match {
      case Some(value: String) => value.contains("CookieInformationScanner")
      case _ => false
    }
  }
}