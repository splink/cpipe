package org.splink.cpipe.processors

import com.datastax.driver.core.{Session, SimpleStatement}
import org.splink.cpipe.{Config, Output, Rps}
import play.api.libs.json.Json
import org.splink.cpipe.JsonColumnParser._

import scala.collection.JavaConverters._

class Exporter extends Processor {

  val rps = new Rps()

  override def process(session: Session, config: Config): Int = {
    val showProgress = config.flags.showProgress

    if (config.flags.showProgress) Output.update("Execute query.")

    val statement = new SimpleStatement(s"select * from ${config.selection.table} ${config.selection.filter};")

    val rs = session.execute(statement)
    rs.iterator().asScala.foreach { row =>
      if (rs.getAvailableWithoutFetching < statement.getFetchSize / 2 && !rs.isFullyFetched) {
        if (showProgress) Output.update(s"Got ${rps.count} rows, off to get more...")
        rs.fetchMoreResults()
      }

      rps.compute()
      if (showProgress) Output.update(s"${rps.count} rows at $rps rows/sec.")

      val json = row2Json(row)
      Console.println(Json.prettyPrint(json))
    }
    rps.count
  }

}
