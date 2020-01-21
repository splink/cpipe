package org.splink.cpipe.processors

import com.datastax.driver.core.{BatchStatement, PreparedStatement, Session}
import org.splink.cpipe.config.Config
import org.splink.cpipe.{JsonFrame, Output, Rps}

import scala.io.Source

class Importer2 extends Processor {

  import org.splink.cpipe.JsonColumnParser._

  val rps = new Rps()

  override def process(session: Session, config: Config): Int = {
    val frame = new JsonFrame()
    var statement: PreparedStatement = null
    val dataTypeMapping = getStringToObjectMappingForTable(session, config.selection.table)

    Source.stdin.getLines().flatMap { line =>
      frame.push(line.toCharArray)
    }.grouped(config.settings.batchSize).foreach { group =>
      val batch = new BatchStatement
      group.foreach { str =>
        string2Json(str).foreach { json =>
          if (statement == null) {
            statement = json2PreparedStatement(config.selection.table, json, session)
          }
          addJsonToBatch(json, statement, batch, dataTypeMapping)
          rps.compute()
        }
      }
      if (config.flags.showProgress) Output.update(s"${rps.count} rows at $rps rows/sec.")
      session.execute(batch)
    }
    rps.count
  }
}
