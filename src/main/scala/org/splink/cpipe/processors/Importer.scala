package org.splink.cpipe.processors

import com.datastax.driver.core.Session
import org.splink.cpipe.{JsonFrame, Output, Rps}
import org.splink.cpipe.config.Config

import scala.io.Source

class Importer extends Processor {

  import org.splink.cpipe.JsonColumnParser._

  val rps = new Rps()

  override def process(session: Session, config: Config): Int = {
    val frame = new JsonFrame()
    Source.stdin.getLines().foreach { line =>
      frame.push(line.toCharArray).foreach { result =>
        string2Json(result).map { json =>

          rps.compute()
          if (config.flags.showProgress) Output.update(s"${rps.count} rows at $rps rows/sec.")

          session.execute(json2Query(json, config.selection.table))
        }
      }
    }

    rps.count
  }

}
