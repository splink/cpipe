package example.processors

import com.datastax.driver.core.Session
import example.{Config, JsonFrame, Output, Rps}

import scala.io.Source

class Importer extends Processor {

  import example.JsonColumnParser._

  val rps = new Rps()

  override def process(session: Session, config: Config): Int = {
    val frame = new JsonFrame()
    Source.stdin.getLines().foreach { line =>
      frame.push(line.toCharArray).foreach { result =>
        string2Json(result).map { json =>

          rps.compute()
          if (config.flags.showProgress) Output(s"${rps.count} rows at $rps rows/sec.")

          session.execute(json2Query(json, config.selection.table))
        }
      }
    }

    rps.count
  }

}
