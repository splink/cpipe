package org.splink.cpipe.processors

import java.util.concurrent.TimeUnit

import com.datastax.driver.core.Session
import org.splink.cpipe.config.Config
import org.splink.cpipe.{JsonFrame, Output, Rps}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source


class Importer extends Processor {

  import org.splink.cpipe.JsonColumnParser._

  val rps = new Rps()

  override def process(session: Session, config: Config): Int = {
    val frame = new JsonFrame()
    Source.stdin.getLines().flatMap { line =>
      frame.push(line.toCharArray)
    }.grouped(500).foreach { group: Iterable[String] =>
      group.map { jsonStr =>
        Future {
          string2Json(jsonStr).map { json =>
            rps.compute()
            if (config.flags.showProgress) Output.update(s"${rps.count} rows at $rps rows/sec.")
            session.execute(json2Query(json, config.selection.table))
          }.get
        }
      }.foreach { future =>
        Await.ready(future, Duration(10, TimeUnit.SECONDS)).recover{case e: Exception =>println(e)}
      }
    }

    rps.count
  }

}
