package example

import com.datastax.driver.core._
import example.JsonColumnParser._
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.io.Source
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

object Extr {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    for {
      hosts <- conf.hosts.toOption
      keyspace <- conf.keyspace.toOption
      table <- conf.table.toOption
      filter <- conf.filter.toOption.map(_.mkString(" "))
      port <- conf.port.toOption
      username <- conf.username.toOption
      password <- conf.password.toOption
      progress <- conf.progress.toOption
      fetchSize <- conf.fetchSize.toOption
      useCompression <- conf.compression.toOption.map {
        case c if c == "ON" => true
        case _ => false
      }
      mode <- conf.mode.toOption
      consistencyLevel <- conf.consistencyLevel.toOption.map {
        case cl if cl == ConsistencyLevel.ANY.name() => ConsistencyLevel.ANY
        case cl if cl == ConsistencyLevel.ONE.name() => ConsistencyLevel.ONE
        case cl if cl == ConsistencyLevel.TWO.name() => ConsistencyLevel.TWO
        case cl if cl == ConsistencyLevel.THREE.name() => ConsistencyLevel.THREE
        case cl if cl == ConsistencyLevel.QUORUM.name() => ConsistencyLevel.QUORUM
        case cl if cl == ConsistencyLevel.ALL.name() => ConsistencyLevel.ALL
        case cl if cl == ConsistencyLevel.LOCAL_QUORUM.name() => ConsistencyLevel.LOCAL_QUORUM
        case cl if cl == ConsistencyLevel.EACH_QUORUM.name() => ConsistencyLevel.EACH_QUORUM
        case cl if cl == ConsistencyLevel.SERIAL.name() => ConsistencyLevel.SERIAL
        case cl if cl == ConsistencyLevel.LOCAL_SERIAL.name() => ConsistencyLevel.LOCAL_SERIAL
        case cl if cl == ConsistencyLevel.LOCAL_ONE.name() => ConsistencyLevel.LOCAL_ONE
      }
    } yield {
      if (progress) Output("Connecting to cassandra.")

      val session = Cassandra(hosts, keyspace, port, username, password, consistencyLevel, fetchSize, useCompression)
      session.execute(s"use $keyspace")

      if (progress) Output(s"Connected to cassandra '${session.getCluster.getClusterName}'")

      val start = System.currentTimeMillis()

      Try {
        mode match {
          case "import" =>
            importer(session, table, progress)
          case "export" =>
            exporter(session, table, filter, progress, consistencyLevel)
        }
      } match {
        case Success(_) =>
        case Failure(e) =>
          Console.err.println(s"\nError during '$mode': message: '${e.getMessage}' ${e.getStackTrace.mkString("\n")}")
          System.exit(1)
      }

      if (progress) Console.err.println(s"\nTook ${(System.currentTimeMillis() - start) / 1000}s")
    }

    System.exit(0)
  }

  var timestamp = System.currentTimeMillis()
  var rps = 0
  var lastRowstamp = 0
  var rowstamps = List.empty[Int]

  def calcRps(index: Int) = {
    val nextTimestamp = System.currentTimeMillis()
    if(nextTimestamp - timestamp > 1000) {
      val currentRps = index - lastRowstamp
      rowstamps = (currentRps :: rowstamps).slice(0, 20).sorted
      rps = if(rowstamps.size > 10) rowstamps(10) else rowstamps.last
      lastRowstamp = index
      timestamp = nextTimestamp
    }
  }

  def importer(session: Session, table: String, progress: Boolean) = {
    var index = 0
    val frame = new JsonFrame()
    Source.stdin.getLines().foreach { line =>
      frame.push(line.toCharArray).foreach { result =>
        string2JsObject(result).map { json =>
          index += 1
          if (progress) Output(s"$index rows at $rps rows/sec.")

          calcRps(index)

          session.execute(json2Query(json, table))
        }
      }
    }
  }

  def exporter(session: Session, table: String, filter: String, progress: Boolean, consistencyLevel: ConsistencyLevel) = {
    if (progress) Output("Execute query.")

    val statement = new SimpleStatement(s"select * from $table $filter;")

    val rs = session.execute(statement)
    rs.iterator().asScala.zipWithIndex.foreach { case (row, index) =>
      if (rs.getAvailableWithoutFetching < statement.getFetchSize / 2 && !rs.isFullyFetched) {
        if (progress) Output(s"Got $index rows, off to get more...")
        rs.fetchMoreResults()
      }
      if (progress) Output(s"$index rows at $rps rows/sec.")

      calcRps(index)

      val json = columnValues.andThen(columns2Json)(row)
      Console.println(Json.prettyPrint(json))
    }
  }
}
