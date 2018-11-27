package example

import com.datastax.driver.core._
import example.JsonColumnParser._
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.io.Source
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

object Extr {
  case class Connection(hosts: List[String], port: Int)
  case class Selection(keyspace: String, table: String, filter: String)
  case class Credentials(username: String, password: String)
  case class Flags(showProgress: Boolean, useCompression: Boolean)
  case class Settings(fetchSize: Int, consistencyLevel: ConsistencyLevel)
  case class Configuration(mode: String, connection: Connection, selection: Selection, credentials: Credentials, flags: Flags, settings: Settings)

  val rps = new Rps()

  def main(args: Array[String]): Unit = {
    parseArguments(new Conf(args)).foreach { conf =>
      if (conf.flags.showProgress) Output("Connecting to cassandra.")

      val session = createSessionFrom(conf)
      session.execute(s"use ${conf.selection.keyspace}")

      if (conf.flags.showProgress) Output(s"Connected to cassandra ${session.getCluster.getClusterName}")

      val start = System.currentTimeMillis()

      Try {
        conf.mode match {
          case "import" =>
            importer(session, conf.selection.table, conf.flags.showProgress)
          case "export" =>
            exporter(session, conf.selection.table, conf.selection.filter, conf.flags.showProgress)
        }
      } match {
        case Success(_) =>
        case Failure(e) =>
          Console.err.println(s"\nError during '${conf.mode}': message: '${e.getMessage}' ${e.getStackTrace.mkString("\n")}")
          System.exit(1)
      }

      if (conf.flags.showProgress) Console.err.println(s"\nTook ${(System.currentTimeMillis() - start) / 1000}s")
    }

    System.exit(0)
  }

  def parseArguments(conf: Conf) =
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
      Configuration(mode,
        Connection(hosts, port),
        Selection(keyspace, table, filter),
        Credentials(username, password),
        Flags(progress, useCompression),
        Settings(fetchSize, consistencyLevel))
    }

  def createSessionFrom(conf: Configuration) = Cassandra(
    conf.connection.hosts,
    conf.selection.keyspace,
    conf.connection.port,
    conf.credentials.username,
    conf.credentials.password,
    conf.settings.consistencyLevel,
    conf.settings.fetchSize,
    conf.flags.useCompression)


  def importer(session: Session, table: String, showProgress: Boolean) = {
    var index = 0
    val frame = new JsonFrame()
    Source.stdin.getLines().foreach { line =>
      frame.push(line.toCharArray).foreach { result =>
        string2Json(result).map { json =>
          index += 1
          if (showProgress) Output(s"$index rows at $rps rows/sec.")

          rps.compute(index)

          session.execute(json2Query(json, table))
        }
      }
    }
  }

  def exporter(session: Session, table: String, filter: String, showProgress: Boolean) = {
    if (showProgress) Output("Execute query.")

    val statement = new SimpleStatement(s"select * from $table $filter;")

    val rs = session.execute(statement)
    rs.iterator().asScala.zipWithIndex.foreach { case (row, index) =>
      if (rs.getAvailableWithoutFetching < statement.getFetchSize / 2 && !rs.isFullyFetched) {
        if (showProgress) Output(s"Got $index rows, off to get more...")
        rs.fetchMoreResults()
      }
      if (showProgress) Output(s"$index rows at $rps rows/sec.")

      rps.compute(index)

      val json = row2Json(row)
      Console.println(Json.prettyPrint(json))
    }
  }
}
