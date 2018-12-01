package example

import java.time.format.DateTimeFormatter

import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures}
import example.JsonColumnParser._
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future, Promise}
import scala.io.Source
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal
import scala.concurrent.duration.Duration._

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
          case "export2" =>
            Await.result(
              exporter2(session, conf.selection.keyspace, conf.selection.table, conf.selection.filter, conf.flags.showProgress),
              Inf)
        }
      } match {
        case Success(_) =>
        case Failure(e) =>
          Console.err.println(s"\nError during '${conf.mode}': message: '${e.getMessage}' ${e.getStackTrace.mkString("\n")}")
          System.exit(1)
      }

      if (conf.flags.showProgress) {
        val sec = (System.currentTimeMillis() - start) / 1000
        Console.err.println(s"\nProcessing ${rps.count} rows took ${ElapsedSecondFormat(sec)}s")
      }
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
    val frame = new JsonFrame()
    Source.stdin.getLines().foreach { line =>
      frame.push(line.toCharArray).foreach { result =>
        string2Json(result).map { json =>

          rps.compute()
          if (showProgress) Output(s"${rps.count} rows at $rps rows/sec.")

          session.execute(json2Query(json, table))
        }
      }
    }
    rps.count
  }

  def exporter(session: Session, table: String, filter: String, showProgress: Boolean) = {
    if (showProgress) Output("Execute query.")

    val statement = new SimpleStatement(s"select * from $table $filter;")

    val rs = session.execute(statement)
    rs.iterator().asScala.foreach { row =>
      if (rs.getAvailableWithoutFetching < statement.getFetchSize / 2 && !rs.isFullyFetched) {
        if (showProgress) Output(s"Got ${rps.count} rows, off to get more...")
        rs.fetchMoreResults()
      }

      rps.compute()
      if (showProgress) Output(s"${rps.count} rows at $rps rows/sec.")

      val json = row2Json(row)
      Console.println(Json.prettyPrint(json))
    }
    rps.count
  }

  def exporter2(session: Session, keyspace:String, table: String, filter: String, showProgress: Boolean) = {
    if (showProgress) Output("Execute query.")

    val meta = session.getCluster.getMetadata
    val keys = meta.getKeyspace(keyspace).getTable(table).getPartitionKey.asScala.map(key => key.getName)

    val tokenId = s"token(${keys.mkString(",")})"

    if (showProgress) Output("Got tokens.")

    val unwrappedRanges = meta.getTokenRanges.asScala.toList.flatMap { range =>
      range.unwrap().asScala.toList
    }.grouped(8).toList

    def execute(groups: List[List[TokenRange]]): Future[Unit] = {
      groups match {
        case Nil =>
          Future.successful(())
        case head :: tail =>
          Future.traverse(head) { range =>
            fetchRow(session, range, table, tokenId, filter, showProgress).map { itr =>
              itr.foreach { json =>
                Console.println(Json.prettyPrint(json))
              }
            }
          }.map { _ =>
              execute(tail)
          }.recover {
            case NonFatal(e) =>
              Console.err.println(s"An error occurred $e")
              execute(groups)
          }.flatten
      }

    }
    execute(unwrappedRanges)
  }

  def fetchRow(session: Session, range: TokenRange, table: String, tokenId: String, filter: String, showProgress: Boolean) = {
    val query = s"select * from $table where $tokenId > ${range.getStart} and $tokenId <= ${range.getEnd};"
    val statement = new SimpleStatement(query)
    session.executeAsync(statement).map { rs =>
      rs.iterator().asScala.map { row =>
        rps.compute()
        if (showProgress) Output(s"${rps.count} rows at $rps rows/sec.")
        row2Json(row)
      }
    }
  }

  implicit def asFuture(resultSet: ResultSetFuture): Future[ResultSet] = {
    val promise = Promise[ResultSet]
    Futures.addCallback[ResultSet](resultSet, new FutureCallback[ResultSet] {
      override def onSuccess(result: ResultSet): Unit = promise.success(result)
      override def onFailure(t: Throwable): Unit = promise.failure(t)
    })
    promise.future
  }

  object ElapsedSecondFormat {
    def zero(i: Long) = if (i < 10) s"0$i" else s"$i"

    def apply(s: Long) =
      s"""${zero(s / 3600)}:${zero((s % 3600) / 60)}:${zero(s % 60)}"""
  }

}
