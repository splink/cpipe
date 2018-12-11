package example.processors

import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures}
import example.{Config, Output, Rps}
import play.api.libs.json.Json

import scala.concurrent.duration.Duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.control.NonFatal
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

class Exporter2 extends Processor {
  import example.JsonColumnParser._

  val rps = new Rps()

  override def process(session: Session, config: Config): Int = {
    val showProgress = config.flags.showProgress

    val meta = session.getCluster.getMetadata
    val keys = meta.getKeyspace(config.selection.keyspace)
      .getTable(config.selection.table).getPartitionKey.asScala.map(key => key.getName)

    val tokenId = s"token(${keys.mkString(",")})"

    val unwrappedRanges = meta.getTokenRanges.asScala.toList.flatMap { range =>
      range.unwrap().asScala.toList
    }.sorted.grouped(config.settings.threads).toList

    if (showProgress)
      Output(s"Got tokens, now execute ${unwrappedRanges.size} * ${config.settings.threads} queries.")

    def execute(groups: List[List[TokenRange]]): Future[Unit] = {
      groups match {
        case Nil =>
          Future.successful(())
        case head :: tail =>
          fetchNextGroup(head).map { _ =>
            execute(tail)
          }.recover {
            case NonFatal(e) =>
              Console.err.println(
                s"\nError during 'import': message: '${if(e != null) e.getMessage else ""}'")
              //TODO add counter to give up after a couple of retries
              execute(groups)
          }.flatten
      }
    }

    def fetchNextGroup(group: List[TokenRange]) = {
      Future.traverse(group) { range =>
        fetchRows(range).flatMap {
          case results if results.nonEmpty =>
            Future {
              Console.println(
                results.map(Json.prettyPrint).mkString("\n")
              )
            }
          case _ =>
            Future.successful(())
        }.recover {
          case NonFatal(e) =>
            Console.err.println(s"Ooops, could not fetch a row. message: ${if (e != null) e.getMessage else ""}")
            Future.successful(())
        }
      }
    }

    def fetchRows(range: TokenRange) = {
      val statement = new SimpleStatement(
        s"select * from ${config.selection.table} where $tokenId > ${range.getStart} and $tokenId <= ${range.getEnd};")

      session.executeAsync(statement).map { rs =>
        rs.iterator().asScala.map { row =>
          if (rs.getAvailableWithoutFetching < statement.getFetchSize / 2 && !rs.isFullyFetched) {
            if (showProgress) Output(s"Got ${rps.count} rows, off to get more...")
            rs.fetchMoreResults()
          }

          rps.compute()
          if (showProgress) Output(s"${rps.count} rows at $rps rows/sec.")

          row2Json(row)
        }
      }
    }

    Await.result(execute(unwrappedRanges), Inf)

    rps.count
  }

  implicit def asFuture(resultSet: ResultSetFuture): Future[ResultSet] = {
    val promise = Promise[ResultSet]
    Futures.addCallback[ResultSet](resultSet, new FutureCallback[ResultSet] {
      override def onSuccess(result: ResultSet): Unit = promise.success(result)
      override def onFailure(t: Throwable): Unit = promise.failure(t)
    })
    promise.future
  }
}
