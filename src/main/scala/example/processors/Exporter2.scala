package example.processors

import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures}
import example.{Config, Output, Rps}
import play.api.libs.json.Json

import scala.concurrent.duration.Duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.forkjoin.ForkJoinPool

class Exporter2 extends Processor {

  import example.JsonColumnParser._
  val printEc = ExecutionContext.fromExecutor(new ForkJoinPool(8))
  val rps = new Rps()

  override def process(session: Session, config: Config): Int = {
    val showProgress = config.flags.showProgress

    val meta = session.getCluster.getMetadata
    val keys = meta.getKeyspace(config.selection.keyspace)
      .getTable(config.selection.table).getPartitionKey.asScala.map(key => key.getName)

    val tokenId = s"token(${keys.mkString(",")})"

    Console.err.println(s"data is spread across ${meta.getAllHosts.size} hosts")

    val rangesByHost = meta.getTokenRanges.asScala.toList.map { range =>
      Set(meta.getReplicas(config.selection.keyspace, range).asScala.head) -> range
    }

    var compactedRanges = Compact(rangesByHost).foldLeft(List.empty[TokenRange]) { case (acc, (_, ranges)) =>
      ranges ::: acc
    }.sorted

    val have = compactedRanges.size
    val hostCount = meta.getAllHosts.size
    // if we have less ranges then hosts, split the ranges until each host can serve a range
    if(have < hostCount) {
      val isEven = (have % hostCount) - have
      val diff = (hostCount - have) + isEven

      (0 until diff).foreach { index =>
        val mod = index % compactedRanges.size
        compactedRanges = compactedRanges.patch(mod, compactedRanges(mod).splitEvenly(2).asScala, 1)
      }
    }

    Console.err.println(s"Got ${compactedRanges.size} compacted ranges")

    val groupedRanges = compactedRanges.grouped(config.settings.threads).toList


    //TODO count the ratio of queries with empty/nonempty results

    if (showProgress)
      Output(s"Query ${compactedRanges.size * config.settings.threads} ranges, ${config.settings.threads} in parallel.")

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
                s"\nError during 'import': message: '${if (e != null) e.getMessage else ""}'")
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
              results.grouped(10).foreach { group =>
                Console.println(
                  group.map(Json.prettyPrint).mkString("\n")
                )
              }
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

    Await.result(execute(groupedRanges), Inf)

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
