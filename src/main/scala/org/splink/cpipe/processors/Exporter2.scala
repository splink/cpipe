package org.splink.cpipe.processors

import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures}
import org.splink.cpipe.{Output, Rps}
import org.splink.cpipe.config.Config
import play.api.libs.json.{JsObject, Json}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration._
import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

class Exporter2 extends Processor {

  import org.splink.cpipe.JsonColumnParser._

  val cores = Runtime.getRuntime.availableProcessors()
  val printEc = ExecutionContext.fromExecutor(new ForkJoinPool(cores))
  val rps = new Rps()

  class Stats {
    var misses = 0
    var hits = 0

    def miss = synchronized(misses = misses + 1)
    def hit = synchronized(hits = hits + 1)
    def hitPercentage(total: Int) =
      Math.round(misses / total.toDouble * 10000) / 100d
  }

  val stats = new Stats()


  override def process(session: Session, config: Config): Int = {
    val showProgress = config.flags.showProgress

    val meta = session.getCluster.getMetadata
    val keys = meta.getKeyspace(config.selection.keyspace)
      .getTable(config.selection.table).getPartitionKey.asScala.map(key => key.getName)

    val tokenId = s"token(${keys.mkString(",")})"

    if (config.flags.verbose) {
      Output.log(s"data is spread across ${meta.getAllHosts.size} hosts.")
      Output.log(s"Partitioner is '${meta.getPartitioner}'")
    }

    val rangesByHost = meta.getTokenRanges.asScala.toList.map { range =>
      Set(meta.getReplicas(config.selection.keyspace, range).asScala.head) -> range
    }

    val compactedRanges = Compact(rangesByHost, config.flags.verbose)
      .foldLeft(List.empty[TokenRange]) { case (acc, (_, ranges)) =>
        ranges ::: acc
      }.sorted

    if (config.flags.verbose) Output.log(s"Got ${compactedRanges.size} compacted ranges")

    val groupedRanges = compactedRanges.grouped(config.settings.threads).toList

    if (showProgress && config.flags.verbose)
      Output.update(s"Query ${compactedRanges.size} ranges, ${config.settings.threads} in parallel.")

    def fetchGroups(groups: List[List[TokenRange]]): Future[Unit] =
      groups match {
        case Nil =>
          Future.successful(())
        case head :: tail =>
          fetchNextGroup(head).map { _ =>
            fetchGroups(tail)
          }.flatten
      }

    def fetchNextGroup(group: List[TokenRange]) =
      Future.traverse(group) { range =>
        fetchRows(range).flatMap {
          case results if results.nonEmpty =>
            stats.hit
            outputProgress()
            output(results)
          case _ =>
            stats.miss
            outputProgress()
            Future.successful(())
        }.recover {
          case NonFatal(e) =>
            Output.log(s"Ooops, could not fetch a row. message: ${if (e != null) e.getMessage else ""}")
            Future.successful(())
        }
      }

    def fetchRows(range: TokenRange) = {
      val statement = new SimpleStatement(
        s"select * from ${config.selection.table} " +
          s"where $tokenId > ${range.getStart} and $tokenId <= ${range.getEnd};")

      session.executeAsync(statement).map { rs =>
        rs.iterator().asScala.map { row =>
          if (rs.getAvailableWithoutFetching < statement.getFetchSize / 2 && !rs.isFullyFetched) {
            if (showProgress) Output.update(s"Got ${rps.count} rows, off to get more...")
            rs.fetchMoreResults()
          }

          row2Json(row)
        }
      }
    }

    def outputProgress() =
      if (showProgress) {
        Output.update(s"${rps.count} rows at $rps rows/sec. " +
          (if (config.flags.verbose)
            s"${stats.hitPercentage(compactedRanges.size)}% misses " +
              s"(missed ${stats.misses} of ${compactedRanges.size} ranges. ${stats.hits} hits)" else "")
        )
      }


    def output(results: Iterator[JsObject]) = Future {
      results.foreach { result =>
        rps.compute()
        Output.render(Json.prettyPrint(result))
      }
    }(printEc)

    Await.result(fetchGroups(groupedRanges), Inf)
    outputProgress()
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

  object Compact {

    implicit private class TokenRangeOps(range: TokenRange) {
      def contains(other: TokenRange) = {
        range.getStart.getValue.asInstanceOf[Long] <= other.getStart.getValue.asInstanceOf[Long] &&
          range.getEnd.getValue.asInstanceOf[Long] >= other.getEnd.getValue.asInstanceOf[Long]
      }
    }

    /**
      * requires the use of Murmur3Partitioner
      */
    def apply(xs: List[(Set[Host], TokenRange)], verbose: Boolean) = {
      val regrouped = regroup(xs)
      val merged = merge(regrouped)
      val filtered = filter(merged)

      if (verbose) {
        Output.log(s"compacted ${filtered.size} hosts.")
        regrouped.zip(filtered).foreach { case ((host1, ranges1), (host2, ranges2)) =>
          assert(host1 == host2)
          Output.log(s"Host $host1 reduced ${ranges1.size} to ${ranges2.size} ranges.")
        }
      }

      filtered
    }

    def regroup(xs: List[(Set[Host], TokenRange)]) =
      xs.foldLeft(Map.empty[Host, List[TokenRange]]) { case (acc, (hosts, range)) =>
        val setOfMaps = hosts.map { host =>
          val item = acc.get(host) match {
            case Some(ranges) =>
              host -> (range :: ranges)
            case None =>
              host -> (range :: Nil)
          }

          acc + item
        }

        val merged = setOfMaps.foldLeft(Seq.empty[(Host, List[TokenRange])]) { case (acc1, next) =>
          acc1 ++ next.toSeq
        }

        merged.groupBy(_._1).map { case (key, xss) =>
          key -> xss.flatMap(_._2).toList.distinct.sortBy(_.getStart.getValue.asInstanceOf[Long])
        }
      }


    def merge(grouped: Map[Host, List[TokenRange]]) =
      grouped.map { case (host, ranges) =>
        host -> ranges.foldLeft(List.empty[TokenRange]) { (acc, next) =>
          acc match {
            case Nil =>
              next :: Nil
            case head :: tail =>
              Try(head.mergeWith(next)) match {
                case Success(value) => value :: tail
                case Failure(_) => next :: acc
              }
          }
        }
      }

    def filter(grouped: Map[Host, List[TokenRange]]) =
      grouped.foldLeft(Map.empty[Host, List[TokenRange]]) { case (acc, (host, ranges)) =>
        val filteredRanges = host -> ranges.filterNot { range =>
          (grouped ++ acc).filterNot(_._1 == host).map { case (_, otherRanges) =>
            otherRanges.exists { otherRange =>
              otherRange.contains(range)
            }
          }.exists(_ == true)
        }

        acc + filteredRanges
      }
  }

}
