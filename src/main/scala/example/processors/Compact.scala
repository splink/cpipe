package example.processors

import com.datastax.driver.core.{Host, TokenRange}

import scala.util.{Failure, Success, Try}

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

    if(verbose) {
      Console.err.println(s"compacted ${filtered.size} hosts.")
      regrouped.zip(filtered).foreach { case ((host1, ranges1), (host2, ranges2)) =>
        assert(host1 == host2)
        Console.err.println(s"Host $host1 reduced ${ranges1.size} to ${ranges2.size} ranges.")
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
