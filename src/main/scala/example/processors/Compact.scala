package example.processors

import com.datastax.driver.core.{Host, TokenRange}

import scala.util.{Failure, Success, Try}

object Compact {

  implicit private class TokenRangeOps(range: TokenRange) {
    // requires the use of Murmur3Partitioner
    def contains(other: TokenRange) = {
      range.getStart.getValue.asInstanceOf[Long] <= other.getStart.getValue.asInstanceOf[Long] &&
        range.getEnd.getValue.asInstanceOf[Long] >= other.getEnd.getValue.asInstanceOf[Long]
    }
  }

  def apply(m: Map[Host, List[TokenRange]]) = {

    Console.err.println(s"${m.size} hosts.")
    m.foreach { case(host, ranges) =>
      Console.err.println(s"host $host has ${ranges.size} ranges.")
    }

    val merged = merge(m)
    Console.err.println(s"merged for ${merged.size} hosts.")
    merged.foreach { case(host, ranges) =>
      Console.err.println(s"host $host has ${ranges.size} ranges.")
    }

    val filtered = filter(merged)
    Console.err.println(s"filtered for ${filtered.size} hosts.")
    filtered.foreach { case(host, ranges) =>
      Console.err.println(s"host $host has ${ranges.size} ranges.")
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
