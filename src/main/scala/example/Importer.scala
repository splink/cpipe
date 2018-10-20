package example

import play.api.libs.json.{JsObject, Json}

import scala.io.Source
import scala.util.{Failure, Success, Try}


object Importer {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    for {
      hosts <- conf.hosts.toOption
      keyspace <- conf.keyspace.toOption
      table <- conf.table.toOption
      port <- conf.port.toOption
      progress <- conf.progress.toOption
    } yield {
      if (progress) Output("Connecting to cassandra.")

      val (cluster, session) = Cassandra(hosts, keyspace, port)
      session.execute(s"use $keyspace")

      if (progress) Output(s"Connected to cassandra '${cluster.getClusterName}'")

      val start = System.currentTimeMillis()

      var index = 0
      val frame = new Frame()
      Source.stdin.getLines().foreach { line =>
        frame.push(line.toCharArray).foreach { result =>
          parse(result).map { json =>
            index += 1
            Output(s"$index rows.")
            session.execute(json2Columns(Json.parse(json).as[JsObject], table))
          }
        }
      }

      if (progress) Console.err.println(s"\nTook ${(System.currentTimeMillis() - start) / 1000}s")
      System.exit(0)
    }
  }

  val json2Columns = (json: JsObject, table: String) => {
    val fieldNames = json.fields.map(_._1).mkString(",")
    val fieldValues = json.fields.map(f => s"""'${Json.prettyPrint(f._2).replaceAllLiterally("'", "''")}'""").mkString(",")
    s"insert into $table ($fieldNames) values ($fieldValues);"
  }

  def parse(result: String) = {
    Try {
      Json.parse(result)
    } match {
      case Success(json) =>
        Some(Json.prettyPrint(json))
      case Failure(e) =>
        Console.err.println(s"Could not parse JSON: '$result' ${e.getMessage}")
        None
    }
  }

  class Frame {

    def push(a: Array[Char]): Option[String] = {
      a.foreach(balance)

      buffer = buffer ++ a

      if (isComplete) {
        val result = Some(buffer.mkString)
        reset
        result

      } else None
    }

    def balance(char: Char) = {
      if (char == Quote && !wasBackslash) {
        isQuoted = !isQuoted
      }

      if (!isQuoted) {

        if (char == OpenSquare && balanceCurly == 0) {
          balanceSquare = balanceSquare + 1
          isArray = true
        } else if (isArray) {
          if (char == OpenSquare) balanceSquare = balanceSquare + 1
          else if (char == CloseSquare) balanceSquare = balanceSquare - 1
        } else {
          if (char == OpenCurly) balanceCurly = balanceCurly + 1
          else if (char == CloseCurly) balanceCurly = balanceCurly - 1
        }

      }

      wasBackslash = char == Backslash
    }

    def isComplete = balanceCurly == 0 && !isArray || balanceSquare == 0 && isArray

    def reset = {
      buffer = Array.empty[Char]
      balanceCurly = 0
      balanceSquare = 0
      isArray = false
    }

    val OpenCurly = '{'
    val CloseCurly = '}'
    val OpenSquare = '['
    val CloseSquare = ']'
    val Quote = '"'
    val Backslash = '\\'
    var balanceCurly = 0
    var balanceSquare = 0
    var isQuoted = false
    var buffer = Array.empty[Char]
    var isArray = false
    var wasBackslash = false

  }


}
