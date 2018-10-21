package example

import com.datastax.driver.core.Row
import play.api.libs.json.{JsObject, Json}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object JsonColumnParser {

  case class Column(name: String, value: String)

  val escapedString2Json = (s: String) => {
    Try {
      val unescaped = StringContext.processEscapes(s)
      Json.parse(unescaped.substring(1, unescaped.length - 1))
    }.toOption.getOrElse {
      Json.parse(s)
    }
  }

  val columns2Json = (columns: Iterator[Column]) => {
    columns.flatMap { case Column(name, value) =>
      Try(Json.parse(value)) match {
        case Success(json) =>

          val map = json.as[JsObject].fields.map { case (fieldName, fieldValue) =>
            fieldName -> escapedString2Json(fieldValue.toString)
          }

          Some(JsObject(map))
        case Failure(e) =>
          Console.err.println(s"Could not convert column '$name' to json ${e.getMessage}")
          None
      }
    }
  }

  val columnValues = (row: Row) => {
    row.getColumnDefinitions.iterator.asScala.map { definition =>
      Column(definition.getName, row.getString(definition.getName))
    }
  }

  val string2JsObject = (s: String) => {
    Try {
      Json.parse(s).as[JsObject]
    } match {
      case Success(json) =>
        Some(json)
      case Failure(e) =>
        Console.err.println(s"Could not parse JSON: '$s' ${e.getMessage}")
        None
    }
  }

  val json2Query = (json: JsObject, table: String) => {
    val fieldNames = json.fields.map(_._1).mkString(",")
    val fieldValues = json.fields.map(f => s"""'${Json.prettyPrint(f._2).replaceAllLiterally("'", "''")}'""").mkString(",")
    s"INSERT INTO $table ($fieldNames) VALUES ($fieldValues);"
  }
}
