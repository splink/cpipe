package example

import java.util.UUID

import com.datastax.driver.core.Row
import play.api.libs.json.{JsObject, JsString, JsValue, Json}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object JsonColumnParser {

  case class Column(name: String, value: String)

  def stripControlChars(s: String) =
    s.replaceAll("[\\u0000-\\u001f]", "")

  val escapedString2Json = (s: String) => {
    Try {
      val unescaped = StringContext.processEscapes(s)
      Json.parse(unescaped.substring(1, unescaped.length - 1)).as[JsObject]
    }.toOption.getOrElse {
      Json.parse(s)
    }
  }

  val columns2Json = (columns: Iterator[Column]) => {
    columns.flatMap { case Column(name, value) =>
      Try(Json.parse(stripControlChars(value))) match {
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
      Json.parse(stripControlChars(s)).as[JsObject]
    } match {
      case Success(json) =>
        Some(json)
      case Failure(e) =>
        Console.err.println(s"Could not parse JSON: '$s' ${e.getMessage}")
        None
    }
  }

  val json2Query = (json: JsObject, table: String) => {
    def sanitize(s: String) =
      s.replaceAllLiterally("'", "''")

    def quoteJson(field: JsValue) =
      if(field.isInstanceOf[JsObject]) {
        JsString(sanitize(Json.stringify(field)))
      } else field

    val sanitizedJson = JsObject(json.fields.map { case (key, value) =>
        key -> quoteJson(value)
    })

    s"INSERT INTO $table JSON '${Json.stringify(sanitizedJson)}';"
  }
}
