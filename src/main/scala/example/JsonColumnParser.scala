package example

import com.datastax.driver.core.Row
import com.fasterxml.jackson.core.io.JsonStringEncoder
import play.api.libs.json.{JsObject, JsString, JsValue, Json}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object JsonColumnParser {

  case class Column(name: String, value: String)

  def column2Json(column: Column) =
    Try(Json.parse(stripControlChars(column.value))) match {
      case Success(json) =>
        Some(JsObject(Map(column.name -> json)))
      case Failure(_) =>
        Some(Json.parse(s"""{"${column.name}": "${quoteAsString(column.value.toString)}"}"""))
    }

  def row2Json(row: Row) =
    row.getColumnDefinitions.iterator.asScala.flatMap { definition =>

      Try(row.getObject(definition.getName).toString) match {
        case Success(value) =>
          column2Json {
            Column(definition.getName, value)
          }
        case Failure(e) =>
          Console.err.println(s"Ooops, reading column '${definition.getName}' produced an error: ${if(e != null) e.getMessage else "null"}")
          None
      }

    }.foldLeft(Json.obj()) { (acc, next) =>
      acc.deepMerge(next.asInstanceOf[JsObject])
    }


  def string2Json(s: String) =
    Try {
      Json.parse(stripControlChars(s)).as[JsObject]
    } match {
      case Success(json) =>
        Some(json)
      case Failure(e) =>
        Console.err.println(s"Could not parse JSON: '$s' ${if (e != null) e.getMessage else "null"}")
        None
    }

  def json2Query(json: JsObject, table: String) = {
    def sanitize(s: String) =
      s.replaceAllLiterally("'", "''")

    def quoteJson(field: JsValue) =
      if (field.isInstanceOf[JsObject]) {
        JsString(sanitize(Json.stringify(field)))
      } else Json.parse(sanitize(Json.stringify(field)))

    val sanitizedJson = JsObject(json.fields.map { case (key, value) =>
      key -> quoteJson(value)
    })

    s"INSERT INTO $table JSON '${Json.stringify(sanitizedJson)}';"
  }


  def stripControlChars(s: String) =
    s.replaceAll("[\\u0000-\\u001f]", "")

  def quoteAsString(s: String) =
    JsonStringEncoder.getInstance.quoteAsString(s.toString).mkString

}
