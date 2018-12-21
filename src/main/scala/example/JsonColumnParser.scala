package example

import com.datastax.driver.core.{DataType, Row}
import play.api.libs.json._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object JsonColumnParser {

  case class Column(name: String, value: String, typ: DataType)

  def column2Json(column: Column) = {
    val sanitized = stripControlChars(column.value)
    Try(Json.parse(sanitized)) match {
      case Success(json) =>
        val r = json match {
          case o: JsObject => o
          case _ => parseCassandraDataType(sanitized, column.typ)
        }

        Some(JsObject(Map(column.name -> r)))

      case Failure(_) =>
        Some(JsObject(Map(column.name -> parseCassandraDataType(sanitized, column.typ))))
    }
  }

  def row2Json(row: Row) =
    row.getColumnDefinitions.iterator.asScala.flatMap { definition =>
      Try(row.getObject(definition.getName).toString) match {
        case Success(value) =>
          column2Json {
            Column(definition.getName, value, definition.getType)
          }
        case Failure(e) =>
          Output.log(s"Ooops, reading column '${definition.getName}' produced an error: ${if (e != null) e.getMessage else "null"}")
          None
      }

    }.foldLeft(Json.obj()) { (acc, next) =>
      acc.deepMerge(next)
    }


  def string2Json(s: String) =
    Try {
      Json.parse(stripControlChars(s)).as[JsObject]
    } match {
      case Success(json) =>
        Some(json)
      case Failure(e) =>
        Output.log(s"Could not parse JSON: '$s' ${if (e != null) e.getMessage else "null"}")
        None
    }

  def json2Query(json: JsObject, table: String) = {
    def sanitize(s: String) =
      s.replaceAllLiterally("'", "''")

    def quoteJson(field: JsValue) =
      if (field.isInstanceOf[JsObject]) {
        JsString(sanitize(Json.stringify(field)))
      } else {
        Json.parse(sanitize(Json.stringify(field)))
      }

    val sanitizedJson = JsObject(json.fields.map { case (key, value) =>
      key -> quoteJson(value)
    })

    s"INSERT INTO $table JSON '${Json.stringify(sanitizedJson)}';"
  }


  import java.util.regex.Pattern

  val pattern = Pattern.compile("[\\u0000-\\u001f]")

  def stripControlChars(s: String) =
    pattern.matcher(s).replaceAll("")

  def parseCassandraDataType(a: String, dt: DataType) =
    dt.getName match {
      case DataType.Name.ASCII => JsString(a)
      case DataType.Name.BLOB => JsString(a)
      case DataType.Name.DATE => JsString(a)
      case DataType.Name.INET => JsString(a)
      case DataType.Name.TEXT => JsString(a)
      case DataType.Name.TIME => JsString(a)
      case DataType.Name.TIMESTAMP => JsString(a)
      case DataType.Name.TIMEUUID => JsString(a)
      case DataType.Name.UUID => JsString(a)
      case DataType.Name.VARCHAR => JsString(a)
      case DataType.Name.BOOLEAN => JsBoolean(a == "true")
      case DataType.Name.BIGINT => JsNumber(BigDecimal(a))
      case DataType.Name.DECIMAL => JsNumber(BigDecimal(a))
      case DataType.Name.DOUBLE => JsNumber(BigDecimal(a))
      case DataType.Name.FLOAT => JsNumber(BigDecimal(a))
      case DataType.Name.INT => JsNumber(BigDecimal(a))
      case DataType.Name.SMALLINT => JsNumber(BigDecimal(a))
      case DataType.Name.TINYINT => JsNumber(BigDecimal(a))
      case DataType.Name.VARINT => JsNumber(BigDecimal(a))
      case DataType.Name.LIST => Json.parse(a)
      case DataType.Name.MAP => Json.parse(a)
      case DataType.Name.SET => Json.parse(a)
      case DataType.Name.TUPLE => Json.parse(a)
      case DataType.Name.UDT => Json.parse(a)
      case _ => Json.parse(a)
    }
}
