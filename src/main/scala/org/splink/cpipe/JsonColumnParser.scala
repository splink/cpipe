package org.splink.cpipe

import java.lang.{Double, Boolean}
import java.util.Date

import com.datastax.driver.core.{BatchStatement, DataType, PreparedStatement, Row, Session}
import play.api.libs.json._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object JsonColumnParser {

  case class Column(name: String, value: Object, typ: DataType)

  // SimpleDateFormat is not thread safe
  private val tlDateFormat = new ThreadLocal[java.text.SimpleDateFormat]

  private def dateFormat = {
    if (tlDateFormat.get() == null) {
      tlDateFormat.set(new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    }
    tlDateFormat.get()
  }

  def column2Json(column: Column) = {
      val value = column.value

      if (value == null) {
        Some(JsObject(Map(column.name -> JsNull)))
      } else {
        val sanitized: String = value match {
          case date: Date => dateFormat.format(date)
          case _ => stripControlChars(value.toString)
        }

        Try(Json.parse(sanitized)) match {
          case Success(json) =>
            val r = json match {
              case o: JsObject => o
              case _ => parseCassandraDataType(value, sanitized, column.typ)
            }

            Some(JsObject(Map(column.name -> r)))

          case Failure(_) =>
            Some(JsObject(Map(column.name -> parseCassandraDataType(value, sanitized, column.typ))))
        }
      }
  }

  def row2Json(row: Row) =
    row.getColumnDefinitions.iterator.asScala.flatMap { definition =>
      Try(row.getObject(definition.getName)) match {
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


  def json2PreparedStatement(table: String, json: JsObject, session: Session): PreparedStatement = {
    val str = s"INSERT INTO $table ( ${json.fields.map(_._1).mkString(", ")} ) VALUES ( ${json.fields.map(_ => "?").mkString(", ")} );"
    session.prepare(str)
  }

  def getStringToObjectMappingForTable(session: Session, table: String): Map[String, String => Object] = {
    val queryResult = session.execute(s"select * from $table limit 1")
    queryResult.getColumnDefinitions.asScala.map {
      definition => definition.getName -> getStringToObjectConversionMethod(definition.getType)
    }.toMap
  }

  def getStringToObjectConversionMethod(dataType: DataType): String => Object = (s: String) => {
      dataType.getName match {
        case DataType.Name.DATE => dateFormat.parse(s)
        case DataType.Name.TIMESTAMP => dateFormat.parse(s)
        case DataType.Name.DOUBLE => new Double(s.toDouble)
        case DataType.Name.INT => new Integer(s.toInt)
        case DataType.Name.VARCHAR => s
        case DataType.Name.BOOLEAN => new Boolean(s == "true")
        case _ => throw new IllegalArgumentException(s"Please add a mapping for the '${dataType.getName}' type")
    }
  }

  def jsValueToScalaObject(name: String, jsValue: JsValue, objectMapping: Map[String, String => Object]) : Object = {
    val v = jsValue.toString.stripPrefix("\"").stripSuffix("\"")
    objectMapping.get(name).getOrElse(throw new IllegalArgumentException(s"$name was not found in the map $objectMapping"))(v)
  }

  def addJsonToBatch(json: JsObject, preparedStatement: PreparedStatement, batch: BatchStatement, objectMapping: Map[String, String => Object]): Unit = {
    val values = json.fields.map { v => jsValueToScalaObject(v._1, v._2, objectMapping) }
    batch.add(preparedStatement.bind(values : _*))
  }

  import java.util.regex.Pattern

  val pattern = Pattern.compile("[\\u0000-\\u001f]")

  def stripControlChars(s: String) =
    pattern.matcher(s).replaceAll("")

  def parseCassandraDataType(v: Object, a: String, dt: DataType) = {
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
        case DataType.Name.DOUBLE => v match {
          case d: Double if Double.isNaN(d) => JsNull
          case _ => JsNumber(BigDecimal(a))
        }
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
}
