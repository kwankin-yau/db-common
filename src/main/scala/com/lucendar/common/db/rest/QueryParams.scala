package com.lucendar.common.db.rest

import com.lucendar.common.db.rest.QueryParams.HookedMapper
import com.lucendar.common.db.schema.{FieldConstraint, FieldDataType, FieldNameMapper, FieldResolver, MapperBuilder, PaginationSupportSpec}
import com.lucendar.common.db.types.{Predication, QueryResult, QueryResultObjectLoader, SqlDialect}
import com.lucendar.common.types.rest.Pagination
import com.lucendar.common.utils.{DateTimeUtils, StringUtils}
import com.typesafe.scalalogging.Logger
import info.gratour.common.error.{ErrorWithCode, Errors}
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.{PlainSelect, Select, SelectExpressionItem}
import org.springframework.jdbc.core.{JdbcTemplate, PreparedStatementSetter, ResultSetExtractor, RowMapper}

import java.lang.reflect.Field
import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, ZoneId}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait Sink[E <: AnyRef] {

  def offer(e: E): Unit
  def eof(): Unit
}

case class SearchConditionOption(notAllowRange: Boolean = false, equalOnly: Boolean = false)

object SearchConditionOption {
  val DEFAULT: SearchConditionOption = SearchConditionOption()
}

case class SearchConditionSpec(
                                paramName: String,
                                dataType: FieldDataType,
                                required: Boolean = false,
                                constraint: FieldConstraint = null,
                                predicationOverride: Array[Predication] = null,
                                option: SearchConditionOption
                              ) {

  case class Phase1ParseResult(
                                predication1: Predication,
                                values: Array[String],
                                predication2: Predication)

  def parse1(text: String, forcedPredication: Predication): Phase1ParseResult = {
    val EXPECT_PREDICATION_LEFT = 1
    val EXPECT_VALUES = 2
    val END = 3

    var state = EXPECT_PREDICATION_LEFT

    val str = new StringBuilder
    val values = ArrayBuffer.empty[String]
    var predication1: Predication = forcedPredication
    var predication2: Predication = null
    var escaped = false

    if (forcedPredication != null)
      state = EXPECT_VALUES


    text.chars().forEach(c => {
      val ch = c.toChar
      state match {
        case EXPECT_PREDICATION_LEFT =>
          ch match {
            case '(' =>
              if (dataType.isText)
                str.append(ch)
              else if (dataType.isBool)
                throw ErrorWithCode.invalidParam(paramName)
              else
                predication1 = Predication.GREAT

            case '[' =>
              if (dataType.isText)
                str.append(ch)
              else if (dataType.isBool)
                throw ErrorWithCode.invalidParam(paramName)
              else
                predication1 = Predication.GREAT_EQUAL

            case '\\' =>
              if (dataType.isText)
                escaped = true
              else
                throw ErrorWithCode.invalidParam(paramName)

            case '%' =>
              if (dataType.isText) {
                predication1 = Predication.START_WITH
              } else
                throw ErrorWithCode.invalidParam(paramName)

            case _ =>
              str.append(ch)
          }

          state = EXPECT_VALUES

        case EXPECT_VALUES =>
          if (escaped) {
            if (ch == '%')
              str.append('\\')
            else if (ch == 's')
              str.append(',')
            else
              str.append(ch)
            escaped = false
          } else {
            ch match {
              case ')' =>
                if (dataType.isText)
                  str.append(ch)
                else if (dataType.isBool)
                  throw ErrorWithCode.invalidParam(paramName)
                else {
                  if (predication1 == null)
                    predication1 = Predication.LESS
                  else
                    predication2 = Predication.LESS
                  state = END
                }

              case ']' =>
                if (dataType.isText)
                  str.append(ch)
                else if (dataType.isBool)
                  throw ErrorWithCode.invalidParam(paramName)
                else {
                  if (predication1 == null)
                    predication1 = Predication.LESS_EQUAL
                  else
                    predication2 = Predication.LESS_EQUAL
                  state = END
                }

              case ' ' =>
                if (dataType.isText || dataType.isTimestamp)
                  str.append(ch)
                else {
                  // ignore it
                }

              case '%' =>
                if (dataType.isText) {
                  if (predication1 == null) {
                    predication1 = Predication.END_WITH
                    state = END // not allow any other follow the data
                  } else if (predication1 == Predication.START_WITH) {
                    predication1 = Predication.INCLUDE
                    state = END // not allow any other follow the data
                  } else
                    throw ErrorWithCode.invalidParam(paramName)
                } else
                  throw ErrorWithCode.invalidParam(paramName)

              case ',' =>
                //                if (dataType.isBool)
                //                  throw ErrorWithCode.invalidParam(paramName)

                //                if (!dataType.isText)
                //                  str.append(ch)
                //                else {
                values += str.toString()
                str.setLength(0)
              //                }


              case '\\' =>
                if (dataType.isText)
                  escaped = true
                else
                  throw ErrorWithCode.invalidParam(paramName)


              case _ =>
                str.append(ch)
            }
          }

        case END =>
          // not allow any character after state END
          throw ErrorWithCode.invalidParam(paramName)
      }

    })

    if (str.nonEmpty)
      values += str.toString()

    if (predication1 == null)
      predication1 = Predication.EQUAL
    //    else if (predication1 == Predication.IN && values.size == 1) {
    //      val s = values(0)
    //      val arr = s.split(",")
    //      values.clear()
    //      values ++= arr
    //    }

    Phase1ParseResult(predication1, values.toArray, predication2)
  }

  def check(c: SearchCondition, dbColumnNames: Array[String]): ParsedSearchCondition = {
    if (required) {
      if (c == null)
        throw ErrorWithCode.invalidParam(paramName)
    }

    if (c == null)
      return null

    val phase1ParseResult = parse1(c.textToSearch, c.predication)
    if (phase1ParseResult.values.isEmpty)
      throw ErrorWithCode.invalidParam(paramName)

    // if use suffix style predication, then not allow any predication specified in value
    //    if (c.predication != null) {
    //      if (phase1ParseResult.predication1 != Predication.EQUAL)
    //        throw ErrorWithCode.invalidParam(paramName)
    //    }

    val (pred1, pred2) =
      if (c.predication != null) {
        (c.predication, null)
      } else {
        (phase1ParseResult.predication1, phase1ParseResult.predication2)
      }


    // check value count and predication

    if (pred1 != Predication.IN && phase1ParseResult.values.length > 2) // only allow more than two values with IN predication
      throw ErrorWithCode.invalidParam(paramName)

    if (pred1 != Predication.IN) {
      if (pred2 == null) {
        if (phase1ParseResult.values.length > 1)
          throw ErrorWithCode.invalidParam(paramName)
      }
    } else {
      if (phase1ParseResult.values.length > 500)
        throw ErrorWithCode.invalidParam(paramName, "Max allowed value count is 500.")
    }

    //    if (pred1 == Predication.EQUAL || Predication.isLike(pred1)) {
    //      if (phase1ParseResult.values.length > 1)
    //        throw ErrorWithCode.invalidParam(paramName)
    //    }

    // check dataType and predication
    if (!dataType.supportPredication(pred1))
      throw ErrorWithCode.invalidParam(paramName, s"${dataType} not support `${pred1}` predication.")

    if (option.notAllowRange) {
      if (pred2 != null)
        throw ErrorWithCode.invalidParam(paramName, "not allow range")
    }

    if (option.equalOnly) {
      if (pred1 != Predication.EQUAL)
        throw ErrorWithCode.invalidParam(paramName, "only support `=` predication.")
    }

    val parsedValues = ArrayBuffer.empty[Object]
    var v: Object = null

    if (pred1 == Predication.IS_NULL || pred1 == Predication.NOT_NULL) {
      // the value must be boolean true/false
      if (phase1ParseResult.values.length != 1)
        throw ErrorWithCode.invalidParam(paramName)

      v = StringUtils.tryParseBool(phase1ParseResult.values(0))
      if (v == null)
        throw ErrorWithCode.invalidParam(paramName)
      parsedValues += v
    } else {
      dataType match {
        case FieldDataType.BOOL =>
          if (phase1ParseResult.values.length != 1)
            throw ErrorWithCode.invalidParam(paramName)
          v = StringUtils.tryParseBool(phase1ParseResult.values(0))
          if (v == null)
            throw ErrorWithCode.invalidParam(paramName)
          parsedValues += v

        case FieldDataType.SMALL_INT =>
          for (s <- phase1ParseResult.values) {
            v = StringUtils.tryParseShort(s)
            if (v == null)
              throw ErrorWithCode.invalidParam(paramName)

            parsedValues += v
          }

        case FieldDataType.INT =>
          for (s <- phase1ParseResult.values) {
            v = StringUtils.tryParseInt(s)
            if (v == null)
              throw ErrorWithCode.invalidParam(paramName)

            parsedValues += v
          }

        case FieldDataType.BIGINT =>
          for (s <- phase1ParseResult.values) {
            v = StringUtils.tryParseLong(s)
            if (v == null)
              throw ErrorWithCode.invalidParam(paramName)

            parsedValues += v
          }

        case FieldDataType.TEXT =>
          phase1ParseResult.values.foreach(parsedValues += _)

        case FieldDataType.DECIMAL =>
          for (s <- phase1ParseResult.values) {
            v = StringUtils.tryParseDecimal(s)
            if (v == null)
              throw ErrorWithCode.invalidParam(paramName)

            parsedValues += v
          }


        case FieldDataType.FLOAT =>
          for (s <- phase1ParseResult.values) {
            v = StringUtils.tryParseFloat(s)
            if (v == null)
              throw ErrorWithCode.invalidParam(paramName)

            parsedValues += v
          }

        case FieldDataType.DOUBLE =>
          for (s <- phase1ParseResult.values) {
            v = StringUtils.tryParseDouble(s)
            if (v == null)
              throw ErrorWithCode.invalidParam(paramName)

            parsedValues += v
          }

        case FieldDataType.LOCAL_DATE =>
          for (s <- phase1ParseResult.values) {
            v = StringUtils.tryParseLocalDate(s)
            if (v == null)
              throw ErrorWithCode.invalidParam(paramName)

            parsedValues += v
          }

        case FieldDataType.LOCAL_DATETIME =>
          for (s <- phase1ParseResult.values) {
            v = StringUtils.tryParseLocalDateTime(s)
            if (v == null)
              throw ErrorWithCode.invalidParam(paramName)

            parsedValues += v
          }

        case FieldDataType.TIMESTAMP_WITH_ZONE =>
          for (s <- phase1ParseResult.values) {
            v = StringUtils.tryParseOffsetDateTime(s)
            if (v == null) {
              // check epoch-millis representation
              v = StringUtils.tryParseLong(s)
              if (v == null)
                throw ErrorWithCode.invalidParam(paramName)
            }

            parsedValues += v
          }

        case FieldDataType.BINARY | FieldDataType.INT_ARRAY =>
          throw ErrorWithCode.invalidParam(paramName) // binary data type does not support search
      }
    }

    ParsedSearchCondition(this, dbColumnNames, pred1, pred2, parsedValues.toArray)
  }

  def postCheck(values: Array[Object]): Unit = {
    if (constraint != null)
      constraint.checkValue(paramName, values)
  }
}

trait SearchConditionTrait {
  def paramName: String
}

case class SearchCondition(
                            paramName: String,
                            textToSearch: String,
                            predication: Predication = null
                          ) extends SearchConditionTrait

case class ParsedSearchCondition(
                                  spec: SearchConditionSpec,
                                  dbColumnNames: Array[String],
                                  predication: Predication,
                                  predication2: Predication,
                                  parsedValues: Array[Object]) extends SearchConditionTrait {
  override def paramName: String = spec.paramName
}

case class SearchConditions(
                             conditions: Array[SearchCondition]
                           ) {
  def isEmpty: Boolean = conditions == null || conditions.length == 0

  def find(paramName: String): Array[SearchCondition] =
    if (isEmpty) null
    else
      conditions.filter(_.paramName == paramName)

  def findFirst(paramName: String): SearchCondition =
    if (isEmpty) null
    else conditions.find(_.paramName == paramName).orNull

  def findStringValue(paramName: String): String = {
    val cond = findFirst(paramName)
    if (cond != null)
      cond.textToSearch
    else
      null
  }

  def findIntegerValue(paramName: String): Integer = {
    val cond = findFirst(paramName)
    if (cond != null) {
      if (cond.textToSearch != null && cond.textToSearch.nonEmpty)
        Integer.parseInt(cond.textToSearch)
      else
        null
    }
    else
      null
  }

  def addCondition(
                    searchCondition: SearchCondition
                  ): SearchConditions = {
    val newConditions: Array[SearchCondition] =
      if (conditions != null) {
        conditions :+ searchCondition
      } else
        Array(searchCondition)

    SearchConditions(newConditions)
  }
  def addCondition(
                    paramName: String,
                    textToSearch: String,
                    predication: Predication = null
                  ): SearchConditions = {
    val cond = SearchCondition(paramName, textToSearch, predication)
    addCondition(cond)
  }
}

object SearchConditions {
  def apply(conditions: Array[SearchCondition]): SearchConditions = new SearchConditions(conditions)

  def apply(conditions: (String, Any)*): SearchConditions = {
    if (conditions == null)
      throw new NullPointerException

    val arr = conditions.map(t => SearchCondition(t._1, t._2.toString, null)).toArray
    SearchConditions(arr)
  }

  def of(conditions: Seq[(String, Any)]): SearchConditions = {
    if (conditions == null)
      throw new NullPointerException

    val arr = conditions.map(t => SearchCondition(t._1, t._2.toString, null)).toArray
    new SearchConditions(arr)
  }

  def apply(conditions: java.util.List[SearchCondition]): SearchConditions = {
    val r: Array[SearchCondition] = new Array(conditions.size())
    new SearchConditions(conditions.toArray(r))
  }
}

case class ParsedSearchConditions(
                                   conditions: Array[ParsedSearchCondition]
                                 ) {
  def isEmpty: Boolean = conditions == null || conditions.isEmpty

  def find(paramName: String): Array[ParsedSearchCondition] =
    conditions.filter(c => c.paramName == paramName)
}

case class SortColumn(columnName: String, ascending: Boolean)

case class QueryParams(
                        conditions: SearchConditions,
                        pagination: Pagination,
                        sortColumns: Array[SortColumn],
                        fields: Array[String]
                      ) {
  def hasConditions: Boolean = conditions != null && !conditions.isEmpty

  def findConditions(paramName: String): Array[SearchCondition] =
    if (conditions != null) conditions.find(paramName)
    else null

  def addCondition(                            paramName: String,
                                               textToSearch: String,
                                               predication: Predication = null
                  ): QueryParams = {
    val condition = SearchCondition(paramName, textToSearch, predication)

    val newConditions: SearchConditions =
      if (conditions != null) {
        conditions.addCondition(condition)
      } else
        SearchConditions(Array(condition))

    copy(conditions = newConditions)
  }

  def findFirst(paramName: String): SearchCondition =
    if (conditions != null) conditions.findFirst(paramName)
    else null

  def findStringValue(paramName: String): String =
    conditions.findStringValue(paramName)

  def findIntegerValue(paramName: String): Integer =
    conditions.findIntegerValue(paramName)

  def hasSortColumns: Boolean = sortColumns != null && !sortColumns.isEmpty

  def hasField(fieldName: String): Boolean =
    if (fields != null)
      fields.contains(fieldName)
    else
      false
}

object QueryParams {

  val TOTAL_RECORD_COUNT_COLUMN_NAME = "__rc__"

  class HookedMapper[A <: AnyRef](val underlying: RowMapper[A])(implicit classTag: ClassTag[A]) extends RowMapper[A] {

    private var trc: java.lang.Long = _

    override def mapRow(v1: ResultSet, rowNum: Int): A = {
      if (trc == null) {
        trc = v1.getLong(QueryParams.TOTAL_RECORD_COUNT_COLUMN_NAME)
        if (trc == 0L)
          return null.asInstanceOf[A]
      }

      underlying.mapRow(v1, rowNum)
    }

    def totalRecordCount: Long =
      if (trc != null)
        trc
      else
        0L
  }

  def parseSortColumns(s: String): Array[SortColumn] = {
    if (s == null || s.isEmpty)
      null
    else {
      val v = s.split(",")
      //      var count = v.length
      val r = ArrayBuffer.empty[SortColumn]
      v.foreach(field => {
        val idx = field.indexOf('~')
        if (idx == 0)
          throw ErrorWithCode.invalidParam("__orderBy")

        val (fieldName, asc) =
          if (idx > 0) {
            val fieldName = field.substring(0, idx)
            val indicator = field.substring(idx + 1)

            val asc = indicator.toLowerCase match {
              case "asc" =>
                true
              case "desc" =>
                false
              case _ =>
                throw ErrorWithCode.invalidParam("__orderBy")
            }

            (fieldName, asc)
          } else {
            (field, true)
          }

        r += SortColumn(fieldName, asc)
      })

      r.toArray
    }
  }

  def apply(conditions: SearchConditions,
            pagination: Pagination,
            sortColumns: Array[SortColumn],
            fields: Array[String]): QueryParams = new QueryParams(conditions, pagination, sortColumns, fields)

  def apply(conditions: (String, Any)*): QueryParams =
    new QueryParams(SearchConditions(conditions: _*), null, null, null)

  def apply(pagination: Pagination, conditions: (String, Any)*): QueryParams =
    new QueryParams(SearchConditions(conditions: _*), pagination, null, null)

  //  def apply(pagination: Pagination, sortColumns: Array[SortColumn], fields: Array[String], conditions: (String, Any)*): QueryParams =
  //    new QueryParams(SearchConditions(conditions: _*), pagination, null, null)

  def opts(conditions: (String, Option[_])*): QueryParams = {
    if (conditions == null || conditions.isEmpty)
      NONE
    else {
      val arr = ArrayBuffer.empty[(String, Any)]
      conditions.foreach(c => {
        if (c._2.isDefined) {
          arr += (c._1 -> c._2.get)
        }
      })

      if (arr.isEmpty)
        NONE
      else
        new QueryParams(SearchConditions.of(arr.toSeq), null, null, null)
    }
  }


  val NONE: QueryParams = new QueryParams(null, null, null, null)
}


class QueryParamsBuilder() {


  private var conditions: ArrayBuffer[SearchCondition] = _
  private var pagination: Pagination = _
  private var sortColumns: ArrayBuffer[SortColumn] = _


  def condition(paramName: String, paramValue: String): QueryParamsBuilder = {
    if (conditions == null)
      conditions = ArrayBuffer.empty[SearchCondition]
    conditions += SearchCondition(paramName, paramValue, null)
    this
  }

  def condition(paramName: String, paramValue: Boolean): QueryParamsBuilder =
    condition(paramName, paramValue.toString)

  def condition(paramName: String, paramValue: Int): QueryParamsBuilder =
    condition(paramName, paramValue.toString)

  def condition(paramName: String, paramValue: Long): QueryParamsBuilder =
    condition(paramName, paramValue.toString)

  def condition(paramName: String, paramValue: Float): QueryParamsBuilder =
    condition(paramName, paramValue.toString)

  def condition(paramName: String, paramValue: Double): QueryParamsBuilder =
    condition(paramName, paramValue.toString)

  def condition(paramName: String, paramValue: BigDecimal): QueryParamsBuilder =
    condition(paramName, paramValue.toString)

  def condition(paramName: String, paramValue: LocalDate): QueryParamsBuilder =
    condition(paramName, paramValue.format(DateTimeFormatter.ISO_LOCAL_DATE))

  def condition(paramName: String, paramValue: LocalDateTime): QueryParamsBuilder =
    condition(paramName, paramValue.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))

  def condition(paramName: String, paramValue: OffsetDateTime): QueryParamsBuilder =
    condition(paramName, paramValue.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))

  def pagination(limit: Int, page: Int): QueryParamsBuilder = {
    this.pagination = new Pagination(limit, page)
    this
  }

  def sortColumn(columnName: String, ascending: Boolean = true): QueryParamsBuilder = {
    if (sortColumns == null)
      sortColumns = ArrayBuffer.empty[SortColumn]

    sortColumns += SortColumn(columnName, ascending)
    this
  }

  def build(): QueryParams = {
    val searchConditions =
      if (conditions != null && conditions.nonEmpty)
        SearchConditions(conditions.toArray)
      else
        null

    QueryParams(
      searchConditions,
      pagination,
      if (sortColumns != null && sortColumns.nonEmpty) sortColumns.toArray else null,
      null
    )
  }
}

object QueryParamsBuilder {
  def apply(): QueryParamsBuilder = new QueryParamsBuilder
}

trait SQLStringValueLiteral {
  def stringValueLiteral(s: String): String
}

// TODO: add a SQLDialect for p_is_grp_under
case class ParsedQueryParams[T <: AnyRef](
                                           spec: QueryParamsSpec[T],
                                           select: String,
                                           conditions: ParsedSearchConditions,
                                           pagination: Pagination,
                                           sortColumns: Array[SortColumn],
                                           fields: Array[String],
                                           groupByClause: String,
                                           sqlDialect: SqlDialect
                                         )(implicit tag: ClassTag[T]) {

  private val logger = Logger(ParsedQueryParams.getClass.getName)

  def hasConditions: Boolean = conditions != null && !conditions.isEmpty

  def hasField(fieldName: String): Boolean =
    if (fields != null)
      fields.contains(fieldName)
    else
      false

  def findCondition(paramName: String): Array[ParsedSearchCondition] =
    if (conditions != null)
      conditions.find(paramName)
    else
      null

  def singleCondValue(paramName: String): Object = {
    val conds = findCondition(paramName)
    if (conds == null || conds.isEmpty)
      throw ErrorWithCode.invalidParam(paramName)

    conds(0).parsedValues(0)
  }

//  def singleCondValueAsEpochMillis(paramName: String): EpochMillis = {
//    val conds = findCondition(paramName)
//    if (conds == null || conds.isEmpty)
//      throw ErrorWithCode.invalidParam(paramName)
//
//    conds(0).parsedValues(0) match {
//      case l: java.lang.Long =>
//        EpochMillis(l.longValue())
//
//      case epochMillis: EpochMillis =>
//        epochMillis
//
//      case ldt: LocalDateTime =>
//        EpochMillis(ldt.toInstant(DateTimeUtils.DEFAULT_ZONE_OFFSET).toEpochMilli)
//
//      case odt: OffsetDateTime =>
//        EpochMillis(odt.toInstant.toEpochMilli)
//
//      case zdt: ZonedDateTime =>
//        EpochMillis(zdt.toInstant.toEpochMilli)
//
//      case dat: Date =>
//        EpochMillis(dat.getTime)
//
//      case s: String =>
//        val odt = StringUtils.tryParseOffsetDateTime(s)
//        if (odt == null)
//          throw ErrorWithCode.invalidParam(paramName)
//
//        EpochMillis(odt.toInstant.toEpochMilli)
//
//      case _ =>
//        throw ErrorWithCode.invalidParam(paramName)
//    }
//  }

  def replaceSelect(newSelect: String): ParsedQueryParams[T] =
    ParsedQueryParams(spec, newSelect, conditions, pagination, sortColumns, fields, groupByClause, sqlDialect)

  def sqlWhereClauseLiteral(stringValueLiteral: SQLStringValueLiteral, columnNameMapper: String => String = null): String = {
    if (conditions != null && !conditions.isEmpty) {
      val where = new StringBuilder()
      where.append(" WHERE ")
      var first = true

      conditions.conditions.foreach(cond => {
        if (first)
          first = false
        else
          where.append(" AND ")

        def value(index: Int): String = cond.spec.dataType match {
          case FieldDataType.TEXT =>
            "'" + stringValueLiteral.stringValueLiteral(cond.parsedValues(index).toString) + "'"
          case FieldDataType.LOCAL_DATE =>
            "'" + cond.parsedValues(index).asInstanceOf[LocalDate].format(DateTimeFormatter.ISO_DATE) + "'"

          case FieldDataType.LOCAL_DATETIME =>
            "'" + cond.parsedValues(index).asInstanceOf[LocalDateTime].atZone(ZoneId.systemDefault()).toOffsetDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "'"

          case FieldDataType.TIMESTAMP_WITH_ZONE =>
            val v = cond.parsedValues(index) match {
              case l: java.lang.Long =>
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(l), DateTimeUtils.DEFAULT_ZONE_ID)
              case odt: OffsetDateTime =>
                odt
              case _ =>
                throw new ErrorWithCode(Errors.INVALID_PARAM, cond.paramName)
            }

            "'" + v.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "'"
          case FieldDataType.BINARY =>
            throw new ErrorWithCode(Errors.NOT_SUPPORTED, cond.paramName)
          case _ =>
            cond.parsedValues(index).toString
        }

        val expressions = cond.dbColumnNames.map(colName => {
          val columnName =
            if (columnNameMapper != null)
              columnNameMapper(colName)
            else
              colName

          val cond1 = cond.predication match {
            case Predication.EQUAL =>
              if (cond.predication2 != null)
                throw ErrorWithCode.internalError("cond.predication2 != null")
              columnName + " = " + value(0)

            case Predication.LESS =>
              columnName + " < " + value(0)

            case Predication.LESS_EQUAL =>
              columnName + " <= " + value(0)

            case Predication.GREAT =>
              columnName + " > " + value(0)

            case Predication.GREAT_EQUAL =>
              columnName + " >= " + value(0)

            case Predication.START_WITH =>
              if (cond.predication2 != null)
                throw ErrorWithCode.internalError("cond.predication2 != null")
              columnName + " ILIKE " + value(0) + " || '%'"


            case Predication.INCLUDE =>
              if (cond.predication2 != null)
                throw ErrorWithCode.internalError("cond.predication2 != null")
              columnName + " ILIKE '%' || " + value(0) + " || '%'"

            case Predication.END_WITH =>
              if (cond.predication2 != null)
                throw ErrorWithCode.internalError("cond.predication2 != null")
              columnName + " ILIKE '%' || " + value(0)

            case Predication.IN =>
              val str = new StringBuilder(columnName)
              str.append(" IN (")
              val values =
                if (cond.spec.dataType.isText) {
                  cond.parsedValues.map(v =>
                    "'" + stringValueLiteral.stringValueLiteral(v.toString) + "'"
                  ).mkString(",")
                  //                  val v = cond.parsedValues(0).toString
                  //                  val arr = v.split(",")
                  //                  arr.map(item => "'" + stringValueLiteral.stringValueLiteral(item) + "'").mkString(",")
                } else
                  cond.parsedValues.map(v => v.toString).mkString(",")
              str.append(values)
              str.append(")")
              str.toString()

            case Predication.IS_NULL =>
              if (cond.parsedValues(0).asInstanceOf[Boolean].booleanValue())
                columnName + " IS NULL"
              else
                columnName + " IS NOT NULL"

            case Predication.NOT_NULL =>
              if (cond.parsedValues(0).asInstanceOf[Boolean].booleanValue())
                columnName + " IS NOT NULL"
              else
                columnName + " IS NULL"

            case Predication.UNDER =>
              val v = cond.parsedValues(0)
              v match {
                case s: String =>
                  "p_is_grp_under('" + s + "'," + columnName + ")"
                case n: Number =>
                  "p_is_grp_under('" + n.longValue() + "'," + columnName + ")"
                case _ =>
                  throw ErrorWithCode.invalidParam(cond.paramName)
              }

            case _ =>
              throw ErrorWithCode.internalError(s"Unhandled case ${cond.predication}.")

          }

          val cond2 =
            if (cond.predication2 != null) {
              cond.predication2 match {
                case Predication.LESS =>
                  columnName + " < " + value(1)
                case Predication.LESS_EQUAL =>
                  columnName + " <= " + value(1)
                case Predication.GREAT =>
                  columnName + " > " + value(1)
                case Predication.GREAT_EQUAL =>
                  columnName + " >= " + value(1)

                case _ =>
                  throw ErrorWithCode.internalError(s"Unhandled case ${cond.predication2}.")
              }
            } else
              null

          if (cond2 == null)
            cond1
          else {
            "(" + cond1 + " AND " + cond2 + ")"
          }
        }) // fieldNames.map(fieldName => {

        if (expressions.length == 1)
          where.append(expressions(0))
        else {
          var flag = true
          val str = new StringBuilder
          expressions.foreach(expr => {
            if (flag)
              flag = false
            else
              str.append(" OR ")

            str.append(expr)
          })

          where.append(str.toString())
        }

      }) // conditions.conditions.foreach(cond => {

      where.toString()
    } else
      ""
  }

  def sqlWhereClause: String = {
    if (conditions != null && !conditions.isEmpty) {
      val where = new StringBuilder()
      where.append(" WHERE ")
      var first = true

      conditions.conditions.foreach(cond => {
        if (first)
          first = false
        else
          where.append(" AND ")


        val expressions = cond.dbColumnNames.map(columnName => {
          val cond1 = cond.predication match {
            case Predication.EQUAL =>
              if (cond.predication2 != null)
                throw ErrorWithCode.internalError("cond.predication2 != null")
              columnName + " = ?"

            case Predication.LESS =>
              columnName + " < ?"

            case Predication.LESS_EQUAL =>
              columnName + " <= ?"

            case Predication.GREAT =>
              columnName + " > ?"

            case Predication.GREAT_EQUAL =>
              columnName + " >= ?"

            case Predication.START_WITH =>
              if (cond.predication2 != null)
                throw ErrorWithCode.internalError("cond.predication2 != null")
              columnName + " ILIKE ? || '%'"

            case Predication.INCLUDE =>
              if (cond.predication2 != null)
                throw ErrorWithCode.internalError("cond.predication2 != null")
              columnName + " ILIKE '%' || ? || '%'"

            case Predication.END_WITH =>
              if (cond.predication2 != null)
                throw ErrorWithCode.internalError("cond.predication2 != null")
              columnName + " ILIKE '%' || ?"

            case Predication.IN =>
              val str = new StringBuilder(columnName)
              str.append(" IN (")
              str.append(cond.parsedValues.map(_ => "?").mkString(","))
              str.append(")")
              str.toString()

            case Predication.IS_NULL =>
              if (cond.parsedValues(0).asInstanceOf[Boolean].booleanValue())
                columnName + " IS NULL"
              else
                columnName + " IS NOT NULL"

            case Predication.NOT_NULL =>
              if (cond.parsedValues(0).asInstanceOf[Boolean].booleanValue())
                columnName + " IS NOT NULL"
              else
                columnName + " IS NULL"

            case Predication.UNDER =>
              val v = cond.parsedValues(0)
              if (!v.isInstanceOf[Number] && !v.isInstanceOf[String])
                throw ErrorWithCode.invalidParam(cond.paramName)

              "p_is_grp_under(?," + columnName + ")"


            case _ =>
              throw ErrorWithCode.internalError(s"Unhandled case ${cond.predication}.")

          }

          val cond2 =
            if (cond.predication2 != null) {
              cond.predication2 match {
                case Predication.LESS =>
                  columnName + "< ?"
                case Predication.LESS_EQUAL =>
                  columnName + "<= ?"
                case Predication.GREAT =>
                  columnName + "> ?"
                case Predication.GREAT_EQUAL =>
                  columnName + ">= ?"

                case _ =>
                  throw ErrorWithCode.internalError(s"Unhandled case ${cond.predication2}.")
              }
            } else
              null

          if (cond2 == null)
            cond1
          else {
            "(" + cond1 + " AND " + cond2 + ")"
          }
        }) // fieldNames.map(fieldName => {

        if (expressions.length == 1)
          where.append(expressions(0))
        else {
          var flag = true
          val str = new StringBuilder
          expressions.foreach(expr => {
            if (flag)
              flag = false
            else
              str.append(" OR ")

            str.append(expr)
          })

          where.append(str.toString())
        }

      }) // conditions.conditions.foreach(cond => {

      where.toString()
    } else
      ""
  }

  def sqlOrderBy(withoutQualifier: Boolean = false): String = {
    if (sortColumns != null && sortColumns.nonEmpty) {
      val str = new StringBuilder()
      str.append(" ORDER BY ")
      var first = true
      sortColumns.foreach(sc => {
        if (first)
          first = false
        else
          str.append(", ")

        val columnName = spec.fieldNameMapper.toFirstDbColumnName(sc.columnName)
        val cn =
          if (withoutQualifier) {
            if (columnName.indexOf('.') >= 0)
              columnName.substring(columnName.lastIndexOf('.') + 1)
            else
              columnName
          } else
            columnName

        str.append(cn)
        if (!sc.ascending)
          str.append(" DESC")
      })

      str.toString()
    } else ""
  }

  def toSqlNoPagination: String = {
    select + sqlWhereClause + sqlOrderBy(false)
  }

  def recordCountSql: String = {
    val where = sqlWhereClause
    val groupBy =
      if (groupByClause != null)
        groupByClause
      else
        ""

    var sub = select + where
    if (groupByClause != null)
      sub += " " + groupBy

    s"SELECT COUNT(1) FROM ($sub)"
  }

  def toSql: String = {
    val where = sqlWhereClause
    val orderBy = sqlOrderBy(false)
    val groupBy =
      if (groupByClause != null)
        groupByClause
      else
        ""


    if (pagination == null) {
      if (groupBy.nonEmpty)
        select + where + " " + groupBy + " " + orderBy
      else
        select + where + orderBy
    } else {
      if (sqlDialect != null && sqlDialect.supportSingleStatementPagination) {
        val orderByWithoutQualifier = sqlOrderBy(true)

        s"""
          WITH cte AS (
           $select
           $where
           $groupBy
           $orderBy
          )
          SELECT * FROM (
           TABLE cte
           $orderByWithoutQualifier
           LIMIT ${pagination.limit} OFFSET ${pagination.offset}
          ) sub
          RIGHT JOIN (SELECT count(1) FROM cte) c(__rc__) ON TRUE
        """
      } else {
        var s = select + where
        if (groupByClause != null)
          s += " " + groupByClause + " "
        s += orderBy
        s += s" LIMIT ${pagination.limit} OFFSET ${pagination.offset} "

        s
      }
    }
  }

  def toSqlNoWhere: String = {
    val orderBy = sqlOrderBy(false)

    if (pagination == null) {
      select + orderBy
    } else {
      val orderByWithoutQualifier = sqlOrderBy(true)

      s"""
          WITH cte AS (
           $select
           $orderBy
          )
          SELECT * FROM (
           TABLE cte
           $orderByWithoutQualifier
           LIMIT ${pagination.limit} OFFSET ${pagination.offset}
          ) sub
          RIGHT JOIN (SELECT count(1) FROM cte) c(__rc__) ON TRUE
        """
    }
  }

  private val selectSql: String = {
    val sql = toSql
    logger.whenDebugEnabled {
      logger.debug("SQL => " + sql)
    }
    sql
  }

  val StatementSetter: PreparedStatementSetter = new PreparedStatementSetter {
    override def setValues(ps: PreparedStatement): Unit = {
      var index = 1
      conditions.conditions.foreach(c => {
        if (c.predication != Predication.IS_NULL && c.predication != Predication.NOT_NULL) {
          c.dbColumnNames.foreach(_ => {
            c.parsedValues.foreach(v => {
              c.spec.dataType match {
                case FieldDataType.BOOL =>
                  ps.setBoolean(index, v.asInstanceOf[java.lang.Boolean])
                case FieldDataType.SMALL_INT =>
                  ps.setShort(index, v.asInstanceOf[java.lang.Short])
                case FieldDataType.INT =>
                  v match {
                    case num: Number =>
                      val i = num.intValue()
                      ps.setInt(index, i)

                    case str: String =>
                      val i = str.toInt
                      ps.setInt(index, i)

                    case _ =>
                      throw ErrorWithCode.invalidParam(c.paramName)
                  }

                case FieldDataType.BIGINT =>
                  v match {
                    case number: Number =>
                      val l = number.longValue()
                      ps.setLong(index, l)
                    case str: String =>
                      val l = str.toLong
                      ps.setLong(index, l)

                    case _ =>
                      throw ErrorWithCode.invalidParam(c.paramName)
                  }

                case FieldDataType.TEXT =>
                  if (v != null) {
                    ps.setString(index, v.toString)
                  } else
                    ps.setString(index, null)
                case FieldDataType.DECIMAL =>
                  ps.setBigDecimal(index, v.asInstanceOf[java.math.BigDecimal])
                case FieldDataType.FLOAT =>
                  ps.setFloat(index, v.asInstanceOf[java.lang.Float])
                case FieldDataType.DOUBLE =>
                  ps.setDouble(index, v.asInstanceOf[java.lang.Double])
                case FieldDataType.LOCAL_DATE | FieldDataType.LOCAL_DATETIME =>
                  ps.setObject(index, v)
                case FieldDataType.TIMESTAMP_WITH_ZONE =>
                  v match {
                    case _: OffsetDateTime | _: Timestamp =>
                      ps.setObject(index, v)
                    case l: java.lang.Long =>
                      val odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(l), DateTimeUtils.DEFAULT_ZONE_ID)
                      ps.setObject(index, odt)

                    case _ =>
                      throw ErrorWithCode.invalidParam(c.paramName)
                  }

                case _ =>
                  throw ErrorWithCode.invalidParam(c.paramName) // binary data type does not support search
              }

              index = index + 1
            })
          })
        } // if predication is not IS_NULL and not NOT_NULL
      })
    }
  }

  val Mapper: RowMapper[T] =
    if (pagination != null && sqlDialect != null && sqlDialect.supportSingleStatementPagination)
      new QueryParams.HookedMapper[T](spec.MAPPER)
    else
      spec.MAPPER

  def hookedMapper: HookedMapper[T] = Mapper.asInstanceOf[HookedMapper[T]]

  def convertToQueryResult(list: java.util.List[T]): QueryResult[T] = {
    if (pagination != null && sqlDialect != null && sqlDialect.supportSingleStatementPagination) {
      val count = Mapper.asInstanceOf[QueryParams.HookedMapper[T]].totalRecordCount
      if (count == 0)
        QueryResult.empty
      else {
        val arr = new Array[T](list.size())
        list.toArray(arr)
        new QueryResult(arr, count)
      }
    } else {
      if (pagination == null) {
        val count = list.size()
        val arr = new Array[T](count)
        list.toArray(arr)
        new QueryResult(arr, count)
      } else
        throw new ErrorWithCode(Errors.INTERNAL_ERROR, "`totalRecordCount` of QueryResult is expected.")
    }
  }

  def convertToQueryResult(list: java.util.List[T], totalRecordCount: Long): QueryResult[T] = {
    val count = list.size()
    val arr = new Array[T](count)
    list.toArray(arr)
    new QueryResult[T](arr, totalRecordCount)
  }

  def query()(implicit template: JdbcTemplate): QueryResult[T] = {
    var count: Long = 0
    if (sqlDialect == null || !sqlDialect.supportSingleStatementPagination) {
      val sql = recordCountSql
      count = template.query(sql, StatementSetter, new RowMapper[java.lang.Long] {
        override def mapRow(rs: ResultSet, rowNum: Int): java.lang.Long = {
          rs.getLong(1)
        }
      }).get(0)
    }

    val list = template.query(selectSql, StatementSetter, Mapper)
    if (sqlDialect == null || !sqlDialect.supportSingleStatementPagination)
      convertToQueryResult(list, count)
    else
      convertToQueryResult(list)
  }

  def queryWithVisitor(visitor: T => Unit)(implicit template: JdbcTemplate): java.util.List[T] = {
    template.query(selectSql, StatementSetter, new RowMapper[T] {
      override def mapRow(rs: ResultSet, rowNum: Int): T = {
        val r = Mapper.mapRow(rs, rowNum)
        visitor(r)
        r
      }
    })
  }

  def queryToSink(sink: Sink[T])(implicit template: JdbcTemplate): Unit = {
    template.query(selectSql, StatementSetter, new ResultSetExtractor[Void] {
      override def extractData(rs: ResultSet): Void = {
        var rowNum = 0
        while (rs.next()) {
          val e = Mapper.mapRow(rs, rowNum)
          rowNum += 1
          sink.offer(e)
        }

        sink.eof()

        null
      }
    })
  }

}

trait ExtraOrderByColumnsResolver {

  def supportOrderBy(
                      parsedSearchConditions: Seq[ParsedSearchCondition],
                      fieldName: String): Boolean

}

/**
 * 查询条件规范
 */
case class QueryParamsSpec[T <: AnyRef](
                                         select: String,
                                         entryClass: Class[T],
                                         searchConditionSpecs: Array[SearchConditionSpec],
                                         supportedOrderFields: Array[String],
                                         paginationSupport: PaginationSupportSpec = PaginationSupportSpec.OPTIONAL,
                                         fieldNameMapper: FieldNameMapper = FieldNameMapper.INSTANCE,
                                         extraOrderByColumnsResolver: ExtraOrderByColumnsResolver = null,
                                         mapper: RowMapper[T] = null,
                                         customFieldLoaderProvider: CustomFieldLoaderProvider[T] = null,
                                         groupByClause: String = null,
                                         columnNames: Array[String] = null,
                                         sqlDialect: SqlDialect = null
                                       )(implicit classTag: ClassTag[T]) {

  val DEFAULT_OBJECT_LOADER: QueryResultObjectLoader[T] =
    MapperBuilder.buildLoader(select, entryClass, fieldNameMapper, customFieldLoaderProvider = customFieldLoaderProvider, columnNames = columnNames)

  val MAPPER: RowMapper[T] =
    if (mapper != null)
      mapper
    else
      DEFAULT_OBJECT_LOADER.toRowMapper

  lazy val resultSetExtractor: ResultSetExtractor[T] =
    (rs: ResultSet) => {
      if (rs.next()) {
        MAPPER.mapRow(rs, 1)
      } else
        null.asInstanceOf[T]
    }

  def supportOrderBy(
                      parsedSearchConditions: Seq[ParsedSearchCondition],
                      fieldName: String): Boolean = {
    val r = supportedOrderFields != null && supportedOrderFields.contains(fieldName)
    if (!r) {
      if (extraOrderByColumnsResolver != null)
        extraOrderByColumnsResolver.supportOrderBy(parsedSearchConditions, fieldName)
      else
        false
    } else
      true
  }

  def extraOrderBy(extraOrderByColumnsResolver: ExtraOrderByColumnsResolver): QueryParamsSpec[T] = {
    this.copy(extraOrderByColumnsResolver = extraOrderByColumnsResolver)
  }

  def check(queryParams: QueryParams): ParsedQueryParams[T] = {
    val parsedSearchConditions = ArrayBuffer.empty[ParsedSearchCondition]

    searchConditionSpecs.foreach(
      spec => {
        val conds = queryParams.findConditions(spec.paramName)
        if (conds == null) {
          if (spec.required)
            throw ErrorWithCode.invalidParam(spec.paramName)
        } else {
          val dbColumnNames = fieldNameMapper.checkedToDbColumnNames(spec.paramName)
          conds.foreach(cond => {
            val parsedCond = spec.check(cond, dbColumnNames)
            parsedSearchConditions += parsedCond
            //            val index = parsedSearchConditions.indexWhere(c => c.paramName == parsedCond.paramName)
            //            if (index < 0)
            //
            //            else {
            //              val exists = parsedSearchConditions(index)
            //              parsedSearchConditions(index) = exists.copy(parsedValues = exists.parsedValues ++ parsedCond.parsedValues)
            //            }
          })
        }
      }
    )

    //    class PV(val pc: ParsedSearchCondition) {
    //      var values: Array[Object] = pc.parsedValues
    //    }
    //
    //    val names = new util.HashMap[String, PV]()
    //    parsedSearchConditions.foreach(pc => {
    //      val pv = names.get(pc.paramName)
    //      if (pv != null) {
    //        pv.values = pv.values ++ pc.parsedValues
    //      } else
    //        names.put(pc.paramName, new PV(pc))
    //    })
    //
    //    names.values().forEach(pv => {
    //      pv.pc.spec.postCheck(pv.values)
    //    })


    if (queryParams.hasSortColumns)
      queryParams.sortColumns.foreach(sc => {
        if (!supportOrderBy(parsedSearchConditions.toSeq, sc.columnName))
          throw ErrorWithCode.invalidParam(s"__orderBy(${sc.columnName})")
      })

    var pagination = queryParams.pagination
    paginationSupport match {
      case PaginationSupportSpec.DEFAULT =>
        if (pagination == null)
          pagination = Pagination.DEFAULT

      case PaginationSupportSpec.OPTIONAL =>
      case PaginationSupportSpec.MUST_SPECIFIED =>
        if (pagination == null)
          throw ErrorWithCode.invalidParam("__limit/__page")

      case PaginationSupportSpec.NOT_SUPPORT =>
        if (pagination != null)
          throw ErrorWithCode.invalidParam("__limit/__page")
    }

    ParsedQueryParams(
      this,
      select,
      ParsedSearchConditions(parsedSearchConditions.toArray),
      pagination,
      queryParams.sortColumns,
      queryParams.fields,
      groupByClause,
      sqlDialect)
  }

  def noPagination(): QueryParamsSpec[T] = copy(paginationSupport = PaginationSupportSpec.NOT_SUPPORT)
}


class QueryParamsSpecBuilder[T <: AnyRef](
                                           val select: String,
                                           val entryClass: Class[T],
                                           val fieldResolver: FieldResolver,
                                           val mapper: RowMapper[T],
                                           val customFieldLoaderProvider: CustomFieldLoaderProvider[T],
                                           val groupByClause: String,
                                           val columnNames: Array[String],
                                           val sqlDialect: SqlDialect)(implicit classTag: ClassTag[T]) {

  private val searchConditionSpecList = ArrayBuffer.empty[SearchConditionSpec]
  private val orderByColumns = ArrayBuffer.empty[String]
  private var paginationSupportSpec = PaginationSupportSpec.NOT_SUPPORT

  def condition(paramName: String,
                required: Boolean = false,
                constraint: FieldConstraint = null,
                predicationOverride: Array[Predication] = null,
                searchConditionOption: SearchConditionOption = SearchConditionOption.DEFAULT): QueryParamsSpecBuilder[T] = {

    val dataType = {
      val columnName = fieldResolver.getFieldNameMapper.toFirstDbColumnName(paramName)

      val dataType = fieldResolver.getFieldDataType(columnName)
      if (dataType == null)
        throw ErrorWithCode.invalidParam(s"Field `$columnName` is not defined.")
      dataType
    }

    searchConditionSpecList += SearchConditionSpec(paramName, dataType, required, constraint, predicationOverride, searchConditionOption)
    this
  }

  def requiredConditions(paramNames: String*): QueryParamsSpecBuilder[T] = {
    paramNames.foreach(condition(_, required = true))
    this
  }

  def optionalConditions(paramNames: String*): QueryParamsSpecBuilder[T] = {
    paramNames.foreach(condition(_))
    this
  }

  /**
   * define special search condition which has no db-mapping
   *
   * @param paramName
   * @param required
   * @param fieldDataType
   * @param constraint
   * @param predicationOverride
   * @return
   */
  def specialCondition(paramName: String,
                       fieldDataType: FieldDataType,
                       required: Boolean = false,
                       constraint: FieldConstraint = null,
                       predicationOverride: Array[Predication] = null,
                       searchConditionOption: SearchConditionOption = SearchConditionOption.DEFAULT): QueryParamsSpecBuilder[T] = {

    searchConditionSpecList += SearchConditionSpec(paramName, fieldDataType, required, constraint, predicationOverride, searchConditionOption)
    this
  }

  def conditionsFromSelect(fieldDataTypeResolver: Map[String, FieldDataType]): QueryParamsSpecBuilder[T] = {
    val parsedSelect = CCJSqlParserUtil.parse(select).asInstanceOf[Select]
    val plainSelect = parsedSelect.getSelectBody.asInstanceOf[PlainSelect]
    plainSelect.getSelectItems.forEach {
      case sei: SelectExpressionItem =>
        sei.getExpression match {
          case c: Column =>
            val columnName = if (sei.getAlias != null) {
              sei.getAlias.getName
            } else
              c.getColumnName

            val fieldName = fieldResolver.getFieldNameMapper.toApiFieldName(columnName)
            if (fieldDataTypeResolver == null)
              condition(fieldName)
            else {
              val dataType1 = fieldDataTypeResolver.get(fieldName).orNull
              val dataType = if (dataType1 != null) dataType1 else fieldResolver.getFieldDataType(columnName)
              specialCondition(fieldName, dataType)
            }
        }
    }

    this
  }

  def orderBy(fieldName: String): QueryParamsSpecBuilder[T] = {
    orderByColumns += fieldName
    this
  }

  def orderBy(fieldNames: String*): QueryParamsSpecBuilder[T] = {
    fieldNames.foreach(orderByColumns += _)
    this
  }

  def orderByAllParams(): QueryParamsSpecBuilder[T] = {
    searchConditionSpecList.foreach(orderByColumns += _.paramName)
    this
  }

  def paginationDefault(): QueryParamsSpecBuilder[T] = {
    paginationSupportSpec = PaginationSupportSpec.DEFAULT
    this
  }

  def paginationOpt(): QueryParamsSpecBuilder[T] = {
    paginationSupportSpec = PaginationSupportSpec.OPTIONAL
    this
  }

  def paginationMust(): QueryParamsSpecBuilder[T] = {
    paginationSupportSpec = PaginationSupportSpec.MUST_SPECIFIED
    this
  }

  def build()(implicit classTag: ClassTag[T]): QueryParamsSpec[T] =
    QueryParamsSpec(
      select,
      entryClass,
      searchConditionSpecList.toArray,
      orderByColumns.toArray,
      paginationSupportSpec,
      fieldResolver.getFieldNameMapper,
      extraOrderByColumnsResolver = null,
      mapper,
      customFieldLoaderProvider,
      groupByClause,
      columnNames,
      sqlDialect
    )

}


trait CustomFieldLoaderProvider[T] {

  /**
   * customFieldLoader(instance: T, field: Field, resultSet: ResultSet, columnIndex: Int): Unit
   */
  type CustomFieldLoader = (T, Field, ResultSet, Int) => Unit;

  def get(fieldName: String): CustomFieldLoader
}



