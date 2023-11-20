package com.lucendar.common.db.schema

import com.lucendar.common.db.rest.CustomFieldLoaderProvider
import com.lucendar.common.db.types.{QueryResultObjectLoader, Reflections}
import com.lucendar.common.utils.CommonUtils
import com.typesafe.scalalogging.Logger
import info.gratour.common.error.ErrorWithCode
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.select.{PlainSelect, Select, SelectExpressionItem, TableFunction}
import org.springframework.jdbc.core.RowMapper

import java.lang.reflect.{Field, Modifier}
import java.sql.ResultSet
import java.time.{LocalDate, LocalDateTime, OffsetDateTime}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object MapperBuilder {

  private val logger = Logger(MapperBuilder.getClass.getName)

  //  case class Col(name: String, originName: String, tableName: String)

  def parse(selectSql: String): Array[String] = {
    val select = CCJSqlParserUtil.parse(selectSql).asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect]
    val aliasMapper = mutable.Map[String, String]()

    val primaryTableName =
      select.getFromItem match {
        case t: Table =>
          t.getName

        case t: TableFunction =>
          t.getFunction.getName

        case t =>
          t.toString
      }

    //    val primaryTableName = select.getFromItem.asInstanceOf[Table].getName
    if (select.getFromItem.getAlias != null)
      aliasMapper += (select.getFromItem.getAlias.getName -> primaryTableName)

    val joins = select.getJoins
    if (joins != null) {
      joins.forEach(
        join => {
          val t = join.getRightItem.asInstanceOf[Table]
          if (t.getAlias != null)
            aliasMapper += (t.getAlias.getName -> t.getName)
        }
      )
    }

    val r = ArrayBuffer.empty[String]

    select.getSelectItems.forEach {
      case item: SelectExpressionItem =>
        item.getExpression match {
          case col: Column =>
            val alias = if (item.getAlias != null) item.getAlias.getName else col.getColumnName
            //            val t = if (col.getTable != null) col.getTable.getName else null
            //            val tableOfColumn = if (t != null) aliasMapper.getOrElse(t, t) else primaryTableName

            r += alias
          case _ =>
        }

      case _ =>
      // do nothing
    }

    r.toArray
  }

  private type SetMethod[T] = (T, ResultSet) => Unit;
  private type Setter[T] = (T, Field, ResultSet) => Unit;


  def buildLoader[T](
                      selectSql: String,
                      entryClass: Class[T],
                      fieldNameMapper: FieldNameMapper = FieldNameMapper.INSTANCE,
                      excludeFieldNames: Array[String] = null,
                      customFieldLoaderProvider: CustomFieldLoaderProvider[T] = null,
                      columnNames: Array[String] = null
                    ): QueryResultObjectLoader[T] = {
    //    val stmt = CCJSqlParserUtil.parse(selectSql)
    val cols =
      if (columnNames != null)
        columnNames
      else
        parse(selectSql)

    logger.whenTraceEnabled {
      logger.trace("ColumnNames: " + cols.mkString("{", ",", "}"))
    }


    def indexOf(fieldName: String): Int = {
      val columnName = fieldNameMapper.toFirstDbColumnName(fieldName)
      val cn = {
        val idx = columnName.indexOf('.')
        if (idx >= 0)
          columnName.substring(idx + 1)
        else
          columnName
      }


      if (excludeFieldNames != null) {
        if (excludeFieldNames.exists(fn => cn.equals(fn)))
          return -1;
      }


      cols.indexOf(cn)
    }

    val setMethodsBuffer = ArrayBuffer.empty[SetMethod[T]]
    var lastColumnIndex = 0

    CommonUtils.getInstanceFields(entryClass).foreach(f => {
      if (!Modifier.isTransient(f.getModifiers)) {
        val fieldName = f.getName
        val idx = indexOf(fieldName)

        logger.whenTraceEnabled {
          logger.trace(s"${fieldName} -> $idx")
        }

        if (idx >= 0) {
          val columnIndex = idx + 1
          if (columnIndex > lastColumnIndex)
            lastColumnIndex = columnIndex

          val typ = f.getType
          f.setAccessible(true)

          val setter: Setter[T] = typ match {
            case Reflections.JBoolean =>
              (t, f, rs) =>
                val v = rs.getBoolean(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Boolean.valueOf(v))

            case Reflections.JBooleanPrimitive =>
              (t, f, rs) =>
                f.setBoolean(t, rs.getBoolean(columnIndex))

            case Reflections.JByte =>
              (t, f, rs) =>
                val v = rs.getShort(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Byte.valueOf(v.toByte))

            case Reflections.JBytePrimitive =>
              (t, f, rs) =>
                f.setByte(t, rs.getShort(columnIndex).toByte)

            case Reflections.JShort =>
              (t, f, rs) =>
                val v = rs.getShort(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Short.valueOf(v))

            case Reflections.JShortPrimitive =>
              (t, f, rs) =>
                f.setShort(t, rs.getShort(columnIndex))

            case Reflections.JInteger =>
              (t, f, rs) =>
                val v = rs.getInt(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Integer.valueOf(v))

            case Reflections.JIntegerPrimitive =>
              (t, f, rs) =>
                f.setInt(t, rs.getInt(columnIndex))

            case Reflections.JLong =>
              (t, f, rs) =>
                val v = rs.getLong(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Long.valueOf(v))

            case Reflections.JLongPrimitive =>
              (t, f, rs) =>
                f.setLong(t, rs.getLong(columnIndex))

            case Reflections.JFloat =>
              (t, f, rs) =>
                val v = rs.getFloat(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Float.valueOf(v))

            case Reflections.JFloatPrimitive =>
              (t, f, rs) =>
                f.setFloat(t, rs.getFloat(columnIndex))

            case Reflections.JDouble =>
              (t, f, rs) =>
                val v = rs.getDouble(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Double.valueOf(v))

            case Reflections.JDoublePrimitive =>
              (t, f, rs) =>
                f.setDouble(t, rs.getDouble(columnIndex))

            case Reflections.JBigDecimal =>
              (t, f, rs) =>
                f.set(t, rs.getBigDecimal(columnIndex))

            case Reflections.JString =>
              (t, f, rs) =>
                f.set(t, rs.getString(columnIndex))

            case Reflections.JLocalDate =>
              (t, f, rs) =>
                f.set(t, rs.getObject(columnIndex, classOf[LocalDate]))

            case Reflections.JLocalDateTime =>
              (t, f, rs) =>
                f.set(t, rs.getObject(columnIndex, classOf[LocalDateTime]))

            case Reflections.JOffsetDateTime =>
              (t, f, rs) =>
                f.set(t, rs.getObject(columnIndex, classOf[OffsetDateTime]))

            case Reflections.JByteArray =>
              (t, f, rs) => {
                val bytes = rs.getBytes(columnIndex)
                f.set(t, bytes)
              }

            case Reflections.JIntArray =>
              (t, f, rs) => {
                val arr = rs.getArray(columnIndex)
                if (arr == null || rs.wasNull())
                  f.set(t, null)
                else {
                  val intArray: Array[Int] = arr.getArray().asInstanceOf[Array[Integer]].map(_.intValue())
                  f.set(t, intArray)
                }
              }

            case _ =>
              (t, f, rs) => {
                if (customFieldLoaderProvider != null) {
                  val loader = customFieldLoaderProvider.get(f.getName)
                  if (loader != null)
                    loader(t, f, rs, columnIndex)
                  else
                    throw ErrorWithCode.internalError(s"Unsupported entry field type: ${typ.getName}($typ).")
                } else
                  throw ErrorWithCode.internalError(s"Unsupported entry field type: ${typ.getName}($typ).")
              }
          }

          val method: SetMethod[T] = (t: T, rs: ResultSet) => {
            logger.whenDebugEnabled {
              logger.debug(s"Call mapper setter(columnIndex=$columnIndex) for ${f.getName}, typ=$typ")
            }
            setter(t, f, rs)
          }

          setMethodsBuffer += method
        }
      }
    })

    val setMethods = setMethodsBuffer.toArray

    new QueryResultObjectLoader(entryClass) {
      override def load(resultSet: ResultSet, entry: T): Int = {
        setMethods.foreach(m =>
          m(entry, resultSet)
        )

        lastColumnIndex
      }
    }
  }

  def build[T](
                selectSql: String,
                entryClass: Class[T],
                fieldNameMapper: FieldNameMapper = FieldNameMapper.INSTANCE,
                excludeFieldNames: Array[String] = null,
                customFieldLoaderProvider: CustomFieldLoaderProvider[T] = null,
                columnNames: Array[String] = null): RowMapper[T] =
    buildLoader(selectSql, entryClass, fieldNameMapper, excludeFieldNames, customFieldLoaderProvider, columnNames).toRowMapper


  /**
   * build loader exclude LOOKUP columns
   *
   * @param selectSql
   * @param entryClass
   * @param tableSchema
   * @tparam T
   * @return
   */
  def buildLoader[T](
                      selectSql: String,
                      entryClass: Class[T],
                      tableSchema: TableSchema
                    ): QueryResultObjectLoader[T] = {
    val cols = tableSchema.columns.filter(c => c.columnKind != ColumnKind.LOOKUP)

    def indexOf(fieldName: String): Int = {
      val columnName = tableSchema.fieldNameMapper.toFirstDbColumnName(fieldName)

      val col = tableSchema.findColumn(columnName)
      if (col == null)
        return -1;

      if (col.columnKind == ColumnKind.LOOKUP)
        return -1;

      cols.indexWhere(col => col.columnName == columnName)
    }

    val setMethodsBuffer = ArrayBuffer.empty[SetMethod[T]]
    var lastColumnIndex = 0

    CommonUtils.getInstanceFields(entryClass).foreach(f => {
      if (!Modifier.isTransient(f.getModifiers)) {
        val idx = indexOf(f.getName)
        if (idx >= 0) {
          val columnIndex = idx + 1
          if (columnIndex > lastColumnIndex)
            lastColumnIndex = columnIndex

          val typ = f.getType
          f.setAccessible(true)
          val setter: Setter[T] = typ match {
            case Reflections.JBoolean =>
              (t, f, rs) =>
                val v = rs.getBoolean(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Boolean.valueOf(v))

            case Reflections.JBooleanPrimitive =>
              (t, f, rs) =>
                f.setBoolean(t, rs.getBoolean(columnIndex))

            case Reflections.JByte =>
              (t, f, rs) =>
                val v = rs.getShort(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Byte.valueOf(v.toByte))

            case Reflections.JBytePrimitive =>
              (t, f, rs) =>
                f.setByte(t, rs.getShort(columnIndex).toByte)

            case Reflections.JShort =>
              (t, f, rs) =>
                val v = rs.getShort(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Short.valueOf(v))

            case Reflections.JShortPrimitive =>
              (t, f, rs) =>
                f.setShort(t, rs.getShort(columnIndex))

            case Reflections.JInteger =>
              (t, f, rs) =>
                val v = rs.getInt(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Integer.valueOf(v))

            case Reflections.JIntegerPrimitive =>
              (t, f, rs) =>
                f.setInt(t, rs.getInt(columnIndex))

            case Reflections.JLong =>
              (t, f, rs) =>
                val v = rs.getLong(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Long.valueOf(v))

            case Reflections.JLongPrimitive =>
              (t, f, rs) =>
                f.setLong(t, rs.getLong(columnIndex))

            case Reflections.JFloat =>
              (t, f, rs) =>
                val v = rs.getFloat(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Float.valueOf(v))

            case Reflections.JFloatPrimitive =>
              (t, f, rs) =>
                f.setFloat(t, rs.getFloat(columnIndex))

            case Reflections.JDouble =>
              (t, f, rs) =>
                val v = rs.getDouble(columnIndex)
                f.set(t, if (rs.wasNull()) null else java.lang.Double.valueOf(v))

            case Reflections.JDoublePrimitive =>
              (t, f, rs) =>
                f.setDouble(t, rs.getDouble(columnIndex))

            case Reflections.JBigDecimal =>
              (t, f, rs) =>
                f.set(t, rs.getBigDecimal(columnIndex))

            case Reflections.JString =>
              (t, f, rs) =>
                f.set(t, rs.getString(columnIndex))

            case Reflections.JLocalDate =>
              (t, f, rs) =>
                f.set(t, rs.getObject(columnIndex, classOf[LocalDate]))

            case Reflections.JLocalDateTime =>
              (t, f, rs) =>
                f.set(t, rs.getObject(columnIndex, classOf[LocalDateTime]))

            case Reflections.JOffsetDateTime =>
              (t, f, rs) =>
                f.set(t, rs.getObject(columnIndex, classOf[OffsetDateTime]))

            case Reflections.JByteArray =>
              (t, f, rs) => {
                val bytes = rs.getBytes(columnIndex)
                f.set(t, bytes)
              }

            case Reflections.JIntArray =>
              (t, f, rs) => {
                val arr = rs.getArray(columnIndex)
                if (arr == null || rs.wasNull())
                  f.set(t, null)
                else {
                  val intArray: Array[Int] = arr.getArray().asInstanceOf[Array[Integer]].map(_.intValue())
                  f.set(t, intArray)
                }
              }

            case _ =>
              (t, f, rs) =>
                throw ErrorWithCode.internalError(s"Unsupported entry field type: ${typ.getName}($typ).")

          }

          val method: SetMethod[T] = (t: T, rs: ResultSet) => {
            logger.whenDebugEnabled {
              logger.debug(s"Call loader setter(columnIndex=$columnIndex)  for ${f.getName}, typ=$typ")
            }
            setter(t, f, rs)
          }

          setMethodsBuffer += method
        }
      }
    })

    val setMethods = setMethodsBuffer.toArray

    new QueryResultObjectLoader(entryClass) {
      override def load(resultSet: ResultSet, entry: T): Int = {
        setMethods.foreach(m =>
          m(entry, resultSet)
        )

        lastColumnIndex
      }
    }
  }


  //  def build[T](selectSql: String, entryClass: Class[T], fieldNameMapper: FieldNameMapper = FieldNameMapper.INSTANCE): RowMapper[T] = {
  //    val stmt = CCJSqlParserUtil.parse(selectSql)
  //    val cols = parse(selectSql)
  //
  //    def indexOf(fieldName: String): Int = {
  //      val columnName = fieldNameMapper.toFirstDbColumnName(fieldName)
  //      cols.indexWhere(col => col.name == columnName)
  //    }
  //
  //    val setMethodsBuffer = ArrayBuffer.empty[SetMethod[T]]
  //
  //    CommonUtils.getInstanceFields(entryClass).foreach(f => {
  //      if (!Modifier.isTransient(f.getModifiers)) {
  //        val idx = indexOf(f.getName)
  //        if (idx >= 0) {
  //          val columnIndex = idx + 1
  //
  //          val typ = f.getType
  //          f.setAccessible(true)
  //          val setter: Setter[T] = typ match {
  //            case Reflections.JBoolean =>
  //              (t, f, rs) =>
  //                val v = rs.getBoolean(columnIndex)
  //                f.set(t, if (rs.wasNull()) null else java.lang.Boolean.valueOf(v))
  //
  //            case Reflections.JBooleanPrimitive =>
  //              (t, f, rs) =>
  //                f.setBoolean(t, rs.getBoolean(columnIndex))
  //
  //            case Reflections.JShort =>
  //              (t, f, rs) =>
  //                val v = rs.getShort(columnIndex)
  //                f.set(t, if (rs.wasNull()) null else java.lang.Short.valueOf(v))
  //
  //            case Reflections.JShortPrimitive =>
  //              (t, f, rs) =>
  //                f.setShort(t, rs.getShort(columnIndex))
  //
  //            case Reflections.JInteger =>
  //              (t, f, rs) =>
  //                val v = rs.getInt(columnIndex)
  //                f.set(t, if (rs.wasNull()) null else java.lang.Integer.valueOf(v))
  //
  //            case Reflections.JIntegerPrimitive =>
  //              (t, f, rs) =>
  //                f.setInt(t, rs.getInt(columnIndex))
  //
  //            case Reflections.JLong =>
  //              (t, f, rs) =>
  //                val v = rs.getLong(columnIndex)
  //                f.set(t, if (rs.wasNull()) null else java.lang.Long.valueOf(v))
  //
  //            case Reflections.JLongPrimitive =>
  //              (t, f, rs) =>
  //                f.setLong(t, rs.getLong(columnIndex))
  //
  //            case Reflections.JFloat =>
  //              (t, f, rs) =>
  //                val v = rs.getFloat(columnIndex)
  //                f.set(t, if (rs.wasNull()) null else java.lang.Float.valueOf(v))
  //
  //            case Reflections.JFloatPrimitive =>
  //              (t, f, rs) =>
  //                f.setFloat(t, rs.getFloat(columnIndex))
  //
  //            case Reflections.JDouble =>
  //              (t, f, rs) =>
  //                val v = rs.getDouble(columnIndex)
  //                f.set(t, if (rs.wasNull()) null else java.lang.Double.valueOf(v))
  //
  //            case Reflections.JDoublePrimitive =>
  //              (t, f, rs) =>
  //                f.setDouble(t, rs.getDouble(columnIndex))
  //
  //            case Reflections.JBigDecimal =>
  //              (t, f, rs) =>
  //                f.set(t, rs.getBigDecimal(columnIndex))
  //
  //            case Reflections.JString =>
  //              (t, f, rs) =>
  //                f.set(t, rs.getString(columnIndex))
  //
  //            case Reflections.JLocalDate =>
  //              (t, f, rs) =>
  //                f.set(t, rs.getObject(columnIndex, classOf[LocalDate]))
  //
  //            case Reflections.JLocalDateTime =>
  //              (t, f, rs) =>
  //                f.set(t, rs.getObject(columnIndex, classOf[LocalDateTime]))
  //
  //            case Reflections.JOffsetDateTime =>
  //              (t, f, rs) =>
  //                f.set(t, rs.getObject(columnIndex, classOf[OffsetDateTime]))
  //
  //            case Reflections.JByteArray =>
  //              (t, f, rs) => {
  //                val bytes = rs.getBytes(columnIndex)
  //                f.set(t, bytes)
  //              }
  //
  //            case Reflections.JEpochMillis =>
  //              (t, f, rs) => {
  //                val dt = rs.getObject(columnIndex, classOf[OffsetDateTime])
  //                val epochMillis = EpochMillis(dt)
  //                f.set(t, epochMillis)
  //              }
  //
  //            case _ =>
  //              (t, f, rs) =>
  //                throw ErrorWithCode.internalError(s"Unsupported entry field type: ${typ.getName}($typ).")
  //
  //          }
  //
  //          val method: SetMethod[T] = (t: T, rs: ResultSet) => {
  //            setter(t, f, rs)
  //          }
  //
  //          setMethodsBuffer += method
  //        }
  //      }
  //    })
  //
  //    val setMethods = setMethodsBuffer.toArray
  //
  //    (wrappedResultSet, _) => {
  //      val e = entryClass.newInstance()
  //      setMethods.foreach(m => {
  //        m(e, wrappedResultSet)
  //      })
  //      e
  //    }
  //  }

}
