package com.lucendar.common.db.schema

import com.lucendar.common.db.jdbc.{SimplePreparedStatementCreator, StatementBinder, StatementBinderProcessor, StatementSetterWrapper}
import com.lucendar.common.db.rest.{CustomFieldLoaderProvider, QueryParamsSpecBuilder}
import com.lucendar.common.db.schema.UpsertBuilder.{COLUMN_REF, CURRENT_DATE, CURRENT_TIMESTAMP, DEFAULT, FIELD_STUB, GENERAL, NULL, Value}
import com.lucendar.common.db.types.SqlDialect
import com.typesafe.scalalogging.Logger
import info.gratour.common.error.{ErrorWithCode, Errors}
import info.gratour.common.types.Flag
import info.gratour.common.utils.CommonUtils
import org.springframework.jdbc.core.{BatchPreparedStatementSetter, JdbcTemplate, PreparedStatementSetter, ResultSetExtractor, RowMapper}

import java.io.ByteArrayInputStream
import java.lang.reflect.Field
import java.sql.{PreparedStatement, ResultSet}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


case class ColumnDef(
                      columnName: String,
                      fieldDataType: FieldDataType,
                      columnKind: ColumnKind = ColumnKind.ORDINARY,
                      constraint: FieldConstraint = null)

trait FieldDataTypeResolver {
  def getFieldDataType(fieldName: String): FieldDataType
}

trait FieldResolver extends FieldDataTypeResolver {

  def getFieldDataType(fieldName: String): FieldDataType

  def getFieldNameMapper: FieldNameMapper

}

case class WeakFieldResolver(fieldNameMapper: FieldNameMapper = FieldNameMapper.INSTANCE) extends FieldResolver {

  override def getFieldDataType(fieldName: String): FieldDataType = null

  override def getFieldNameMapper: FieldNameMapper = fieldNameMapper
}

case class WrappedFieldResolver(fieldResolver: FieldResolver, toDbFieldNameOverride: PartialFunction[String, Array[String]]) extends FieldResolver {
  override def getFieldDataType(fieldName: String): FieldDataType = fieldResolver.getFieldDataType(fieldName)

  override def getFieldNameMapper: FieldNameMapper = new FieldNameMapper() {
    override def toApiFieldName(columnName: String): String = fieldResolver.getFieldNameMapper.toApiFieldName(columnName)

    override def toDbColumnNames(fieldName: String): Array[String] = {
      if (toDbFieldNameOverride.isDefinedAt(fieldName))
        toDbFieldNameOverride.apply(fieldName)
      else
        fieldResolver.getFieldNameMapper.toDbColumnNames(fieldName)
    }
  }


}

case class TableSchema(
                        tableName: String,
                        columns: Seq[ColumnDef],
                        primaryKeyColumns: Array[ColumnDef],
                        sqlDialect: SqlDialect,
                        fieldNameMapper: FieldNameMapper = FieldNameMapper.INSTANCE)
  extends FieldResolver {

  def findColumn(columnName: String): ColumnDef = {
    columns.find(_.columnName.equals(columnName)).orNull
  }

  def getColumn(columnName: String): ColumnDef = {
    val r = findColumn(columnName)
    if (r == null)
      throw new ErrorWithCode(Errors.INTERNAL_ERROR, s"Column: `$columnName` does not found.")

    r
  }

  def queryParamsBuilder[T >: Null <: AnyRef](select: String, entryClass: Class[T],
                                              toDbFieldNameOverride: PartialFunction[String, Array[String]] = null,
                                              mapper: RowMapper[T] = null,
                                              customFieldLoaderProvider: CustomFieldLoaderProvider[T] = null,
                                              groupByClause: String = null,
                                              columnNames: Array[String] = null
                                             )(implicit classTag: ClassTag[T]): QueryParamsSpecBuilder[T] = {
    val resolver =
      if (toDbFieldNameOverride != null)
        WrappedFieldResolver(this, toDbFieldNameOverride)
      else
        this

    val cnArray =
      if (columnNames != null) {
        if (columnNames.isEmpty)
          columns.map(_.columnName).toArray
        else
          columnNames
      } else
        null

    new QueryParamsSpecBuilder(select, entryClass, resolver, mapper, customFieldLoaderProvider, groupByClause, cnArray, sqlDialect)
  }

  //  def queryParamsBuilder[T >: Null <: AnyRef](select: String, entryClass: Class[T], toDbFieldNameOverride: PartialFunction[String, Array[String]])(implicit classTag: ClassTag[T]): QueryParamsSpecBuilder[T] = {
  //    val resolver = WrappedFieldResolver(this, toDbFieldNameOverride)
  //    new QueryParamsSpecBuilder(select, entryClass, resolver, null, null)
  //  }
  //
  //  def queryParamsBuilder[T >: Null <: AnyRef](select: String, entryClass: Class[T], mapper: RowMapper[T])(implicit classTag: ClassTag[T]): QueryParamsSpecBuilder[T] =
  //    new QueryParamsSpecBuilder(select, entryClass, this, mapper)
  //
  //  def queryParamsBuilder[T >: Null <: AnyRef](select: String, entryClass: Class[T], mapper: RowMapper[T], toDbFieldNameOverride: PartialFunction[String, Array[String]])(implicit classTag: ClassTag[T]): QueryParamsSpecBuilder[T] = {
  //    val resolver = WrappedFieldResolver(this, toDbFieldNameOverride)
  //    new QueryParamsSpecBuilder(select, entryClass, resolver, mapper)
  //  }

  def upsertBuilder[T >: Null <: AnyRef](entryClass: Class[T]): UpsertBuilder[T] = {
    new UpsertBuilder[T](this, CommonUtils.getInstanceFields(entryClass).toArray)
  }

  def upsertBuilderClassBind[T >: Null <: AnyRef](entryClass: Class[T]): UpsertBuilderClassBind[T] =
    new UpsertBuilderClassBind[T](this, entryClass)

  /**
   * generate parameterised delete sql
   *
   * @return
   */
  def deleteSql(): String = {
    val str = new StringBuilder
    str.append("DELETE FROM ").append(tableName).append(" WHERE ")
    val flag = Flag(true)
    primaryKeyColumns.foreach(c => {
      if (flag.value)
        flag.value = false
      else {
        str.append(" AND ")
      }

      str.append(c.columnName).append(" = ?")
    })
    str.toString()
  }

  lazy val DELETE_SQL: String = deleteSql()

  def delete(keys: Any*)(implicit template: JdbcTemplate): Boolean = {

    template.update(DELETE_SQL, StatementSetterWrapper(binder => {
      keys.foreach(k =>
        binder.set(k))
      //      binder.bind(keys)
    })) > 0
  }

  def deleteEntry[T](e: T)(implicit template: JdbcTemplate, classTag: ClassTag[T]): Boolean = {
    val parameters = ArrayBuffer.empty[Object]

    val fields = CommonUtils.getInstanceFields(classTag.runtimeClass)
    primaryKeyColumns.foreach(f => {
      val fieldName = fieldNameMapper.toApiFieldName(f.columnName)
      val field = fields.find(_.getName.equals(fieldName)).orNull
      if (field == null)
        throw ErrorWithCode.invalidParam(fieldName)
      field.setAccessible(true)
      val value = field.get(e)
      parameters += value
    })

    delete(parameters)
  }

  override def getFieldDataType(fieldName: String): FieldDataType = {
    val fn =
      if (fieldName.indexOf('.') >= 0)
        fieldName.substring(fieldName.lastIndexOf('.') + 1)
      else
        fieldName

    val col = findColumn(fn)
    if (col == null)
      throw ErrorWithCode.invalidParam(s"Field `$fieldName` is not defined.")

    col.fieldDataType
  }

  override def getFieldNameMapper: FieldNameMapper = fieldNameMapper
}

object TableSchema {
  def builder(tableName: String, sqlDialect: SqlDialect): TableSchemaBuilder = new TableSchemaBuilder(tableName, sqlDialect)
}

case class FieldDataTypeOverride(tableSchema: TableSchema, fieldDataTypeResolver: Map[String, FieldDataType]) extends FieldResolver {
  override def getFieldDataType(fieldName: String): FieldDataType = {

    val r = fieldDataTypeResolver.get(tableSchema.getFieldNameMapper.toApiFieldName(fieldName)).orNull
    if (r == null)
      tableSchema.getFieldDataType(fieldName)
    else
      r
  }

  override def getFieldNameMapper: FieldNameMapper = tableSchema.getFieldNameMapper
}

class TableSchemaBuilder(val tableName: String, val sqlDialect: SqlDialect) {

  private val columns: ArrayBuffer[ColumnDef] = ArrayBuffer.empty[ColumnDef]
  private val primaryKeyColumns: ArrayBuffer[ColumnDef] = ArrayBuffer.empty[ColumnDef]

  def column(
              columnName: String,
              fieldDataType: FieldDataType,
              columnKind: ColumnKind = ColumnKind.ORDINARY,
              constraint: FieldConstraint = null): TableSchemaBuilder = {
    val col = ColumnDef(columnName, fieldDataType, columnKind, constraint)

    columns += col

    if (columnKind == ColumnKind.PRIMARY_KEY)
      primaryKeyColumns += col

    this
  }

  def ordinary(columnName: String, fieldDataType: FieldDataType, constraint: FieldConstraint = null): TableSchemaBuilder =
    column(columnName, fieldDataType, ColumnKind.ORDINARY, constraint)

  def primaryKey(columnName: String, fieldDataType: FieldDataType, constraint: FieldConstraint = null): TableSchemaBuilder =
    column(columnName, fieldDataType, ColumnKind.PRIMARY_KEY, constraint)

  def notNull(columnName: String, fieldDataType: FieldDataType, constraint: FieldConstraint = null): TableSchemaBuilder =
    column(columnName, fieldDataType, ColumnKind.NOT_NULL, constraint)

  def lookup(columnName: String, fieldDataType: FieldDataType, constraint: FieldConstraint = null): TableSchemaBuilder =
    column(columnName, fieldDataType, ColumnKind.LOOKUP, constraint)

  def build(fieldNameMapper: FieldNameMapper = FieldNameMapper.INSTANCE): TableSchema = {
    val pkColumns = columns.filter(_.columnKind == ColumnKind.PRIMARY_KEY).toArray
    if (pkColumns.isEmpty)
      throw ErrorWithCode.internalError("No primary key defined.")

    new TableSchema(tableName, columns.toSeq, pkColumns, sqlDialect, fieldNameMapper)
  }

}

object TableSchemaBuilder {
  def apply(tableName: String, sqlDialect: SqlDialect): TableSchemaBuilder = new TableSchemaBuilder(tableName, sqlDialect)
}

case class UpsertBuilderClassBind[T >: Null <: AnyRef](tableSchema: TableSchema, entryClass: Class[T]) {
  private val fieldsOfEntryClass: Array[Field] = CommonUtils.getInstanceFields(entryClass).toArray

  def upsertBuilder(): UpsertBuilder[T] =
    new UpsertBuilder[T](tableSchema, fieldsOfEntryClass)
}

/**
 *
 * @param tableSchema
 * @param fieldsOfEntryClass
 * @tparam T
 */
class UpsertBuilder[T >: Null <: AnyRef](val tableSchema: TableSchema, val fieldsOfEntryClass: Array[Field]) {

  private val logger: Logger = Logger(UpsertBuilder.getClass.getName)

  private var insertValues: ArrayBuffer[UpsertBuilder.ColumnValue] = _
  private var conflictUpdate: Boolean = false
  private var conflictDoNothing: Boolean = false
  private var updateValues: ArrayBuffer[UpsertBuilder.ColumnValue] = _
  private var excludedUpdateColumns: ArrayBuffer[String] = _
  private var searchExpression: String = _
  private var doReturning: Boolean = false
  private var definedReturningColumns: ArrayBuffer[String] = _

  private def findClassField(fieldName: String): Field = {
    fieldsOfEntryClass.find(_.getName.equals(fieldName)).orNull
  }

  private def getClassField(fieldName: String): Field = {
    val r = fieldsOfEntryClass.find(_.getName.equals(fieldName))
    if (r.isEmpty)
      throw ErrorWithCode.internalError(s"Field `$fieldName` was not found.")
    r.get
  }

  private def findInsertValue(columnName: String): UpsertBuilder.ColumnValue = {
    if (insertValues != null)
      insertValues.find(_.columnName.equals(columnName)).orNull
    else
      null
  }

  private def findInsertOrUpdateValue(columnName: String): UpsertBuilder.ColumnValue = {
    val r = findInsertValue(columnName)
    if (r == null)
      findUpdateValue(columnName)
    else
      r
  }

  private def isUpdateExcluded(columnName: String): Boolean = {
    if (excludedUpdateColumns != null)
      excludedUpdateColumns.contains(columnName)
    else
      false
  }

  private def findUpdateValue(columnName: String): UpsertBuilder.ColumnValue = {
    if (updateValues != null)
      updateValues.find(_.columnName.equals(columnName)).orNull
    else
      null
  }

  def insertValue(columnName: String, value: Object): UpsertBuilder[T] =
    insertValue(columnName, GENERAL(value, tableSchema.getColumn(columnName)))

  def insertValue(columnName: String, value: Value): UpsertBuilder[T] = {
    if (insertValues == null)
      insertValues = ArrayBuffer.empty[UpsertBuilder.ColumnValue]

    insertValues += UpsertBuilder.ColumnValue(columnName, value)

    this
  }

  def insertDefault(columnName: String, returning: Boolean = false): UpsertBuilder[T] = {
    insertValue(columnName, UpsertBuilder.DEFAULT)
    if (returning)
      withReturning(columnName)

    this
  }

  def onConflictDoNothing(): UpsertBuilder[T] = {
    this.conflictDoNothing = true
    this
  }

  def onConflictUpdate(excludedColumnNames: String*): UpsertBuilder[T] = {
    this.conflictUpdate = true
    if (excludedColumnNames != null) {
      if (excludedUpdateColumns == null)
        excludedUpdateColumns = ArrayBuffer.empty[String]

      excludedUpdateColumns ++= excludedColumnNames
    }

    this
  }

  def onConflictUpdate(): UpsertBuilder[T] = {
    onConflictUpdate(null)
  }

  def updateValue(columnName: String, value: Object): UpsertBuilder[T] = {
    if (updateValues == null)
      updateValues = ArrayBuffer.empty[UpsertBuilder.ColumnValue]

    updateValues += UpsertBuilder.ColumnValue(columnName, GENERAL(value, tableSchema.getColumn(columnName)))

    this
  }

  def updateValue(columnName: String, value: Value): UpsertBuilder[T] = {
    if (updateValues == null)
      updateValues = ArrayBuffer.empty[UpsertBuilder.ColumnValue]

    updateValues += UpsertBuilder.ColumnValue(columnName, value)

    this
  }

  def excludeUpdate(excludedColumnNames: String*): UpsertBuilder[T] = {
    if (excludedColumnNames != null) {
      if (excludedUpdateColumns == null)
        excludedUpdateColumns = ArrayBuffer.empty[String]

      excludedUpdateColumns ++= excludedColumnNames
    }

    this
  }

  /**
   * The where clause of upsert builder does not support parameter.
   *
   * <pre>
   * where('f_update_time < exclude.f_update_time')         // SUPPORTED
   * where('f_age > 10')                                    // SUPPORTED
   * where('f_age > ?')                                     // NOT SUPPORTED
   * </pre>
   *
   * @param searchExpression
   * @return
   */
  def where(searchExpression: String): UpsertBuilder[T] = {
    this.searchExpression = searchExpression

    this
  }

  /**
   * returning all fields both defined in entryClass and columns
   *
   * @return
   */
  def withReturning(): UpsertBuilder[T] = {
    this.doReturning = true

    this
  }

  /**
   * returning fields specified in columnNames
   *
   * @param columnNames
   * @return
   */
  def withReturning(columnNames: String*): UpsertBuilder[T] = {
    this.doReturning = true
    if (columnNames != null) {
      if (definedReturningColumns == null)
        definedReturningColumns = ArrayBuffer.empty[String]

      definedReturningColumns.appendAll(columnNames)
    }


    this
  }

  private def appendValue(str: StringBuilder, columnValue: UpsertBuilder.ColumnValue): Unit = {
    columnValue.value match {
      case CURRENT_TIMESTAMP =>
        str.append("CURRENT_TIMESTAMP")

      case CURRENT_DATE =>
        str.append("CURRENT_DATE")

      case NULL =>
        str.append("NULL")

      case DEFAULT =>
        str.append("DEFAULT")

      case COLUMN_REF(columnName) =>
        str.append(columnName)

      case _ =>
        str.append("?")
    }
  }


  private def generateInsert(str: StringBuilder, parameters: ArrayBuffer[Value]): Unit = {
    str.append("INSERT INTO ").append(tableSchema.tableName).append('(')
    val flag = Flag(true)
    for (c <- tableSchema.columns) {
      if (c.columnKind.isPersisted) {
        if (flag.value)
          flag.value = false
        else str.append(", ")

        str.append(c.columnName)
      }
    }
    str.append(") VALUES (")
    flag.value = true
    for (c <- tableSchema.columns) {
      if (c.columnKind.isPersisted) {
        if (flag.value)
          flag.value = false
        else str.append(", ")

        val value = findInsertValue(c.columnName)
        if (value != null) {
          appendValue(str, value)
          value.value match {
            case general: GENERAL =>
              if (c.columnKind.isValueRequired) {
                val v = general.value
                if (v == null)
                  throw ErrorWithCode.invalidParam(c.columnName)
              }
              parameters += general

            case _ =>
          }
        } else {

          val fieldName = tableSchema.fieldNameMapper.toApiFieldName(c.columnName)
          val field = findClassField(fieldName)
          if (field != null) {
            str.append("?")
            field.setAccessible(true)
            parameters += FIELD_STUB(field, c)
          } else
            str.append("NULL")
        }
      }
    }
    str.append(")")
  }

  private def generateOnConflictUpdate(str: StringBuilder, parameters: ArrayBuffer[Value]): Unit = {
    val flag = Flag(true)
    tableSchema.columns.foreach(c => {
      if (!isUpdateExcluded(c.columnName) && c.columnKind.isPersisted) {
        if (flag.value) {
          flag.value = false
          str.append(" ON CONFLICT(")
          val flag2 = Flag(true)
          tableSchema.primaryKeyColumns.foreach(c => {
            if (flag2.value)
              flag2.value = false
            else
              str.append(", ")

            str.append(c.columnName)
          })
          str.append(") DO UPDATE SET ")
        } else
          str.append(", ")

        val value = findUpdateValue(c.columnName)
        if (value == null)
          str.append(c.columnName).append(" = excluded.").append(c.columnName)
        else {
          str.append(c.columnName).append(" = ")
          appendValue(str, value)
          value.value match {
            case general: GENERAL =>
              if (c.columnKind.isValueRequired) {
                if (general.value == null)
                  throw ErrorWithCode.invalidParam(c.columnName)
              }
              parameters += general
          }
        }
      }
    })
  }

  private def generateOnConflictDoNothing(str: StringBuilder): Unit = {
    str.append(" ON CONFLICT DO NOTHING")
  }

  private def generateReturning(str: StringBuilder): Unit = {
    val flag = Flag(true)

    if (definedReturningColumns == null) {
      tableSchema.columns.foreach(columnDef => {
        if (columnDef.columnKind.isPersisted) {
          if (flag.value) {
            flag.value = false

            str.append(" RETURNING ")
          } else
            str.append(", ")

          str.append(columnDef.columnName)
        }
      })
//      for (f <- fieldsOfEntryClass) {
//        val fieldName = f.getName
//        val columnName = tableSchema.fieldNameMapper.toFirstDbColumnName(fieldName)
//        val columnDef = tableSchema.findColumn(columnName)
//        if (columnDef != null && columnDef.columnKind.isPersisted) {
//          if (flag.value) {
//            flag.value = false
//
//            str.append(" RETURNING ")
//          } else
//            str.append(", ")
//
//          str.append(columnDef.columnName)
//        }
//      }
    } else {
      for (columnName <- definedReturningColumns) {
        if (flag.value) {
          flag.value = false

          str.append(" RETURNING ")
        }
        else
          str.append(", ")

        str.append(columnName)
      }
    }
  }

  private def generateUpdate(str: StringBuilder, parameters: ArrayBuffer[Value]): Unit = {
    str.append("UPDATE ").append(tableSchema.tableName).append(" SET ")
    val flag = Flag(true)
    for (c <- tableSchema.columns) {
      if (!isUpdateExcluded(c.columnName) && c.columnKind != ColumnKind.PRIMARY_KEY && c.columnKind.isPersisted) {
        if (flag.value)
          flag.value = false
        else str.append(", ")

        str.append(c.columnName).append(" = ")
        val value = findUpdateValue(c.columnName)
        if (value != null) {
          appendValue(str, value)
          value.value match {
            case general: GENERAL =>
              if (c.columnKind.isValueRequired) {
                if (general.value == null)
                  throw ErrorWithCode.invalidParam(c.columnName)
              }

              parameters += general
          }
        } else {
          val fieldName = tableSchema.fieldNameMapper.toApiFieldName(c.columnName)
          val field = findClassField(fieldName)
          if (field != null) {
            str.append("?")
            field.setAccessible(true)
            parameters += FIELD_STUB(field, c)
          } else
            str.append("NULL")
        }
      }
    }

    str.append(" WHERE ")
    flag.value = true
    for (c <- tableSchema.columns) {
      if (c.columnKind == ColumnKind.PRIMARY_KEY) {
        if (flag.value)
          flag.value = false
        else str.append(" AND ")

        val fieldName = tableSchema.fieldNameMapper.toApiFieldName(c.columnName)
        val field = getClassField(fieldName)
        field.setAccessible(true)
        parameters += FIELD_STUB(field, c)

        str.append(c.columnName).append(" = ?")
      }
    }
  }

  private def generateDynamicUpdate(entry: T, str: StringBuilder, parameters: ArrayBuffer[Value]): Unit = {
    str.append("UPDATE ").append(tableSchema.tableName).append(" SET ")
    val flag = Flag(true)
    for (c <- tableSchema.columns) {
      if (c.columnKind != ColumnKind.PRIMARY_KEY && c.columnKind.isPersisted) {
        val fieldName = tableSchema.fieldNameMapper.toApiFieldName(c.columnName)
        val field = findClassField(fieldName)
        if (field != null) {
          field.setAccessible(true)
          val v = field.get(entry)
          if (v != null) {
            if (flag.value)
              flag.value = false
            else str.append(", ")

            str.append(c.columnName).append(" = ?")

            parameters += FIELD_STUB(field, c)
          }
        }
      }
    }

    str.append(" WHERE ")
    flag.value = true
    for (c <- tableSchema.columns) {
      if (c.columnKind == ColumnKind.PRIMARY_KEY) {
        if (flag.value)
          flag.value = false
        else str.append(" AND ")

        val fieldName = tableSchema.fieldNameMapper.toApiFieldName(c.columnName)
        val field = getClassField(fieldName)
        field.setAccessible(true)
        parameters += FIELD_STUB(field, c)

        str.append(c.columnName).append(" = ?")
      }
    }
  }

  def buildUpsert: Upsert[T] = {
    val parameters = ArrayBuffer.empty[UpsertBuilder.Value]
    val str = new StringBuilder
    generateInsert(str, parameters)
    if (conflictUpdate)
      generateOnConflictUpdate(str, parameters)
    else if (conflictDoNothing)
      generateOnConflictDoNothing(str)
    if (doReturning)
      generateReturning(str)
    val sql = str.toString()
    Upsert(sql, parameters.toArray, doReturning)
  }

  def buildInsert: Upsert[T] = {
    val parameters = ArrayBuffer.empty[UpsertBuilder.Value]
    val str = new StringBuilder
    generateInsert(str, parameters)
    if (doReturning)
      generateReturning(str)
    val sql = str.toString()
    Upsert(sql, parameters.toArray, doReturning)
  }

  def buildDynamicUpdate(entry: T): Upsert[T] = {
    val parameters = ArrayBuffer.empty[UpsertBuilder.Value]
    val str = new StringBuilder
    generateDynamicUpdate(entry, str, parameters)
    if (doReturning)
      generateReturning(str)
    val sql = str.toString()
    Upsert(sql, parameters.toArray, doReturning)
  }

  def buildUpdate: Upsert[T] = {
    val parameters = ArrayBuffer.empty[UpsertBuilder.Value]
    val str = new StringBuilder
    generateUpdate(str, parameters)
    if (doReturning)
      generateReturning(str)
    val sql = str.toString()
    Upsert(sql, parameters.toArray, doReturning)
  }

  //  def bind[A](sql: SQL[A, NoExtractor], entry: T): SQL[A, NoExtractor] = {
  //    val parameters = ArrayBuffer.empty[Object]
  //    var fields: Array[Field] = null
  //    // insert part
  //    tableSchema.columns.foreach(col => {
  //      val value = findInsertOrUpdateValue(col.columnName)
  //      if (value != null) {
  //        value.value match {
  //          case GENERAL(v) =>
  //            parameters += v
  //        }
  //      } else {
  //        if (fields == null)
  //          fields = CommonUtils.getDeclaredFields(entry.getClass).toArray
  //
  //        val fieldName = tableSchema.fieldNameMapper.toApiFieldName(col.columnName)
  //        val field = fields.find(f => f.getName.equals(fieldName)).orNull
  //        if (field == null)
  //          throw new ErrorWithCode(Errors.INTERNAL_ERROR, s"No field for `${col.columnName}`.")
  //        field.setAccessible(true)
  //        parameters += field.get(entry)
  //      }
  //    })
  //
  //    sql.bind(parameters: _*)
  //  }
  //
  //  def execute[R >: Null <: AnyRef](entry: T, rowMapper: ScalikeMapper[R])(implicit session: DBSession): R = {
  //    val sql = toSql
  //    logger.debug(sql)
  //    bind(SQL(toSql), entry)
  //      .map(rowMapper)
  //      .single()
  //      .apply()
  //      .orNull
  //  }

}

case class Upsert[T >: Null <: AnyRef](sql: String, parameters: Array[Value], withReturning: Boolean) {

  import Upsert.logger

  logger.whenDebugEnabled {
    logger.debug(s"sql=$sql")
    logger.debug(s"parameters=$parameters")
  }

  def bind[A](binder: StatementBinder, entry: T): PreparedStatement = {
    //    val values = ArrayBuffer.empty[Object]
    parameters.foreach {
      case GENERAL(value, c) =>
        if (value == null) {
          if (c.columnKind.isValueRequired)
            throw ErrorWithCode.invalidValue(c.columnName)
        }
        binder.set(value)
      //        values += value

      case UpsertBuilder.FIELD_STUB(field, c) =>
        logger.whenDebugEnabled {
          logger.debug(s"bind $field, $c")
        }
        val notNull = c.columnKind.isValueRequired
        val v = field.get(entry)
        if (notNull && v == null)
          throw ErrorWithCode.invalidValue(field.getName)


        //        field.getType

        v match {
          case bytes: Array[Byte] =>
            binder.set(new ByteArrayInputStream(bytes), c.fieldDataType)
          case intArr: Array[Int] =>
            binder.setIntArray(intArr)
          case _ =>
            if (v == null) {
              //              val clzz = field.getType
              binder.setNull(c.fieldDataType)
            } else
              binder.set(v, c.fieldDataType)
        }

      case _ =>
        throw ErrorWithCode.internalError("Unexpected case.")
    }

    binder.st
  }

  def bind[A](statement: PreparedStatement, entry: T): PreparedStatement = {
    val binder = StatementBinder(statement)
    bind(binder, entry)
  }

  def buildStatementSetter(entry: T): PreparedStatementSetter = new PreparedStatementSetter {
    override def setValues(ps: PreparedStatement): Unit = {
      bind(ps, entry)
    }
  }

  def buildStatementSetter(entry: T, preprocessor: StatementBinderProcessor, postProcessor: StatementBinderProcessor): PreparedStatementSetter = new PreparedStatementSetter {
    override def setValues(ps: PreparedStatement): Unit = {
      val binder = StatementBinder(ps)
      if (preprocessor != null)
        preprocessor.process(binder)

      bind(binder, entry)

      if (postProcessor != null)
        postProcessor.process(binder)
    }
  }

  def buildBatchStatementSetter(entries: Array[T]): BatchPreparedStatementSetter = new BatchPreparedStatementSetter {
    override def setValues(ps: PreparedStatement, i: Int): Unit = {
      bind(ps, entries(i))
    }

    override def getBatchSize: Int = entries.length
  }

  def execute[R >: Null <: AnyRef](entry: T, rowMapper: RowMapper[R])(implicit template: JdbcTemplate): R = {
    if (withReturning) {
      template.query(new SimplePreparedStatementCreator(sql), new PreparedStatementSetter {
        override def setValues(ps: PreparedStatement): Unit = {
          bind(ps, entry)
        }
      }, new ResultSetExtractor[R] {
        override def extractData(rs: ResultSet): R = {
          if (rs.next()) {
            rowMapper.mapRow(rs, 1)
          } else
            null
        }
      })
    } else {
      template.update(sql, new PreparedStatementSetter {
        override def setValues(ps: PreparedStatement): Unit = {
          bind(ps, entry)
        }
      })

      null
    }
  }

  /**
   *
   * @param entry
   * @param template
   * @return effected row count
   */
  def execute(entry: T)(implicit template: JdbcTemplate): Int = {
    template.update(sql, new PreparedStatementSetter {
      override def setValues(ps: PreparedStatement): Unit = {
        bind(ps, entry)
      }
    })
  }

}

object Upsert {
  private val logger = Logger("info.gratour.db.sql.Upsert")
}

object UpsertBuilder {

  trait Value

  case class ColumnValue(columnName: String, value: Value)

  case class GENERAL(value: Object, column: ColumnDef) extends Value

  case object CURRENT_TIMESTAMP extends Value

  case object CURRENT_DATE extends Value

  case object NULL extends Value

  case object DEFAULT extends Value

  case class COLUMN_REF(columnName: String) extends Value

  private[schema] case class FIELD_STUB(field: Field, column: ColumnDef) extends Value

}
