package com.lucendar.common.db.types

import com.lucendar.common.db.jdbc.ResultSetAccessor
import org.springframework.jdbc.core.RowMapper

import java.sql.ResultSet
import java.time.OffsetDateTime

trait AccessorBasedRowMapper[T] {
  def map(accessor: ResultSetAccessor): T
}



case class IdAndOffsetDateTime(id: String, offsetDateTime: OffsetDateTime) {
  def idToLong: Long = id.toLong
  def dateTimeToEpochMillis: Long = offsetDateTime.toInstant.toEpochMilli
}

class IdAndOffsetDateTimeRowMapper extends RowMapper[IdAndOffsetDateTime] {
  override def mapRow(rs: ResultSet, rowNum: Int): IdAndOffsetDateTime =
    IdAndOffsetDateTime(rs.getString(1), rs.getObject(2, classOf[OffsetDateTime]))
}

object IdAndOffsetDateTimeRowMapper {
  def apply(): IdAndOffsetDateTimeRowMapper = new IdAndOffsetDateTimeRowMapper
}

class IntRowMapper extends RowMapper[java.lang.Integer] {
  override def mapRow(rs: ResultSet, rowNum: Int): Integer = {
    val r = rs.getInt(1)
    if (rs.wasNull())
      null
    else
      r
  }
}

object IntRowMapper {
  def apply(): IntRowMapper = new IntRowMapper
}

class LongRowMapper extends RowMapper[java.lang.Long] {
  override def mapRow(rs: ResultSet, rowNum: Int): java.lang.Long = {
    val r = rs.getLong(1)
    if (rs.wasNull())
      null
    else
      r
  }
}

object LongRowMapper {
  def apply(): LongRowMapper = new LongRowMapper
}

class StrRowMapper extends RowMapper[String] {
  override def mapRow(rs: ResultSet, rowNum: Int): String = rs.getString(1)
}

object StrRowMapper {
  def apply(): StrRowMapper = new StrRowMapper
}



/**
 *
 * @param entryClass entry class
 * @tparam T entry class
 */
abstract class QueryResultObjectLoader[T](entryClass: Class[T]) {

  /**
   *
   * @param resultSet
   * @param entry
   * @return lastColumnIndex
   */
  def load(resultSet: ResultSet, entry: T): Int

  def toRowMapper: RowMapper[T] =
    (resultSet, _) => {
      val e = entryClass.getDeclaredConstructor().newInstance()
      load(resultSet, e)
      e
    }
}

object Mappers {

}
