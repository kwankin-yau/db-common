package com.lucendar.common.db.jdbc


import java.sql.{ResultSet, SQLException, Timestamp}
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, ZoneId, ZoneOffset}
import com.google.gson.{Gson, GsonBuilder}
import info.gratour.common.types.{EpochMillis, IncIndex}
import info.gratour.common.utils.DateTimeUtils
import org.springframework.jdbc.core.RowMapper

import java.util.TimeZone

class ResultSetAccessor(val resultSet: ResultSet) {

  var rs: ResultSet = resultSet

  val colIndex: IncIndex = IncIndex()

  def next(): Boolean = {
    val r = rs.next()
    if (r)
      reset()

    r
  }

  def reset(): Unit = colIndex.index = 0

  def reset(resultSet: ResultSet): ResultSetAccessor = {
    rs = resultSet
    colIndex.index = 0
    this
  }

  def wasNull: Boolean = rs.wasNull()

  def str(): String = {
    rs.getString(colIndex.inc())
  }

  def small(): Short = {
    rs.getShort(colIndex.inc())
  }

  def smallObj(): java.lang.Short = {
    val r = rs.getShort(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }

  def smallOpt(): Option[Short] = {
    val r = rs.getShort(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def int(): Int = {
    rs.getInt(colIndex.inc())
  }

  def intObj(): Integer = {
    val r = rs.getInt(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }

  def intOpt(): Option[Int] = {
    val r = rs.getInt(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def long(): Long = {
    rs.getLong(colIndex.inc())
  }

  def longObj(): java.lang.Long = {
    val r = rs.getLong(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }

  def longOpt(): Option[Long] = {
    val r = rs.getLong(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def bool(): Boolean =
    rs.getBoolean(colIndex.inc())

  def boolObj(): java.lang.Boolean = {
    val r = rs.getBoolean(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }


  def boolOpt(): Option[Boolean] = {
    val r = rs.getBoolean(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def single(): Float =
    rs.getFloat(colIndex.inc())

  def singleObj(): java.lang.Float = {
    val r = rs.getFloat(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }

  def singleOpt(): Option[Float] = {
    val r = rs.getFloat(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def double(): Double =
    rs.getDouble(colIndex.inc())

  def doubleObj(): java.lang.Double = {
    val r = rs.getDouble(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }

  def doubleOpt(): Option[Double] = {
    val r = rs.getDouble(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def decimal(): java.math.BigDecimal =
    rs.getBigDecimal(colIndex.inc())

  def localDate(): LocalDate =
    rs.getObject(colIndex.inc(), classOf[LocalDate])

  def localTime(): LocalTime =
    rs.getObject(colIndex.inc(), classOf[LocalTime])

  def offsetDateTime(): OffsetDateTime =
    rs.getObject(colIndex.inc(), classOf[OffsetDateTime])

  def localDateTime(zoneId: ZoneId): LocalDateTime = {
    if (zoneId != null) {
      val odt = offsetDateTime()
      if (odt != null) {
        odt.atZoneSameInstant(zoneId).toLocalDateTime
      } else
        null
    } else
      rs.getObject(colIndex.inc(), classOf[LocalDateTime])
  }

  def localDateTime(): LocalDateTime =
    rs.getObject(colIndex.inc(), classOf[LocalDateTime])

  /**
   * Read OffsetDateTime value and convert to convenient date time format using given `zoneId` if available.
   *
   * @param zoneId zone ID used switch time zone, null if use default timezone
   *
   * @return Convenient date time format string.
   */
  def convenientDateTimeStr(zoneId: ZoneId): String = {
    val odt = offsetDateTime()
    if (odt != null) {
      if (zoneId != null)
        odt.atZoneSameInstant(zoneId).format(DateTimeUtils.CONVENIENT_DATETIME_FORMATTER)
      else
        odt.format(DateTimeUtils.CONVENIENT_DATETIME_FORMATTER)
    } else
      null
  }

  def convenientDateTimeStr(): String = convenientDateTimeStr(null)


  def epochMillis(): EpochMillis = {
    val r = rs.getObject(colIndex.inc(), classOf[Timestamp])
    if (rs.wasNull())
      null
    else
      EpochMillis(r.getTime)
  }

  def epochMillisLong(): Long = {
    val r = rs.getObject(colIndex.inc(), classOf[Timestamp])
    if (rs.wasNull())
      throw new SQLException("Can not convert `NULL` to Epoch milli-seconds.")
    else
      r.getTime
  }

  def epochMillisLongObj(): java.lang.Long = {
    val r = rs.getObject(colIndex.inc(), classOf[Timestamp])
    if (rs.wasNull())
      null
    else
      r.getTime
  }

  def json[T >: AnyRef]()(implicit m: Manifest[T]): T = {
    val s = rs.getString(colIndex.inc())
    if (rs.wasNull())
      null
    else
      ResultSetAccessor.GSON.fromJson(s, m.runtimeClass.asInstanceOf[Class[T]])
  }

  def byteArray(): Array[Byte] = {
    val str = rs.getBinaryStream(colIndex.inc())
    if (rs.wasNull())
      null
    else
      str.readAllBytes()
  }

  def intArray(): Array[Int] = {
    val arr = rs.getArray(colIndex.inc())
    if (rs.wasNull())
      null
    else
      arr.getArray().asInstanceOf[Array[Integer]].map(_.intValue())
  }
}

object ResultSetAccessor {
  def apply(rs: ResultSet): ResultSetAccessor = new ResultSetAccessor(rs)

  def apply(): ResultSetAccessor = new ResultSetAccessor(resultSet = null)


  val GSON: Gson = new GsonBuilder().create()
}

trait ResultSetMapper[T] {
  def map(acc: ResultSetAccessor): T
}

class RowMapperWrapper[T](mapper: ResultSetMapper[T]) extends RowMapper[T] {
  override def mapRow(rs: ResultSet, rowNum: Int): T = {
    val acc = ResultSetAccessor(rs)
    mapper.map(acc)
  }
}

object RowMapperWrapper {

  def apply[T](mapper: ResultSetMapper[T]): RowMapperWrapper[T] = {
    if (mapper != null)
      new RowMapperWrapper[T](mapper)
    else
      null
  }

}
