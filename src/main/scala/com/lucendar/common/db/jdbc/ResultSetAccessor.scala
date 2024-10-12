package com.lucendar.common.db.jdbc


import com.google.gson.{Gson, GsonBuilder}
import com.lucendar.common.utils.DateTimeUtils
import com.lucendar.common.utils.DateTimeUtils.BeijingConv
import org.springframework.jdbc.core.RowMapper

import java.sql.{Date, ResultSet, SQLException, Timestamp}
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, ZoneId}

class ResultSetAccessor(val resultSet: ResultSet) {

  var rs: ResultSet = resultSet

  var colIndex: Int = 0

  def next(): Boolean = {
    val r = rs.next()
    if (r)
      reset()

    r
  }

  def reset(): Unit = colIndex = 0

  def reset(resultSet: ResultSet): ResultSetAccessor = {
    rs = resultSet
    colIndex = 0
    this
  }

  def wasNull: Boolean = rs.wasNull()

  def str(): String = {
    colIndex += 1
    rs.getString(colIndex)
  }

  def small(): Short = {
    colIndex += 1
    rs.getShort(colIndex)
  }

  def smallObj(): java.lang.Short = {
    colIndex += 1
    val r = rs.getShort(colIndex)
    if (rs.wasNull())
      null
    else
      r
  }

  def smallOpt(): Option[Short] = {
    colIndex += 1
    val r = rs.getShort(colIndex)
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  /**
   *
   * @return
   * @deprecated use int32() instead due to int() method is not direct visible to java source code.
   */
  @Deprecated
  def int(): Int = {
    colIndex += 1
    rs.getInt(colIndex)
  }

  def int32(): Int = {
    colIndex += 1
    rs.getInt(colIndex)
  }

  def intObj(): Integer = {
    colIndex += 1
    val r = rs.getInt(colIndex)
    if (rs.wasNull())
      null
    else
      r
  }

  def intOpt(): Option[Int] = {
    colIndex += 1
    val r = rs.getInt(colIndex)
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  /**
   *
   * @return
   * @deprecated use bigInt() instead due to long() method is not direct visible to java source code.
   */
  @Deprecated
  def long(): Long = {
    colIndex += 1
    rs.getLong(colIndex)
  }
  def bigInt(): Long = {
    colIndex += 1
    rs.getLong(colIndex)
  }

  def bigIntObj(): java.lang.Long = {
    colIndex += 1
    val r = rs.getLong(colIndex)
    if (rs.wasNull())
      null
    else
      r
  }


  def longObj(): java.lang.Long = {
    colIndex += 1
    val r = rs.getLong(colIndex)
    if (rs.wasNull())
      null
    else
      r
  }

  def longOpt(): Option[Long] = {
    colIndex += 1
    val r = rs.getLong(colIndex)
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def bool(): Boolean = {
    colIndex += 1
    rs.getBoolean(colIndex)
  }

  def boolObj(): java.lang.Boolean = {
    colIndex += 1
    val r = rs.getBoolean(colIndex)
    if (rs.wasNull())
      null
    else
      r
  }


  def boolOpt(): Option[Boolean] = {
    colIndex += 1
    val r = rs.getBoolean(colIndex)
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def single(): Float = {
    colIndex += 1
    rs.getFloat(colIndex)
  }

  def singleObj(): java.lang.Float = {
    colIndex += 1
    val r = rs.getFloat(colIndex)
    if (rs.wasNull())
      null
    else
      r
  }

  def singleOpt(): Option[Float] = {
    colIndex += 1
    val r = rs.getFloat(colIndex)
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def double(): Double = {
    colIndex += 1
    rs.getDouble(colIndex)
  }

  def doubleObj(): java.lang.Double = {
    colIndex += 1
    val r = rs.getDouble(colIndex)
    if (rs.wasNull())
      null
    else
      r
  }

  def doubleOpt(): Option[Double] = {
    colIndex += 1
    val r = rs.getDouble(colIndex)
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def decimal(): java.math.BigDecimal = {
    colIndex += 1
    rs.getBigDecimal(colIndex)
  }
  def date(): Date = {
    colIndex += 1
    rs.getDate(colIndex)
  }

  def timestamp(): Timestamp = {
    colIndex += 1
    rs.getTimestamp(colIndex)
  }

  def localDate(): LocalDate = {
    colIndex += 1
    rs.getObject(colIndex, classOf[LocalDate])
  }

  def localTime(): LocalTime = {
    colIndex += 1
    rs.getObject(colIndex, classOf[LocalTime])
  }

  def offsetDateTime(): OffsetDateTime = {
    colIndex += 1
    rs.getObject(colIndex, classOf[OffsetDateTime])
  }

  def localDateTime(zoneId: ZoneId): LocalDateTime = {
    if (zoneId != null) {
      val odt = offsetDateTime()
      if (odt != null) {
        odt.atZoneSameInstant(zoneId).toLocalDateTime
      } else
        null
    } else {
      colIndex += 1
      rs.getObject(colIndex, classOf[LocalDateTime])
    }
  }

  def localDateTime(): LocalDateTime = {
    colIndex += 1
    rs.getObject(colIndex, classOf[LocalDateTime])
  }

  /**
   * Get current OffsetDateTime value and convert to convenient date time format using given `zoneId` if available.
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

  /**
   * Get current OffsetDateTime value and convert to convenient date time format using default `zoneId`.
   *
   * @return Convenient date time format string.
   * @deprecated Use com.lucendar.common.db.jdbc.ResultSetAccessor#beijingConvDateTimeStr() instead
   */
  @Deprecated
  def convenientDateTimeStr(): String = convenientDateTimeStr(null)

  def beijingConvDateTimeStr(): String = BeijingConv.odtToStr(offsetDateTime());

  def beijingConvTimestampStr(): String = {
    val ts = timestamp()
    if (ts != null)
      BeijingConv.millisToString(ts.getTime)
    else
      null
  }


  def epochMillisLong(): Long = {
    colIndex += 1
    val r = rs.getObject(colIndex, classOf[Timestamp])
    if (rs.wasNull())
      throw new SQLException("Can not convert `NULL` to Epoch milli-seconds.")
    else
      r.getTime
  }

  def epochMillisLongObj(): java.lang.Long = {
    colIndex += 1
    val r = rs.getObject(colIndex, classOf[Timestamp])
    if (rs.wasNull())
      null
    else
      r.getTime
  }

  def json[T >: AnyRef]()(implicit m: Manifest[T]): T = {
    colIndex += 1
    val s = rs.getString(colIndex)
    if (rs.wasNull())
      null
    else
      ResultSetAccessor.GSON.fromJson(s, m.runtimeClass.asInstanceOf[Class[T]])
  }

  def byteArray(): Array[Byte] = {
    colIndex += 1
    val str = rs.getBinaryStream(colIndex)
    if (rs.wasNull())
      null
    else
      str.readAllBytes()
  }

  def intArray(): Array[Int] = {
    colIndex += 1
    val arr = rs.getArray(colIndex)
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
