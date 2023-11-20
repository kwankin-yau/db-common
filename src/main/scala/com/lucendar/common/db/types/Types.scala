/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db.types

import com.lucendar.common.db.jdbc.StatementBinder

import java.sql.PreparedStatement
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime}
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeTag

object Types {

  trait MultiBinding[T] {
    def bind(entry: T, binder: StatementBinder): Unit
  }

  trait MultiSetting[T] {
    def setParams(entry: T, ps: PreparedStatement): Unit
  }

  final val BoolType: universe.Type = typeTag[Boolean].tpe
  final val ByteType: universe.Type = typeTag[Byte].tpe
  final val ShortType: universe.Type = typeTag[Short].tpe
  final val IntType: universe.Type = typeTag[Int].tpe
  final val LongType: universe.Type = typeTag[Long].tpe
  final val CharType: universe.Type = typeTag[Char].tpe
  final val StringType: universe.Type = typeTag[String].tpe
  final val FloatType: universe.Type = typeTag[Float].tpe
  final val DoubleType: universe.Type = typeTag[Double].tpe

  final val JBooleanType: universe.Type = typeTag[java.lang.Boolean].tpe
  final val JByteType: universe.Type = typeTag[java.lang.Byte].tpe
  final val JShortType: universe.Type = typeTag[java.lang.Short].tpe
  final val JIntegerType: universe.Type = typeTag[java.lang.Integer].tpe
  final val JLongType: universe.Type = typeTag[java.lang.Long].tpe
  final val JCharacterType: universe.Type = typeTag[java.lang.Character].tpe


  final val JFloatType: universe.Type = typeTag[java.lang.Float].tpe
  final val JDoubleType: universe.Type = typeTag[java.lang.Double].tpe
  final val JBigDecimalType: universe.Type = typeTag[java.math.BigDecimal].tpe
  final val LocalDateType: universe.Type = typeTag[LocalDate].tpe
  final val LocalTimeType: universe.Type = typeTag[LocalTime].tpe
  final val LocalDateTimeType: universe.Type = typeTag[LocalDateTime].tpe
  final val OffsetDateTimeType: universe.Type = typeTag[OffsetDateTime].tpe

  final val InputStreamType: universe.Type = typeTag[java.io.InputStream].tpe

}
