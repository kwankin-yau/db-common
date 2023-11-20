/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ******************************************************************************/
package com.lucendar.common.db.types

import java.lang
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime}

object Reflections {

  final val JBoolean: Class[lang.Boolean] = classOf[java.lang.Boolean]
  final val JBooleanPrimitive: Class[lang.Boolean] = java.lang.Boolean.TYPE
  final val JByte: Class[lang.Byte] = classOf[java.lang.Byte]
  final val JBytePrimitive: Class[lang.Byte] = java.lang.Byte.TYPE
  final val JShort: Class[lang.Short] = classOf[java.lang.Short]
  final val JShortPrimitive: Class[lang.Short] = java.lang.Short.TYPE
  final val JInteger: Class[Integer] = classOf[java.lang.Integer]
  final val JIntegerPrimitive: Class[Integer] = java.lang.Integer.TYPE
  final val JLong: Class[lang.Long] = classOf[java.lang.Long]
  final val JLongPrimitive: Class[lang.Long] = java.lang.Long.TYPE
  final val JChar: Class[lang.Character] = classOf[java.lang.Character]
  final val JCharPrimitive: Class[lang.Character] = java.lang.Character.TYPE
  final val JString: Class[String] = classOf[java.lang.String]
  final val JFloat: Class[lang.Float] = classOf[java.lang.Float]
  final val JFloatPrimitive: Class[lang.Float] = java.lang.Float.TYPE
  final val JDouble: Class[lang.Double] = classOf[java.lang.Double]
  final val JDoublePrimitive: Class[lang.Double] = java.lang.Double.TYPE
  final val JBigDecimal: Class[java.math.BigDecimal] = classOf[java.math.BigDecimal]
  final val JLocalDate: Class[LocalDate] = classOf[LocalDate]
  final val JLocalTime: Class[LocalTime] = classOf[LocalTime]
  final val JLocalDateTime: Class[LocalDateTime] = classOf[LocalDateTime]
  final val JOffsetDateTime: Class[OffsetDateTime] = classOf[OffsetDateTime]
  final val JByteArray: Class[_] = JavaByteArray.BYTE_ARRAY_CLASS
  final val JIntArray: Class[_] = JavaIntArray.INT_ARRAY_CLASS
  final val JInputStream: Class[java.io.InputStream] = classOf[java.io.InputStream]
}
