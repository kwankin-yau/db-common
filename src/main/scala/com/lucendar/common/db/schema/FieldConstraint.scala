package com.lucendar.common.db.schema

import info.gratour.common.error.ErrorWithCode

trait FieldConstraint {

  def checkValue(fieldName: String, conditions: Array[Object]): Unit
}

case class MinLen(minLen: Int) extends FieldConstraint {

  override def checkValue(fieldName: String, fieldValues: Array[Object]): Unit = {
    if (fieldValues.isEmpty)
      throw ErrorWithCode.invalidParam(s"$fieldName(minLen=${minLen})")

    val paramValue = fieldValues(0)
    val value = paramValue.asInstanceOf[String]

    if (value.length < minLen)
      throw ErrorWithCode.invalidParam(s"$fieldName(minLen=${minLen})")
  }
}

case class MaxLen(maxLen: Int) extends FieldConstraint {

  override def checkValue(fieldName: String, fieldValues: Array[Object]): Unit = {
    if (fieldValues.isEmpty)
      return
    val paramValue = fieldValues(0)
    val value = paramValue.asInstanceOf[String]


    if (value.length > maxLen)
      throw ErrorWithCode.invalidParam(s"$fieldName(maxLen<=${maxLen})")
  }
}


case class IntFieldConstraint(minValue: Option[Int], maxValue: Option[Int]) extends FieldConstraint {

  override def checkValue(fieldName: String, fieldValues: Array[Object]): Unit = {
    if (fieldValues.isEmpty)
      throw ErrorWithCode.invalidParam(s"$fieldName(>=${minValue.get})")


    if (minValue.isDefined || maxValue.isDefined) {
      val paramValue = fieldValues(0)
      val value = paramValue.asInstanceOf[Int]

      if (minValue.isDefined)
        if (value < minValue.get)
          throw ErrorWithCode.invalidParam(s"$fieldName(>=${minValue.get})")

      if (maxValue.isDefined)
        if (value > maxValue.get)
          throw ErrorWithCode.invalidParam(s"$fieldName(<=${maxValue.get})")
    }
  }
}

case class DateTimeFieldConstraint(maxDaysSpan: Int) extends FieldConstraint {
  override def checkValue(fieldName: String, fieldValues: Array[Object]): Unit = {
    if (fieldValues.isEmpty)
      throw ErrorWithCode.invalidParam(s"$fieldName")

//    val paramValue = fieldValues.find(pc => pc.predication)
//    val paramValue2 =
//      if (fieldValues.length > 1)
//        fieldValues(1)
//      else
//        null
//
//    paramValue match {
//      case d1: LocalDate =>
//        val d2 =
//          if (paramValue2 == null)
//            LocalDate.now()
//          else
//            paramValue2.asInstanceOf[LocalDate]
//
//        if (d2.minusDays(maxDaysSpan).isAfter(d1))
//          throw ErrorWithCode.invalidParam(fieldName)
//
//      case l1: java.lang.Long =>
//        val instant2: Instant =
//          if (paramValue2 == null)
//            Instant.now()
//          else
//            Instant.ofEpochMilli(paramValue2.asInstanceOf[java.lang.Long])
//
//        val instant1 = Instant.ofEpochMilli(l1)
//        if (instant2.minus(maxDaysSpan, ChronoUnit.DAYS).isAfter(instant1))
//          throw ErrorWithCode.invalidParam(fieldName)
//
//
//      case ldt1: LocalDateTime =>
//        val ldt2 =
//          if (paramValue2 == null)
//            LocalDateTime.now()
//          else
//            paramValue2.asInstanceOf[LocalDateTime]
//
//        if (ldt2.minusDays(maxDaysSpan).isAfter(ldt1))
//          throw ErrorWithCode.invalidParam(fieldName)
//
//      case odt1: OffsetDateTime =>
//        val odt2 =
//          if (paramValue2 == null)
//            OffsetDateTime.now()
//          else
//            paramValue2.asInstanceOf[OffsetDateTime]
//
//        if (odt2.minusDays(maxDaysSpan).isAfter(odt1))
//          throw ErrorWithCode.invalidParam(fieldName)
//
//      case em1: EpochMillis =>
//        val em2 =
//          if (paramValue2 == null)
//            EpochMillis.now()
//          else
//            paramValue2.asInstanceOf[EpochMillis]
//
//        if (em2.toOffsetDateTimeZ.minusDays(maxDaysSpan).isAfter(em1.toOffsetDateTimeZ))
//          throw ErrorWithCode.invalidParam(fieldName)
//
//      case _ =>
//        throw ErrorWithCode.INTERNAL_ERROR
//    }
  }
}

object Constraint {
  def rangeWithin(maxDaysSpan: Int): DateTimeFieldConstraint = DateTimeFieldConstraint(maxDaysSpan)
}
