/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db.types

import info.gratour.common.types.rest.Reply

import java.util
import scala.reflect.ClassTag

case class QueryResult[T <: Object](dataArray: Array[T], totalRecordCount: Long) {
  def toReply: Reply[T] = {
    new Reply[T](dataArray, totalRecordCount)
  }

  def mapTo[C <: Object](mapper: T => C)(implicit classTag: ClassTag[C]): QueryResult[C] = {
    if (dataArray != null) {
      val arr = new Array[Object](dataArray.length)
      for (i <- dataArray.indices) {
        val e = dataArray(i)
        val c = mapper(e)
        arr(i) = c
      }

      new QueryResult(arr.asInstanceOf[Array[C]], totalRecordCount)
    } else {
      new QueryResult((new Array[Object](0)).asInstanceOf[Array[C]], totalRecordCount)
    }
  }

  def foreach[U](func: T => U): Unit = {
    if (dataArray != null)
      dataArray.foreach(func)
  }

  def toList: java.util.List[T] = {
    val r = new util.ArrayList[T]()
    if (dataArray != null)
      dataArray.foreach(t => r.add(t))
    r
  }

  def isEmpty: Boolean = dataArray == null || dataArray.isEmpty

  def nonEmpty: Boolean = dataArray != null && dataArray.nonEmpty

  def first: Option[T] = if (nonEmpty) Some(dataArray(0)) else None

  def firstOrNull: T = if (nonEmpty) dataArray(0) else null.asInstanceOf[T]

  def toIntArray(arr: java.sql.Array): Array[Int] = {
    if (arr == null)
      null
    else
      arr.getArray().asInstanceOf[Array[Integer]].map(_.intValue())
  }
}

object QueryResult {
  def empty[T <: Object](implicit classTag: ClassTag[T]): QueryResult[T] =
    new QueryResult[T](java.lang.reflect.Array.newInstance(classTag.runtimeClass, 0).asInstanceOf[Array[T]], 0)

  def apply[T <: Object](list: util.Collection[T], totalRecordCount: Long)(implicit classTag: ClassTag[T]): QueryResult[T] = {
    if (totalRecordCount == 0)
    //      new QueryResult[T](Array(), 0)
      new QueryResult[T](java.lang.reflect.Array.newInstance(classTag.runtimeClass, 0).asInstanceOf[Array[T]], 0)
    else {
      val arr = java.lang.reflect.Array.newInstance(classTag.runtimeClass, list.size()).asInstanceOf[Array[T]]
      var idx = 0
      list.forEach { item =>
        arr(idx) = item
        idx += 1
      }

      new QueryResult(arr, totalRecordCount)
    }
  }

  def apply[T <: Object](list: util.Collection[T])(implicit classTag: ClassTag[T]): QueryResult[T] = apply(list, list.size())

  def apply[T <: Object](o: T)(implicit classTag: ClassTag[T]): QueryResult[T] = {
    if (o != null)
      new QueryResult[T](Array(o), 1)
    else
      empty
  }

  def apply[T <: Object](dataArray: Array[T])(implicit classTag: ClassTag[T]): QueryResult[T] =
    new QueryResult[T](dataArray, dataArray.length)
}
