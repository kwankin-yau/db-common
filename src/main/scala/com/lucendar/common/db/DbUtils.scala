/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db

object DbUtils {

  def valuesToSqlPlaceHolders(values: Array[_]): String =
    if (values != null && !values.isEmpty)
      values.map(_ => "?").mkString(",")
    else
      null

  /**
   *
   * @param tableName
   * @param keyColumns
   * @param otherColumns
   * @param rows
   * @param r
   * @return all column names
   */
  def batchInsertSql(tableName   : String,
                     keyColumns  : Array[String],
                     otherColumns: Array[String],
                     rows        : Int,
                     r           : java.lang.StringBuilder
                    ): Array[String] = {
    val columns = keyColumns ++ otherColumns
    val params = columns.mkString("(", ",", ")")

    r.append("INSERT INTO ").append(tableName).append("(");
    var i = 0
    keyColumns.foreach(k => {
      if (i > 0)
        r.append(','.toChar)
      i += 1

      r.append(k)
    })
    r.append(")VALUES")
    for (i <- 0 until rows) {
      if (i > 0)
        r.append(','.toChar)

      r.append(params)
    }

    columns
  }

  def splitColumns(columnsSeparatedByComma: String): Array[String] =
    columnsSeparatedByComma.split(",").map(_.trim)

  def batchInsertOnConflictNoActionSql(tableName: String, keys: Array[String], others: Array[String], rows: Int): String = {
    val r = new java.lang.StringBuilder()
    batchInsertSql(tableName, keys, others, rows, r)
    r.append("ON CONFLICT (").append(keys.mkString(",")).append(") DO NOTHING")

    r.toString
  }

  def batchInsertOnConflictNoActionSql(tableName: String, keyColumns: String, otherColumns: String, rows: Int): String = {
    val keys = splitColumns(keyColumns)
    val others = splitColumns(otherColumns)
    batchInsertOnConflictNoActionSql(tableName, keys, others, rows)
  }

  def upsertSql(tableName: String, keys: Array[String], others: Array[String], rows: Int): String = {
    val r = new java.lang.StringBuilder()
    batchInsertSql(tableName, keys, others, rows, r)
    r.append("ON CONFLICT ").append(keys.mkString(",")).append(") DO UPDATE SET ")
    for (i <- 0 until others.length) {
      val col = others(i)
      if (i > 0)
        r.append(','.toChar)

      r.append(col).append("=EXCLUDED.").append(col)
    }

    r.toString
  }

  def upsertSql(tableName: String, keyColumns: String, otherColumns: String, rows: Int): String =
    upsertSql(tableName, splitColumns(keyColumns), splitColumns(otherColumns), rows)


}
