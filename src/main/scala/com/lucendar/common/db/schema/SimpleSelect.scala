package com.lucendar.common.db.schema


import com.lucendar.common.db.rest.SortColumn
import com.lucendar.common.db.types.SqlDialect
import com.lucendar.common.types.rest.Pagination
import info.gratour.common.error.{ErrorWithCode, Errors}

class SimpleSelect(sql: String, val sqlDialect: SqlDialect) {

  private val str = new StringBuilder(sql)
  private val whereClause = new StringBuilder()
  private var groupBy: String = _
  private val orderClause = new StringBuilder()
  private var paginationClause: String = _
  private var limit: Integer = _
  private var offset: Integer = _

  def reset(sql: String): Unit = {
    str.clear()
    str.append(sql)
    whereClause.clear()
    orderClause.clear()
  }

  def where(expr: String): SimpleSelect = {
    if (whereClause.nonEmpty)
      whereClause.append(" AND ")

    whereClause.append(expr)

    this
  }

  def whereNot(expr: String): SimpleSelect = {
    where("(NOT " + expr + ")")
  }

  def whereIsNull(expr: String): SimpleSelect = {
    where(expr + " IS NULL")
  }

  def whereIsNotNull(expr: String): SimpleSelect = {
    where(expr + " IS NOT NULL")
  }

  def someColumnsLike(columns: Array[String]): SimpleSelect = {
    val str = new StringBuilder()
    for (col <- columns) {
      if (str.nonEmpty)
        str.append(" OR ")

      str.append("LOWER(").append(col).append(") LIKE '%' || LOWER(?) || '%'")
    }
    where(str.toString())

    this
  }

  def hasWhereClause: Boolean =
    whereClause.nonEmpty

  def or(expr2: String): SimpleSelect = {
    if (whereClause.isEmpty) {
      where(expr2)
    } else {
      val s = whereClause.toString()
      whereClause.clear()
      whereClause.append("((").append(s).append(")").append(" OR (").append(expr2).append("))")
    }

    this
  }

  def groupBy(clause: String): Unit = {
    this.groupBy = clause
  }

  def hasOrderBy: Boolean =
    orderClause.nonEmpty

  def orderBy(columnName: String, desc: Boolean = false): SimpleSelect = {
    if (orderClause.nonEmpty)
      orderClause.append(", ")

    orderClause.append(columnName)
    if (desc)
      orderClause.append(" DESC ")

    this
  }

  def orderBy(sorting: Array[SortColumn], columnNameMapper: String => String): SimpleSelect = {
    if (sorting != null) {
      sorting.foreach(sc => orderBy(columnNameMapper(sc.columnName), !sc.ascending))
    }

    this
  }

  def paginate(pagination: Pagination): SimpleSelect = {

    if (pagination != null) {
      if (limit != null || offset != null)
        throw new ErrorWithCode(Errors.ILLEGAL_STATE, "pagination and limit/offset can not be both applied.")

      if (sqlDialect != null && sqlDialect.isDerby)
        paginationClause = " OFFSET " + pagination.offset() + " ROWS FETCH FIRST " + pagination.limit + " ROWS ONLY"
      else
        paginationClause = " LIMIT " + pagination.limit + " OFFSET " + pagination.offset()
    }

    this
  }

  def paginate(
                limit: Int,
                page: Int // 1 based
              ): SimpleSelect =
    paginate(new Pagination(limit, page))

  def limit(limit: Int): SimpleSelect = {
    if (limit < 1)
      throw ErrorWithCode.invalidParam("limit")

    if (paginationClause != null)
      throw new ErrorWithCode(Errors.ILLEGAL_STATE, "pagination and limit can not be both applied.")

    this.limit = limit

    this
  }

  def offset(offset: Int): SimpleSelect = {
    if (offset < 0)
      throw ErrorWithCode.invalidParam("offset")

    if (paginationClause != null)
      throw new ErrorWithCode(Errors.ILLEGAL_STATE, "pagination and offset can not be both applied.")

    this.offset = offset;

    this
  }

  override def toString: String = toSql

  def toSql: String =
    toSql(addOrderByClause = true, addPaginationClause = true)

  def toSql(addOrderByClause: Boolean, addPaginationClause: Boolean): String = {
    val r = new StringBuilder
    r.append(str)
    if (whereClause.nonEmpty)
      r.append(" WHERE ").append(whereClause)

    if (groupBy != null && groupBy.nonEmpty)
      r.append(" GROUP BY " + groupBy)

    if (addOrderByClause && orderClause.nonEmpty)
      r.append(" ORDER BY ").append(orderClause)

    if (limit != null || offset != null) {
      if (sqlDialect != null && sqlDialect.isDerby) {
        if (offset != null)
          r.append(" OFFSET ").append(offset).append(" ROWS")

        if (limit != null)
          r.append(" FETCH FIRST ").append(limit).append(" ROWS ONLY")
      } else {
        if (limit != null)
          r.append(" LIMIT " + limit)
        if (offset != null)
          r.append(" OFFSET " + offset)
      }

    } else if (addPaginationClause && (paginationClause != null))
      r.append(paginationClause)

    r.toString()
  }

  def toPgSqlWithTotalRowCount: String = {
    if (paginationClause == null)
      return toSql


    val main = str.toString()
    val where = if (whereClause.isEmpty) "" else " WHERE " + whereClause.toString()
    val grpBy = if (groupBy != null && groupBy.nonEmpty) "GROUP BY " + groupBy else ""

    val orderBy = if (orderClause.isEmpty) "" else " ORDER BY " + orderClause.toString()
    val page =
      if (limit != null || offset != null) {
        if (limit != null) {
          if (offset != null)
            " LIMIT " + limit + " OFFSET " + offset
          else
            " LIMIT " + limit
        } else {
          " OFFSET " + offset
        }
      }
      else if (paginationClause == null) ""
      else paginationClause

    s"""WITH cte AS (
          $main
          $where
          $grpBy
          $orderBy
        )
        SELECT * FROM (
          TABLE cte
          $orderBy
          $page
        ) sub
        RIGHT JOIN (SELECT count(1) FROM cte) c(__rc__) ON true
        """
  }

}


object SimpleSelect {
  def apply(sql: String, sqlDialect: SqlDialect): SimpleSelect = {
    new SimpleSelect(sql, sqlDialect)
  }


  def mapColumnName(columnName: String): String = {
    val r = new StringBuilder()

    for (c <- columnName) {
      if (c >= 'A' && c <= 'Z') {
        r.append('_').append(c.toLower)
      } else
        r.append(c)
    }

    r.toString
  }
}
