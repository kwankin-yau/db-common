/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db.types

import info.gratour.common.error.ErrorWithCode
import org.checkerframework.checker.nullness.qual.{NonNull, Nullable}

import java.sql.Connection
import java.util
import javax.sql.DataSource
import scala.util.Using

/**
 * SQL方言
 */
trait SqlDialect {

  /**
   * 取SQL方言的 ID。如 [[com.lucendar.common.db.types.SqlDialects#POSTGRESQL()]]
   * @return SQL方言的 ID。
   */
  def id: String

  def isPostgreSQL: Boolean = id == SqlDialects.POSTGRESQL
  def isDerby: Boolean = id == SqlDialects.DERBY
  def isSqlite: Boolean = id == SqlDialects.SQLITE
  def isH2: Boolean = id == SqlDialects.H2
  def isOracle: Boolean = id == SqlDialects.ORACLE
  def isMySql: Boolean = id == SqlDialects.MYSQL
  def isSqlServer: Boolean = id == SqlDialects.SQL_SERVER

  def stringValueLiteral(s: String): String

  def setConstraintsDeferred(conn: Connection): Unit

  def publicSchemaName: String = "PUBLIC"

  /**
   * 是否支持单条语句分页 SQL。
   *
   * @return 是否支持单条语句分页 SQL。
   */
  def supportSingleStatementPagination: Boolean = false

  /**
   * 取数据库服务端版本号
   *
   * @param conn 连接对象
   * @return 数据库服务端版本号
   */
  def getServerVer(conn: Connection): ServerVer
  def getServerVer(ds: DataSource): ServerVer =
    Using.resource(ds.getConnection) { conn => getServerVer(conn) }

  /**
   * 查询给定数据表是否存在于数据库中。
   *
   * @param conn 数据库连接
   * @param schemaName Schema 名称。
   * @param tableName 表名
   * @return 给定数据表是否存在于数据库中。
   */
  def tableExists(conn: Connection, schemaName: String, tableName: String): Boolean = {
    val sql =
      """
              SELECT table_name FROM information_schema.tables
              WHERE  UPPER(table_schema) = ?
              AND    UPPER(table_name)   = ?
          """

    Using.resource(conn.prepareStatement(sql)) { st =>
      st.setString(1, schemaName.toUpperCase)
      st.setString(2, tableName.toUpperCase)

      Using.resource(st.executeQuery()) { rs =>
        rs.next()
      }
    }
  }

  def tableExists(conn: Connection, tableName: String): Boolean = tableExists(conn, publicSchemaName, tableName)
}

object SqlDialects {

  final val POSTGRESQL = "postgresql"
  final val H2 = "h2"
  final val ORACLE = "oracle"
  final val SQL_SERVER = "sqlserver"
  final val MYSQL = "mysql"
  final val SQLITE = "sqlite"
  final val DERBY = "derby"

  private final val RegisteredSqlDialects: java.util.Map[String, SqlDialect] = new util.HashMap[String, SqlDialect]()


  def detectIdFromJdbcUrl(@NonNull jdbcUrl: String): String = {
    var p = jdbcUrl
    val idx = p.indexOf('/'.toChar)
    if (idx > 0)
      p = p.substring(0, idx)

    if (p.contains(":postgresql:"))
      POSTGRESQL
    else if (p.contains(":derby:"))
      DERBY
    else if (p.contains(":h2:"))
      H2
    else if (p.contains(":mysql:"))
      MYSQL
    else if (p.contains(":sqlserver:"))
      SQL_SERVER
    else if (p.contains(":oracle:"))
      ORACLE
    else if (p.contains(":sqlite:"))
      SQLITE
    else
      throw ErrorWithCode.invalidParam("jdbcUrl", s"Unrecognized SqlDialect: `$jdbcUrl`.")
  }

  def register(@NonNull sqlDialect: SqlDialect): Unit = {
    RegisteredSqlDialects.synchronized {
      RegisteredSqlDialects.put(sqlDialect.id, sqlDialect)
    }
  }

  @Nullable
  def getRegistered(@NonNull sqlDialectId: String): SqlDialect = {
    RegisteredSqlDialects.synchronized {
      RegisteredSqlDialects.get(sqlDialectId)
    }
  }

  @NonNull
  def registeredSqlDialects(): java.util.List[SqlDialect] = {
    RegisteredSqlDialects.synchronized {
      val r = new util.ArrayList[SqlDialect]()
      r.addAll(RegisteredSqlDialects.values());

      r
    }
  }


}
