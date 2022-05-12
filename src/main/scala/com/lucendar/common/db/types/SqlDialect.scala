/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db.types

import java.sql.Connection
import scala.util.Using

trait SqlDialect {
  def id: String

  def stringValueLiteral(s: String): String

  def setConstraintsDeferred(conn: Connection): Unit

  def publicSchemaName: String = "PUBLIC"

  def supportSingleStatementPagination: Boolean = false

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
  final val MSSQL = "mssql"
  final val MYSQL = "mysql"
  final val SQLITE = "sqlite"
}

//object SQLDialect_Pg extends SQLDialect {
//  override def id: String = SQLDialects.POSTGRESQL
//
//  override def stringValueLiteral(s: String): String =
//    org.postgresql.core.Utils.escapeLiteral(null, s, true).toString
//
//  override def tableExists(conn: Connection, tableName: String): Boolean = ???
//
//  override def tableExists(conn: Connection, schemaName: String, tableName: String): Boolean = ???
//}

//object SQLDialect_H2 extends SQLDialect {
//  override def id: String = SQLDialects.H2
//
//  override def stringValueLiteral(s: String): String = {
//    throw new SQLException(s"stringValueLiteral() is not supported in dialect `$id`.")
//  }
//
//  override def tableExists(conn: Connection, tableName: String): Boolean = tableExists(conn, "PUBLIC", tableName)
//
//  override def tableExists(conn: Connection, schemaName: String, tableName: String): Boolean = {
//    val sql = s"SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"
//    Using.resource(conn.prepareStatement(sql)) { st =>
//      st.setString(1, schemaName)
//      st.setString(2, tableName.toUpperCase)
//
//      Using.resource(st.executeQuery()) { rs =>
//        rs.next()
//      }
//    }
//  }
//}
