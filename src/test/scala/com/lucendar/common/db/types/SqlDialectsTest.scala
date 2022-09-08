/** *****************************************************************************
 * Copyright (c) 2019, 2022 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db.types

import org.junit.jupiter.api.Test

class SqlDialectsTest {

  @Test
  def testDetectIdFromJdbcUrl(): Unit = {
    val pgJdbcUrl = "jdbc:postgresql://postgresql.db.server:5430/my_database?ssl=true&loglevel=2"
    assert(SqlDialects.detectIdFromJdbcUrl(pgJdbcUrl) == SqlDialects.POSTGRESQL)

    val h2JdbcUrl = "jdbc:h2:~/test"
    assert(SqlDialects.detectIdFromJdbcUrl(h2JdbcUrl) == SqlDialects.H2)

    val oracleJdbcUrl = "jdbc:oracle:thin:@//myoracle.db.server:1521/my_servicename"
    assert(SqlDialects.detectIdFromJdbcUrl(oracleJdbcUrl) == SqlDialects.ORACLE)

    val sqlServerJdbcUrl = "jdbc:sqlserver://mssql.db.server\\mssql_instance;databaseName=my_database"
    assert(SqlDialects.detectIdFromJdbcUrl(sqlServerJdbcUrl) == SqlDialects.SQL_SERVER)

    val mySqlJdbcUrl = "jdbc:mysql://mysql.db.server:3306/my_database?useSSL=false&serverTimezone=UTC"
    assert(SqlDialects.detectIdFromJdbcUrl(mySqlJdbcUrl) == SqlDialects.MYSQL)

    val sqliteJdbcUrl = "jdbc:sqlite:sample.db"
    assert(SqlDialects.detectIdFromJdbcUrl(sqliteJdbcUrl) == SqlDialects.SQLITE)
  }
}
