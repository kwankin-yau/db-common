/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db.jdbc

import com.lucendar.common.db.jdbc.DbHelper.JdbcContext
import com.lucendar.common.db.types.SqlDialect
import com.lucendar.common.db.types.Types.{MultiBinding, MultiSetting}
import org.springframework.jdbc.core.{ConnectionCallback, JdbcTemplate, PreparedStatementSetter, RowMapper}
import org.springframework.jdbc.datasource.DataSourceTransactionManager

import java.sql.Connection
import java.util
import scala.util.Using

trait DbSupport {

  def sqlDialect: SqlDialect

  def jdbcCtx: JdbcContext

  implicit protected def jdbc: JdbcTemplate = jdbcCtx.jdbcTemplate

  protected def txMgr: DataSourceTransactionManager = jdbcCtx.txMgr

  protected def defaultSchemaName: String = sqlDialect.publicSchemaName

  protected def tx[T](op: () => T): T = {
    val ts = txMgr.getTransaction(null)
    try {
      val r = op.apply()
      txMgr.commit(ts)
      r
    } catch {
      case t: Throwable =>
        txMgr.rollback(ts)
        throw t
    }
  }

  protected def inTransRequired[T](op: () => T): T = tx(op)

  protected def existsQry(sql: String, pss: PreparedStatementSetter): Boolean = jdbc.execute(new ConnectionCallback[Boolean] {
    override def doInConnection(con: Connection): Boolean = DbHelper.existsQry(sql, pss)(con)
  })

  protected def existsQry(sql: String): Boolean = existsQry(sql, null)

  protected def existsQryEx(sql: String, setter: StatementSetter): Boolean = existsQry(sql, StatementSetterWrapper(setter))


  protected def tableExists(tableName: String): Boolean = jdbc.execute(new ConnectionCallback[Boolean] {
    override def doInConnection(con: Connection): Boolean = sqlDialect.tableExists(con, defaultSchemaName, tableName)
  })

  protected def qryValue[T](sql: String, pss: PreparedStatementSetter, reader: RowMapper[T]): Option[T] = jdbc.execute(new ConnectionCallback[Option[T]] {
    override def doInConnection(con: Connection): Option[T] = DbHelper.qryValue(sql, pss, reader)(con)
  })

  protected def qryValueEx[T](sql: String, setter: StatementSetter, mapper: ResultSetMapper[T]): Option[T] =
    qryValue(sql, StatementSetterWrapper(setter), RowMapperWrapper(mapper))

  protected def qryIntValue(sql: String, pss: PreparedStatementSetter = null): Option[Int] =
    jdbc.execute(new ConnectionCallback[Option[Int]] {
      override def doInConnection(con: Connection): Option[Int] = DbHelper.qryIntValue(sql, pss)(con)
    })

  protected def qryIntValueEx(sql: String, setter: StatementSetter = null): Option[Int] =
    qryIntValue(sql, StatementSetterWrapper(setter))

  protected def qryLongValue(sql: String, pss: PreparedStatementSetter = null): Option[Long] = jdbc.execute(new ConnectionCallback[Option[Long]] {
    override def doInConnection(con: Connection): Option[Long] = DbHelper.qryLongValue(sql, pss)(con)
  })

  protected def qryLongValueEx(sql: String, setter: StatementSetter = null): Option[Long] =
    qryLongValue(sql, StatementSetterWrapper(setter))

  protected def qryStringValue(sql: String, pss: PreparedStatementSetter = null): Option[String] = jdbc.execute(new ConnectionCallback[Option[String]] {
    override def doInConnection(con: Connection): Option[String] = DbHelper.qryStringValue(sql, pss)(con)
  })

  protected def qryStringValueEx(sql: String, setter: StatementSetter = null): Option[String] =
    qryStringValue(sql, StatementSetterWrapper(setter))

  protected def qryBooleanValue(sql: String, pss: PreparedStatementSetter = null): Option[Boolean] = jdbc.execute(new ConnectionCallback[Option[Boolean]] {
    override def doInConnection(con: Connection): Option[Boolean] = DbHelper.qryBooleanValue(sql, pss)(con)
  })

  protected def qryBooleanValueEx(sql: String, setter: StatementSetter = null): Option[Boolean] =
    qryBooleanValue(sql, StatementSetterWrapper(setter))

  protected def qryObject[T >: Null](sql: String, pss: PreparedStatementSetter, rowMapper: RowMapper[T]): T = jdbc.execute(new ConnectionCallback[T] {
    override def doInConnection(con: Connection): T = DbHelper.qryObject(sql, pss, rowMapper)(con)
  })

  protected def qryObjectEx[T >: Null](sql: String, setter: StatementSetter, mapper: ResultSetMapper[T]): T =
    qryObject(sql, StatementSetterWrapper(setter), RowMapperWrapper(mapper))

  protected def qryObjectMix[T >: Null](sql: String, setter: StatementSetter, mapper: RowMapper[T]): T =
    qryObject(sql, StatementSetterWrapper(setter), mapper)


  def qryList[T >: Null](sql: String, pss: PreparedStatementSetter, rowMapper: RowMapper[T]): java.util.List[T] = jdbc.execute(new ConnectionCallback[util.List[T]] {
    override def doInConnection(con: Connection): util.List[T] = DbHelper.qryList(sql, pss, rowMapper)(con)
  })

  def qryListEx[T >: Null](sql: String, setter: StatementSetter, mapper: ResultSetMapper[T]): java.util.List[T] =
    qryList(sql, StatementSetterWrapper(setter), RowMapperWrapper(mapper))

  def qryListMix[T >: Null](sql: String, setter: StatementSetter, mapper: RowMapper[T]): java.util.List[T] =
    qryList(sql, StatementSetterWrapper(setter), mapper)



  protected def update(sql: String, pss: PreparedStatementSetter = null): Int = jdbc.execute(new ConnectionCallback[Integer] {
    override def doInConnection(con: Connection): Integer = {
      DbHelper.update(sql, pss)(con)
    }
  })

  protected def updateEx(sql: String, setter: StatementSetter = null): Int =
    update(sql, StatementSetterWrapper(setter))

  protected def batchUpdate[T >: Null](sql: String, argumentEntities: util.Collection[T], binding: MultiBinding[T], batchSize: Int = 200): Array[Int] = {
    inTransRequired(() => {
      jdbc.execute(new ConnectionCallback[Array[Int]] {
        override def doInConnection(con: Connection): Array[Int] = DbHelper.batchUpdate(sql, argumentEntities, binding, batchSize)(con)
      })
    })
  }

  protected def batchUpdateJdbc[T >: Null](sql: String, argumentEntities: util.Collection[T], binding: MultiSetting[T], batchSize: Int = 200): Array[Int] = {
    inTransRequired(() => {
      jdbc.execute(new ConnectionCallback[Array[Int]] {
        override def doInConnection(con: Connection): Array[Int] = DbHelper.batchUpdateJdbc(sql, argumentEntities, binding, batchSize)(con)
      })
    })
  }

  protected def batchExec[T >: Null](sql: String, argumentEntities: util.Collection[T], binding: MultiBinding[T], batchSize: Int = 200): Unit = {
    inTransRequired(() => {
      jdbc.execute(new ConnectionCallback[Void] {
        override def doInConnection(con: Connection): Void = {
          DbHelper.batchExec(sql, argumentEntities, binding, batchSize)(con)
          null
        }
      })
    })
  }

  protected def call(sql: String, pss: PreparedStatementSetter = null): Unit = jdbc.execute(new ConnectionCallback[Void] {
    override def doInConnection(con: Connection): Void = {
      DbHelper.call(sql, pss)(con)
      null
    }
  })

  protected def callEx(sql: String, setter: StatementSetter = null): Unit =
    call(sql, StatementSetterWrapper(setter))

}
