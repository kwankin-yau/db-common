/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db.jdbc

import com.lucendar.common.db.types.Types.MultiBinding
import com.lucendar.common.serv.utils.ServUtils
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.springframework.core.io.ResourceLoader
import org.springframework.jdbc.core.{JdbcTemplate, PreparedStatementSetter, RowMapper}
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.TransactionManager
import org.springframework.transaction.support.TransactionTemplate

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util
import java.util.Properties
import javax.sql.DataSource
import scala.collection.mutable.ArrayBuffer
import scala.util.Using

object DbHelper {

  /**
   * Create a HikariDataSource from given arguments.
   *
   * @param jdbcUrl
   * @param username                      database username, optional
   * @param password                      database password, optional
   * @param maxPoolSize                   maximum pool size, 0 for not specified
   * @param leakDetectionThresholdSeconds time in seconds that it is a period of connection which does not return to pool will make pool start a leak detection, 0 for not specified
   * @return data source
   */
  def createDataSource(
                        jdbcUrl: String,
                        username: String,
                        password: String,
                        maxPoolSize: Int,
                        leakDetectionThresholdSeconds: Int): DataSource = {
    val hc = new HikariConfig()
    hc.setJdbcUrl(jdbcUrl)

    if (username != null)
      hc.setUsername(username)

    if (password != null)
      hc.setPassword(password)

    if (maxPoolSize > 0)
      hc.setMaximumPoolSize(maxPoolSize)

    if (leakDetectionThresholdSeconds > 0)
      hc.setLeakDetectionThreshold(leakDetectionThresholdSeconds * 1000)

    new HikariDataSource(hc)
  }

  def tryLoadHikariConfig(resourceLoader: ResourceLoader, configPropertiesFile: String): HikariConfig = {
    var hc: HikariConfig = null

    val rs = ServUtils.tryLoadResource(configPropertiesFile, resourceLoader)
    if (rs != null) {
      try {
        val props = new Properties()
        props.load(rs)

        hc = new HikariConfig(props)
      } finally {
        rs.close()
      }
    }

    if (hc == null)
      hc = new HikariConfig()

    hc
  }

  case class JdbcContext(txMgr: DataSourceTransactionManager, jdbcTemplate: JdbcTemplate)

  def createJdbcContext(ds: DataSource): JdbcContext = {
    val txMgr = new DataSourceTransactionManager(ds)
    val jdbcTemplate = new JdbcTemplate(txMgr.getDataSource)

    JdbcContext(txMgr, jdbcTemplate)
  }


  def existsQry(sql: String, pss: PreparedStatementSetter)(implicit conn: Connection): Boolean = {
    if (pss != null) {
      Using.resource(conn.prepareStatement(sql)) { st =>
        pss.setValues(st)

        Using.resource(st.executeQuery()) { rs =>
          rs.next()
        }
      }
    } else {
      Using.resource(conn.createStatement()) { st =>
        Using.resource(st.executeQuery(sql)) { rs =>
          rs.next()
        }
      }
    }
  }

  def existsQryEx(sql: String, setter: StatementSetter)(implicit conn: Connection): Boolean =
    existsQry(sql, StatementSetterWrapper(setter))

  def existsQry(sql: String)(implicit conn: Connection): Boolean = existsQry(sql, null)

  def qryValue[T](sql: String, pss: PreparedStatementSetter, reader: RowMapper[T])(implicit conn: Connection): Option[T] = {
    if (pss != null) {
      Using.resource(conn.prepareStatement(sql)) { st =>
        pss.setValues(st)

        Using.resource(st.executeQuery()) { rs =>
          if (rs.next())
            Some(reader.mapRow(rs, 0))
          else
            None
        }
      }
    } else {
      Using.resource(conn.createStatement()) { st =>
        Using.resource(st.executeQuery(sql)) { rs =>
          if (rs.next())
            Some(reader.mapRow(rs, 0))
          else
            None
        }
      }
    }
  }

  def qryValueEx[T](sql: String, setter: StatementSetter, mapper: ResultSetMapper[T])(implicit conn: Connection): Option[T] =
    qryValue(sql, StatementSetterWrapper(setter), RowMapperWrapper(mapper))

  def qryLongValue(sql: String, pss: PreparedStatementSetter = null)(implicit conn: Connection): Option[Long] =
    qryValue(sql, pss, (rs, _) => rs.getLong(1))

  def qryLongValueEx(sql: String, setter: StatementSetter = null)(implicit conn: Connection): Option[Long] =
    qryValueEx(sql, setter, rs => rs.long())

  def qryStringValue(sql: String, pss: PreparedStatementSetter = null)(implicit conn: Connection): Option[String] =
    qryValue(sql, pss, (rs, _) => rs.getString(1))

  def qryStringValueEx(sql: String, setter: StatementSetter = null)(implicit conn: Connection): Option[String] =
    qryValueEx(sql, setter, rs => rs.str())

  def qryBooleanValue(sql: String, pss: PreparedStatementSetter = null)(implicit conn: Connection): Option[Boolean] =
    qryValue(sql, pss, (rs, _) => rs.getBoolean(1))

  def qryBooleanValueEx(sql: String, setter: StatementSetter = null)(implicit conn: Connection): Option[Boolean] =
    qryValueEx(sql, setter, rs => rs.bool())

  def qryObject[T >: Null](sql: String, pss: PreparedStatementSetter, rowMapper: RowMapper[T])(implicit conn: Connection): T =
    qryValue(sql, pss, (rs, _) => rowMapper.mapRow(rs, 0)).orNull

  def qryObjectEx[T >: Null](sql: String, setter: StatementSetter, mapper: ResultSetMapper[T])(implicit conn: Connection): T =
    qryValueEx(sql, setter, acc => {
      mapper.map(acc)
    }).orNull

  def qryList[T >: Null](sql: String, pss: PreparedStatementSetter, rowMapper: RowMapper[T])(implicit conn: Connection): java.util.List[T] = {
    val list = new util.ArrayList[T]()

    def loadFromResultSet(rs: ResultSet): Unit = {
      var i = 0
      try {
        while (rs.next()) {
          val r = rowMapper.mapRow(rs, i)
          i += 1
          list.add(r)
        }
      } finally {
        rs.close()
      }
    }

    if (pss != null) {
      val st = conn.prepareStatement(sql)
      try {
        pss.setValues(st)

        val rs = st.executeQuery()
        loadFromResultSet(rs)
      } finally {
        st.close()
      }
    } else {
      val st = conn.createStatement()
      try {
        val rs = st.executeQuery(sql)
        loadFromResultSet(rs)
      } finally {
        st.close()
      }
    }

    list
  }

  def qryList[T >: Null](sql: String, rowMapper: RowMapper[T])(implicit conn: Connection): java.util.List[T] = qryList(sql, null, rowMapper)

  def qryListEx[T >: Null](sql: String, setter: StatementSetter, mapper: ResultSetMapper[T])(implicit conn: Connection): java.util.List[T] = {
    val pss = StatementSetterWrapper(setter)

    val rowMapper = new RowMapper[T] {
      override def mapRow(rs: ResultSet, rowNum: Int): T = {
        val acc = ResultSetAccessor(rs)
        mapper.map(acc)
      }
    }

    qryList(sql, pss, rowMapper)
  }

  def qryListEx[T >: Null](sql: String, mapper: ResultSetMapper[T])(implicit conn: Connection): java.util.List[T] =
    qryListEx(sql, null, mapper)

  def update(sql: String, pss: PreparedStatementSetter = null)(implicit conn: Connection): Int = {
    if (pss != null) {
      val st = conn.prepareStatement(sql)
      try {
        pss.setValues(st)

        st.executeUpdate()
      } finally {
        st.close()
      }
    } else {
      val st = conn.createStatement()
      try {
        st.executeUpdate(sql)
      } finally {
        st.close()
      }
    }
  }

  def updateEx(sql: String, setter: StatementSetter = null)(implicit conn: Connection): Int =
    update(sql, StatementSetterWrapper(setter))

  def execute(sql: String, pss: PreparedStatementSetter = null)(implicit conn: Connection): Unit = {
    if (pss != null) {
      Using.resource(conn.prepareStatement(sql)) { st =>
        pss.setValues(st)

        st.execute()
      }
    } else {
      Using.resource(conn.createStatement()) { st =>
        st.execute(sql)
      }
    }
  }

  def executeEx(sql: String, setter: StatementSetter = null)(implicit conn: Connection): Unit =
    execute(sql, StatementSetterWrapper(setter))

  def batchUpdate[T >: Null](sql: String, argumentEntities: util.Collection[T], binding: MultiBinding[T], batchSize: Int = 200)(implicit conn: Connection): Array[Int] = {
    val r: ArrayBuffer[Int] = ArrayBuffer()

    val st = conn.prepareStatement(sql)
    try {
      var cnt = 0

      val binder = StatementBinder(st)


      argumentEntities.forEach(a => {
        st.clearParameters()
        binding.apply(a, binder.restart())
        st.addBatch()

        cnt += 1
        if (cnt == batchSize) {
          val updated = st.executeBatch()
          r ++= updated

          cnt = 0
        }
      })

      if (cnt > 0)
        r ++= st.executeBatch()
    } finally {
      st.close()
    }

    r.toArray
  }

  def batchExec[T >: Null](sql: String, argumentEntities: util.Collection[T], binding: MultiBinding[T], batchSize: Int = 200)(implicit conn: Connection): Unit = {
    val st = conn.prepareStatement(sql)
    try {
      var cnt = 0

      val binder = StatementBinder(st)


      argumentEntities.forEach(a => {
        st.clearParameters()
        binding.apply(a, binder.restart())
        st.addBatch()

        cnt += 1
        if (cnt == batchSize) {
          st.executeBatch()
          cnt = 0
        }
      })

      if (cnt > 0)
        st.executeBatch()
    } finally {
      st.close()
    }

  }

  def call(sql: String, pss: PreparedStatementSetter = null)(implicit conn: Connection): Unit = {
    Using.resource(conn.prepareCall(sql)) { st =>
      pss.setValues(st)

      st.execute()
    }
  }

  def callEx(sql: String, setter: StatementSetter = null)(implicit conn: Connection): Unit =
    call(sql, StatementSetterWrapper(setter))

}
