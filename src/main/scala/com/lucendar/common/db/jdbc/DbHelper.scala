/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db.jdbc

import com.lucendar.common.db.types.SqlDialect
import com.lucendar.common.db.types.Types.{MultiBinding, MultiSetting}
import com.lucendar.common.serv.utils.ServUtils
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.micrometer.common.lang.Nullable
import org.springframework.core.io.ResourceLoader
import org.springframework.jdbc.core.{ConnectionCallback, JdbcTemplate, PreparedStatementSetter, RowMapper}
import org.springframework.jdbc.datasource.DataSourceTransactionManager

import java.io.Closeable
import java.sql.{Connection, PreparedStatement, ResultSet, Statement, Timestamp}
import java.time.{OffsetDateTime, ZoneId}
import java.{lang, util}
import java.util.Properties
import javax.sql.DataSource
import scala.collection.mutable.ArrayBuffer
import scala.util.Using


object DbHelper {

  final val DEFAULT_MAX_POOL_SIZE = 20
  final val DEFAULT_LEAK_DETECTION_THRESHOLD_SECONDS = 20 * 60

  /**
   * Create a HikariDataSource from given arguments.
   *
   * @param jdbcUrl                       jdbc url for data source
   * @param username                      database username, optional
   * @param password                      database password, optional
   * @param maxPoolSize                   maximum pool size, 0 for not specified
   * @param leakDetectionThresholdSeconds time in seconds that it is a period of connection which does not return to pool will make pool start a leak detection, 0 for not specified
   * @return data source
   */
  def createDataSource(
                        jdbcUrl                      : String,
                        username                     : String,
                        password                     : String,
                        maxPoolSize                  : Int,
                        leakDetectionThresholdSeconds: Int
                      ): DataSource = {
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

  /**
   * Create a HikariDataSource from given arguments.
   *
   * @param jdbcUrl  jdbc url for data source
   * @param username database username, optional
   * @param password database password, optional
   * @return data source
   */
  def createDataSource(
                        jdbcUrl : String,
                        username: String,
                        password: String
                      ): DataSource =
    createDataSource(jdbcUrl, username, password, DEFAULT_MAX_POOL_SIZE, DEFAULT_LEAK_DETECTION_THRESHOLD_SECONDS)

  /**
   * Create a HikariDataSource from given arguments.
   *
   * @param jdbcUrl  jdbc url for data source
   * @param username database username, optional
   * @param password database password, optional
   * @return data source
   */
  def createDataSource(
                        jdbcUrl : String
                      ): DataSource =
    createDataSource(jdbcUrl, null, null)


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

  /**
   * JDBC 上下文
   *
   * @param txMgr 数据源事务管理器
   * @param jdbcTemplate 可供使用的 JdbcTemplate 对象
   */
  case class JdbcContext(txMgr: DataSourceTransactionManager, jdbcTemplate: JdbcTemplate) {

    /**
     * 返回数据源( DataSource )
     * @return 数据源( DataSource )
     */
    def ds: DataSource = txMgr.getDataSource
  }

  def createJdbcContext(ds: DataSource): JdbcContext = {
    val txMgr = new DataSourceTransactionManager(ds)
    val jdbcTemplate = new JdbcTemplate(txMgr.getDataSource)

    JdbcContext(txMgr, jdbcTemplate)
  }

  def silentClose(resource: Closeable): Unit = {
    try {
      resource.close()
    } catch {
      case t: Throwable =>
    }
  }

  def inTrans(ds: DataSource, func: (Connection) => Unit): Unit = {
    Using.resource(ds.getConnection) {
      conn => {
        conn.setAutoCommit(false)
        try {
          func(conn)

          conn.commit()
        } catch {
          case t: Throwable =>
            conn.rollback()
            throw t
        }
      }
    }
  }

  def tableExists(ds: DataSource, sqlDialect: SqlDialect, tableName: String): Boolean = {
    val jdbcTemplate = new JdbcTemplate(ds)
    jdbcTemplate.execute(
      new ConnectionCallback[Boolean] {
        override def doInConnection(con: Connection): Boolean = {
          sqlDialect.tableExists(con, tableName)
        }
      }
    )
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

  def qryIntValue(sql: String, pss: PreparedStatementSetter = null)(implicit conn: Connection): Option[Int] =
    qryValue(sql, pss, (rs, _) => rs.getInt(1))

  def qryIntValueEx(sql: String, setter: StatementSetter = null)(implicit conn: Connection): Option[Int] =
    qryValueEx(sql, setter, rs => rs.int())

  def qryLongValue(sql: String, pss: PreparedStatementSetter = null)(implicit conn: Connection): Option[Long] =
    qryValue(sql, pss, (rs, _) => rs.getLong(1))

  def qryLongValueEx(sql: String, setter: StatementSetter = null)(implicit conn: Connection): Option[Long] =
    qryValueEx(sql, setter, rs => rs.bigInt())

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
    qryValueEx(
      sql, setter, acc => {
        mapper.map(acc)
      }
    ).orNull

  def qryList[T >: Null](sql: String, pss: PreparedStatementSetter, rowMapper: RowMapper[T])(implicit conn: Connection): java.util.List[T] = {
    val list = new util.ArrayList[T]()

    def loadFromResultSet(rs: ResultSet): Unit = {
      var i = 0
      try {
        while (rs.next()) {
          val r = rowMapper.mapRow(rs, i)
          if (r != null) {
            i += 1
            list.add(r)
          }
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

  def updateWithGenKey(sql: String, pss: PreparedStatementSetter = null)(implicit conn: Connection): UpdateCountAndKey = {
    if (pss != null) {
      val st = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
      try {
        pss.setValues(st)

        val updateCount = st.executeUpdate()

        Using.resource(st.getGeneratedKeys) { rs =>
          rs.next()
          new UpdateCountAndKey(updateCount, rs.getLong(1))
        }
      } finally {
        st.close()
      }
    } else {
      val st = conn.createStatement()
      try {
        val updateCount = st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS)

        Using.resource(st.getGeneratedKeys) { rs =>
          rs.next()
          new UpdateCountAndKey(updateCount, rs.getLong(1))
        }
      } finally {
        st.close()
      }
    }
  }

  def updateExWithGenKey(sql: String, setter: StatementSetter = null)(implicit conn: Connection): UpdateCountAndKey =
    updateWithGenKey(sql, StatementSetterWrapper(setter))

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


      argumentEntities.forEach(
        a => {
          st.clearParameters()
          binding.bind(a, binder.restart())
          st.addBatch()

          cnt += 1
          if (cnt == batchSize) {
            val updated = st.executeBatch()
            r ++= updated

            cnt = 0
          }
        }
      )

      if (cnt > 0)
        r ++= st.executeBatch()
    } finally {
      st.close()
    }

    r.toArray
  }

  def batchUpdateJdbc[T >: Null](sql: String, argumentEntities: util.Collection[T], binding: MultiSetting[T], batchSize: Int = 200)(implicit conn: Connection): Array[Int] = {
    val r: ArrayBuffer[Int] = ArrayBuffer()

    val st = conn.prepareStatement(sql)
    try {
      var cnt = 0

      argumentEntities.forEach(
        a => {
          st.clearParameters()
          binding.setParams(a, st)
          st.addBatch()

          cnt += 1
          if (cnt == batchSize) {
            val updated = st.executeBatch()
            r ++= updated

            cnt = 0
          }
        }
      )

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


      argumentEntities.forEach(
        a => {
          st.clearParameters()
          binding.bind(a, binder.restart())
          st.addBatch()

          cnt += 1
          if (cnt == batchSize) {
            st.executeBatch()
            cnt = 0
          }
        }
      )

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

  def callEx2[T](sql: String, processor: CallStmtProcessor[T])(implicit conn: Connection): T =
    Using.resource(conn.prepareCall(sql)) { st =>
      val binder = new CallableStmtHandler(st)
      processor.exec(conn, binder)
    }

  final val StringValueRowMapper: RowMapper[String] = new RowMapper[String] {
    override def mapRow(rs: ResultSet, rowNum: Int): String = rs.getString(1)
  }

  final val LongValueRowMapper: RowMapper[java.lang.Long] = new RowMapper[lang.Long] {
    override def mapRow(rs: ResultSet, rowNum: Int): lang.Long = {
      val r = rs.getLong(1)
      if (rs.wasNull())
        null
      else
        r
    }
  }

  def toTotalCountSql(relation: String): String =
    "SELECT COUNT(1) FROM " + relation

  def strPreparedStmtSetter(value: String): PreparedStatementSetter = new PreparedStatementSetter {
    override def setValues(ps: PreparedStatement): Unit = ps.setString(1, value)
  }

  def intPreparedStmtSetter(value: Int): PreparedStatementSetter = new PreparedStatementSetter {
    override def setValues(ps: PreparedStatement): Unit = ps.setInt(1, value)
  }

  def longPreparedStmtSetter(value: Long): PreparedStatementSetter = new PreparedStatementSetter {
    override def setValues(ps: PreparedStatement): Unit = ps.setLong(1, value)
  }

  def boolPreparedStmtSetter(value: Boolean): PreparedStatementSetter = new PreparedStatementSetter {
    override def setValues(ps: PreparedStatement): Unit = ps.setBoolean(1, value)
  }

  def strStatementSetter(value: String): StatementSetter = new StatementSetter {
    override def set(binder: StatementBinder): Unit = binder.setString(value)
  }

  def twoStrStatementSetter(s1: String, s2: String): StatementSetter = new StatementSetter {

    override def set(binder: StatementBinder): Unit = {
      binder.setString(s1)
      binder.setString(s2)
    }
  }

  def intStatementSetter(value: Int): StatementSetter = new StatementSetter {
    override def set(binder: StatementBinder): Unit = binder.setInt(value)
  }

  def longStatementSetter(value: Long): StatementSetter = new StatementSetter {
    override def set(binder: StatementBinder): Unit = binder.setLong(value)
  }

  def boolStatementSetter(value: Boolean): StatementSetter = new StatementSetter {
    override def set(binder: StatementBinder): Unit = binder.setBool(value)
  }

  def dateTimeStmtSetter(value: OffsetDateTime): StatementSetter = new StatementSetter {
    override def set(binder: StatementBinder): Unit = binder.setOffsetDateTime(value)
  }

  def timestampStmtSetter(value: Timestamp): StatementSetter = new StatementSetter {
    override def set(binder: StatementBinder): Unit = binder.setTimestamp(value)
  }

  def timestampStmtSetter(epochMillis: Long): StatementSetter = new StatementSetter {
    override def set(binder: StatementBinder): Unit = binder.setTimestamp(epochMillis)
  }

  def beijingDateTimeStmtSetter(epochMillis: Long): StatementSetter = new StatementSetter {
    override def set(binder: StatementBinder): Unit = binder.setBeijingConvOdt(epochMillis)
  }

  def dateTimeStmtSetter(epochMillis: Long, @Nullable zoneId: ZoneId): StatementSetter = new StatementSetter {
    override def set(binder: StatementBinder): Unit = binder.setOffsetDateTime(epochMillis, zoneId)
  }

}

