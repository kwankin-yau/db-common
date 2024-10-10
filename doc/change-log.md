# 2.1.0 [2024-03-04]
## 改进
- 引入 `ServerVer` （数据库）服务端版本号
- 引入 `com.lucendar.common.db.jdbc.StatementBinder.setBeijingConvOdt(Long)`, 
  `setTimestampBeijing(LocalDateTime)`, `setTimestamp(String)`, `setTimestamp(OffsetDateTime)`
- 引入 `com.lucendar.common.db.jdbc.DbHelper.updateWithGenKey`
- `JdbcContext` 增加 `ds` 方法
- 引入 `com.lucendar.common.db.rest.QueryParams.HookedResultSetMapper`
- `DbHelper` 增加 `beijingDateTimeStmtSetter`, `twoStrStatementSetter`, `callEx2(String, (Connection, CallableStmtBinder) => T)` 方法
- 引入 `com.lucendar.common.db.jdbc.DbSupport.execute`
- `DbUtils` 增加 `batchInsertSql`, `splitColumns`, `batchInsertOnConflictNoActionSql`, `upsertSql` 方法
- `CallableStmtBinder` 改名为 `CallableStmtHandler`, 并增加 `execute` 方法
- 增加 `DERBY` 方言
- `SqlDialect` 增加 `isPostgreSQL`, `isDerby`, `isSqlite` 方法
- 引入 `com.lucendar.common.db.jdbc.DbHelper.timestampStmtSetter`
- 引入 `com.lucendar.common.db.jdbc.ResultSetAccessor.date()`, `timestamp()` 方法
- 引入 `com.lucendar.common.db.jdbc.StatementBinder.setDate` 方法
- 引入 `com.lucendar.common.db.jdbc.DbHelper.dateStmtSetter` 方法
- 引入 `com.lucendar.common.db.jdbc.DbSupport.tableExists(String, String)` 方法 

## 变更
- 不兼容变更
  - `StatementBinder.setOffsetDateTime(java.lang.Long)` 增加`zoneId: ZoneId`参数
  - `DbHelper.dateTimeStmtSetter(java.lang.Long)`增加`zoneId: ZoneId`参数

# 2.0.0 [2023-12-03]
## Improvement
- Introduce `SqlDialectProvider`
- Introduce `com.lucendar.common.db.jdbc.StatementBinder.setOdtBeijingConv`
- Introduce `com.lucendar.common.db.jdbc.ResultSetAccessor.beijingConvDateTimeStr`
- Introduce `com.lucendar.common.db.jdbc.ResultSetAccessor.bigIntObj`
- Introduce `com.lucendar.common.db.jdbc.ResultSetAccessor.int32`, `bigInt`, `bigIntObj`
- Introduce `com.lucendar.common.db.schema.SimpleSelect.offset`


## Changes
- Breaking changes: 
  - value of `com.lucendar.common.db.types.SqlDialects.POSTGRESQL` changed to `postgresql`.
- Maven group id changed to `com.lucendar`
- Dependencies
  - Bump `lucendar-common` to 2.0.0
  - Bump `server-common` to 3.0.0

# 1.1.0 [2023-10-25]
## Improvement
- `com.lucendar.common.db.jdbc.StatementBinder.setShortUseInteger` added
- Upgrade JVM version to 17
- Upgrade to spring 6
## Fixed
- `com.lucendar.common.db.jdbc.StatementBinder.setBytes` direct use PreparedStatement.setBytes() method instead of 
  setBinaryStream because sqlite does not support this method.

# 1.0.6 [2023-05-15]
## Changed
update build.gradle to adapt to gradle 8.0, upgrade dependencies
## Improvement
- `com.lucendar.common.db.jdbc.DbHelper.tableExists` added

# 1.0.5 [2022-12-11]
## Changed
- Dependencies:
  - Bump `scala-library` to 2.13.10
  - Bump `scala-reflect` to 2.13.10
  - Bump `gratour-common` to 3.2.7
  - Bump `server-common` to 1.0.5

# 1.0.4 [2022-09-09]
## Improvement
- `com.lucendar.common.db.jdbc.DbHelper.qryList` add handle case for HookedMapper.mapRow return `null` 
  when total record count is `0`.


# 1.0.3 [2022-05-12]
## Improvement
- `com.lucendar.common.db.jdbc.DbHelper.qryIntValue, qryIntValueEx` added
- `com.lucendar.common.db.jdbc.DbSupport.qryIntValue, qryIntValueEx` added

## Changed
- Shorten SqlDialect ID
- Dependencies:
    - Bump `spring-jdbc`, `spring-web` from `5.3.9` to `5.3.19`
    - Bump `HikariCP` from `5.0.0` to `5.0.1`
    - Bump `scala-library`, `scala-reflect` from `2.13.6` to `2.13.8`
    - Bump `gratour-common` from `3.2.2` to `3.2.5`
  
# 1.0.2 [2022-03-12]
## Added
- `com.lucendar.common.db.types.SqlDialects.SQLITE`
- `com.lucendar.common.db.jdbc.DbHelper.DEFAULT_MAX_POOL_SIZE`
- `com.lucendar.common.db.jdbc.DbHelper.DEFAULT_LEAK_DETECTION_THRESHOLD_SECONDS`
- `com.lucendar.common.db.jdbc.DbHelper.toTotalCountSql`
- `com.lucendar.common.db.schema.SimpleSelect`
- `com.lucendar.common.db.jdbc.DbHelper.strPreparedStmtSetter`
- `com.lucendar.common.db.jdbc.DbHelper.intPreparedStmtSetter`
- `com.lucendar.common.db.jdbc.DbHelper.longPreparedStmtSetter`
- `com.lucendar.common.db.jdbc.DbHelper.boolPreparedStmtSetter`
- `com.lucendar.common.db.jdbc.DbHelper.strStatementSetter`
- `com.lucendar.common.db.jdbc.DbHelper.intStatementSetter`
- `com.lucendar.common.db.jdbc.DbHelper.longStatementSetter`
- `com.lucendar.common.db.jdbc.DbHelper.boolStatementSetter`

## Changed
- Visibility of `com.lucendar.common.db.jdbc.DbSupport.sqlDialect` changed to public
- Visibility of `com.lucendar.common.db.jdbc.DbSupport.jdbcCtx` changed to public

# 1.0.1 [2021-12-30]
## Added
- `Types.MultiSetting`
- `DbHelper.batchUpdateJdbc`, `DbSupport.batchUpdateJdbc`, `DbHelper.StringValueRowMapper`, `DbHelper.LongValueRowMapper`
- `TableSchema` allow no primary key defined 
- `ResultSetAccessor.epochMillisLong()`, `ResultSetAccessor.epochMillisLongObj()`
