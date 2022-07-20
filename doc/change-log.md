# 1.0.3 [2022-05-12]
## Changed
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
