package com.lucendar.common.db.types;

public interface SqlDialectProvider {

    SqlDialect get();

    class SimpleSqlDialectProvider implements SqlDialectProvider {
        private final SqlDialect sqlDialect;

        public SimpleSqlDialectProvider(SqlDialect sqlDialect) {
            this.sqlDialect = sqlDialect;
        }

        @Override
        public SqlDialect get() {
            return sqlDialect;
        }
    }

}
