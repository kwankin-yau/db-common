package com.lucendar.common.db.jdbc;

import java.sql.Connection;

/**
 * Callable statement processor
 * @param <T> 返回类型
 */
public interface CallStmtProcessor<T> {

    /**
     * Execute callback.
     *
     * @param conn the connection
     * @param handler the CallableStmtHandler
     * @return the result
     */
    T exec(Connection conn, CallableStmtHandler handler);
}
