package com.lucendar.common.db.schema;

/**
 * 列（字段）类型枚举
 */
public enum ColumnKind {

    /**
     * 常规列
     */
    ORDINARY,

    /**
     * NOT NULL的列
     */
    NOT_NULL,

    /**
     * 主键列
     */
    PRIMARY_KEY,

    /**
     * 计算列
     */
    CALCULATED,

    /**
     * 查找列
     */
    LOOKUP;

    // CALCULATED, LOOKUP

    /**
     * 返回本列是否数据库有物理存储的列。没有物理存储的列类型是：计算列( CALCULATED ), 查找列( LOOKUP )
     * @return 本列类型是否在数据库有物理存储
     */
    public boolean isPersisted() {
        switch (this) {
            case ORDINARY:
            case NOT_NULL:
            case PRIMARY_KEY:
                return true;

            default:
                return false;
        }
    }

    /**
     * 返回本列类型是否必要列
     *
     * @return 本列类型是否必要列
     */
    public boolean isValueRequired() {
        switch (this) {
            case NOT_NULL:
            case PRIMARY_KEY:
                return true;

            default:
                return false;
        }
    }
}
