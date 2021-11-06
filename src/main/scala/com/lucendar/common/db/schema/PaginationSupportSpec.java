package com.lucendar.common.db.schema;

/**
 * 分页支持规范
 */
public enum PaginationSupportSpec {

    /**
     * 默认(第1页，每页20条记录)
     */
    DEFAULT,

    /**
     * 可选
     */
    OPTIONAL,

    /**
     * 必须指定
     */
    MUST_SPECIFIED,

    /**
     * 不支持
     */
    NOT_SUPPORT
}
