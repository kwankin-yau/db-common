package com.lucendar.common.db.schema;

import com.lucendar.common.db.types.Predication;
import info.gratour.common.error.ErrorWithCode;

public enum FieldDataType {

    BOOL, SMALL_INT, INT, BIGINT, TEXT, DECIMAL, FLOAT, DOUBLE,
    LOCAL_DATE, LOCAL_DATETIME, TIMESTAMP_WITH_ZONE, BINARY,
    INT_ARRAY;

    public boolean isDateOrTimestamp() {
        return this == LOCAL_DATE || this == LOCAL_DATETIME || this == TIMESTAMP_WITH_ZONE;
    }
    public boolean isText() {
        return this == TEXT;
    }
    public boolean isBool() {
        return this == BOOL;
    }
    public boolean isNumber() {
        switch (this) {
            case SMALL_INT:
            case INT:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    public boolean isTimestamp() {
        return this == LOCAL_DATETIME || this == TIMESTAMP_WITH_ZONE;
    }

    public boolean supportPredication(Predication pred) {
        switch (this) {
            case BOOL:
                return pred == Predication.EQUAL || pred == Predication.IN || pred == Predication.IS_NULL || pred == Predication.NOT_NULL;

            case SMALL_INT:
            case INT:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
            case LOCAL_DATE:
            case LOCAL_DATETIME:
            case TIMESTAMP_WITH_ZONE:
                switch (pred) {
                    case EQUAL:
                    case LESS:
                    case LESS_EQUAL:
                    case GREAT:
                    case GREAT_EQUAL:
                    case IN:
                    case IS_NULL:
                    case NOT_NULL:
                        return true;
                    default:
                        return false;
                }

            case INT_ARRAY:
                switch (pred) {
                    case IS_NULL:
                    case NOT_NULL:
                        return true;

                    default:
                        return false;
                }

            case TEXT:
                switch (pred) {
                    case EQUAL:
                    case IN:
                    case IS_NULL:
                    case NOT_NULL:
                    case START_WITH:
                    case END_WITH:
                    case INCLUDE:
                    case UNDER:
                        return true;

                    default:
                        return Predication.isLike(pred);
                }

            default:
                throw ErrorWithCode.internalError(String.format("Unhandled case `%s`.", pred.name()));
        }
    }
}
