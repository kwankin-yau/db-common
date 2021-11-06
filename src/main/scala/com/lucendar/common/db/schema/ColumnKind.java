package com.lucendar.common.db.schema;

public enum ColumnKind {

    ORDINARY, NOT_NULL, PRIMARY_KEY, CALCULATED, LOOKUP;

    // CALCULATED, LOOKUP

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
