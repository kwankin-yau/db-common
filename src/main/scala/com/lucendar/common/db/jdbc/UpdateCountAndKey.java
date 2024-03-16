/*
 * Copyright (c) 2024 lucendar.com.
 * All rights reserved.
 */
package com.lucendar.common.db.jdbc;

import java.util.StringJoiner;

public class UpdateCountAndKey {

    private int updateCount;
    private long generatedKey;

    public UpdateCountAndKey(int updateCount, long generatedKey) {
        this.updateCount = updateCount;
        this.generatedKey = generatedKey;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public long getGeneratedKey() {
        return generatedKey;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", UpdateCountAndKey.class.getSimpleName() + "[", "]")
                .add("updateCount=" + updateCount)
                .add("generatedKey=" + generatedKey)
                .toString();
    }
}
