package com.lucendar.common.db.schema;


import info.gratour.common.error.ErrorWithCode;
import info.gratour.common.error.Errors;

public class FieldNameMapper implements ToDbFieldNameMapper, ToApiFieldNameMapper {

    public static final FieldNameMapper INSTANCE = new FieldNameMapper();

    @Override
    public String toApiFieldName(String columnName) {
        if (columnName.indexOf("f_") != 0)
            throw new ErrorWithCode(Errors.INTERNAL_ERROR, String.format("Unsupported DB column name: %s.", columnName));

        StringBuilder result = new StringBuilder();
        int i = 2;
        boolean upperCase = false;
        while (i < columnName.length()) {
            char c = columnName.charAt(i);
            if (c == '_')
                upperCase = true;
            else {
                if (upperCase && c >= 'a' && c <= 'z') {
                    result.append(Character.toUpperCase(c));
                } else
                    result.append(c);

                upperCase = false;
            }

            i++;
        }

        return result.toString();
    }

    @Override
    public String[] toDbColumnNames(String fieldName) {
        return new String[]{toDbColumnName(fieldName)};
    }

    private String toDbColumnName(String fieldName) {
        // find `~` character and truncate
        int end = fieldName.indexOf('~');
        if (end >= 0)
            fieldName = fieldName.substring(0, end);

        if (fieldName.length() == 0)
            throw new ErrorWithCode(Errors.INTERNAL_ERROR, String.format("Unsupported API column name: %s.", fieldName));

        StringBuilder result = new StringBuilder("f_");
        int i = 0;
        while (i < fieldName.length()) {
            char c = fieldName.charAt(i);

            if (c >= 'A' && c <= 'Z') {
                result.append('_');
                result.append(Character.toLowerCase(c));
            } else
                result.append(c);

            i++;
        }
        return result.toString();
    }
}
