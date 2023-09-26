package com.myself.big.data.utils;

public enum PhType {
    DEFAULT(-1, "DEFAULT"),
    UNSIGNED_INT(4, "UNSIGNED_INT"),
    UNSIGNED_BIGINT(8, "UNSIGNED_BIGINT"),
    UNSIGNED_TINYINT(1, "UNSIGNED_TINYINT"),
    UNSIGNED_SMAILLINT(2, "UNSIGNED_SMAILLINT"),
    UNSIGNED_FLOAT(4, "UNSIGNED_FLOAT"),
    UNSIGNED_DOUBLE(8, "UNSIGNED_DOUBLE"),
    INTEGER(4, "INTEGER"),
    BIGINT(8, "BIGINT"),
    TINYINT(1, "TINYINT"),
    SMAILLINT(2, "SMAILLINT"),
    FLOAT(4, "FLOAT"),
    DOUBLE(8, "DOUBLE"),
    DECIMAL(-1, "DECIMAL"),
    BOOLEAN(1, "BOOLEAN"),
    TIME(8, "TIME"),                       //对应Phoenix的UNSIGNED_TIME
    DATE(8, "DATE"),                       //对应Phoenix的UNSIGNED_DATE
    TIMESTAMP(12, "TIMESTAMP"),            //对应Phoenix的UNSIGNED_TIMESTAMP
    VARCHAR(-1, "VARCHAR"),
    VARBINARY(-1, "VARBINARY");

    /**
     * -1：长度可变
     */
    private int len;
    private String type;

    PhType(int len, String type) {
        this.len = len;
        this.type = type;
    }

    public int getLen() {
        return len;
    }

    public String getType() {
        return this.type;
    }

    public static PhType getType(String type) {
        if (type == null) return DEFAULT;
        PhType phType;
        if (type.equalsIgnoreCase(UNSIGNED_INT.type)) {
            phType = UNSIGNED_INT;
        } else if (type.equalsIgnoreCase(UNSIGNED_BIGINT.type)) {
            phType = UNSIGNED_BIGINT;
        } else if (type.equalsIgnoreCase(UNSIGNED_TINYINT.type)) {
            phType = UNSIGNED_TINYINT;
        } else if (type.equalsIgnoreCase(UNSIGNED_SMAILLINT.type)) {
            phType = UNSIGNED_SMAILLINT;
        } else if (type.equalsIgnoreCase(UNSIGNED_FLOAT.type)) {
            phType = UNSIGNED_FLOAT;
        } else if (type.equalsIgnoreCase(UNSIGNED_DOUBLE.type)) {
            phType = UNSIGNED_DOUBLE;
        } else if (type.equalsIgnoreCase(INTEGER.type)) {
            phType = INTEGER;
        } else if (type.equalsIgnoreCase(BIGINT.type)) {
            phType = BIGINT;
        } else if (type.equalsIgnoreCase(TINYINT.type)) {
            phType = TINYINT;
        } else if (type.equalsIgnoreCase(SMAILLINT.type)) {
            phType = SMAILLINT;
        } else if (type.equalsIgnoreCase(FLOAT.type)) {
            phType = FLOAT;
        } else if (type.equalsIgnoreCase(DOUBLE.type)) {
            phType = DOUBLE;
        } else if (type.equalsIgnoreCase(BOOLEAN.type)) {
            phType = BOOLEAN;
        } else if (type.equalsIgnoreCase(TIME.type)) {
            phType = TIME;
        } else if (type.equalsIgnoreCase(DATE.type)) {
            phType = DATE;
        } else if (type.equalsIgnoreCase(TIMESTAMP.type)) {
            phType = TIMESTAMP;
        } else if (type.equalsIgnoreCase(VARCHAR.type)) {
            phType = VARCHAR;
        } else if (type.equalsIgnoreCase(VARBINARY.type)) {
            phType = VARBINARY;
        } else if (type.equalsIgnoreCase(DECIMAL.type)) {
            phType = DECIMAL;
        } else {
            phType = DEFAULT;
        }
        return phType;
    }


}
