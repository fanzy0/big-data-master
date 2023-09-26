package com.myself.big.data.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;

@Slf4j
public abstract class HbaseResultToBeanUtils {

    public static <T> T convertToBean(Result result, List<String> columnFamilyNames, Class<T> clazz) {
        try {
            T object = clazz.getDeclaredConstructor().newInstance();

            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                String columnName = field.getName();
                for (String columnFamilyName : columnFamilyNames) {
                    byte[] columnValueBytes = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName));
                    if (columnValueBytes != null) {
                        try {
                            setObjectFieldValue(object, field, columnValueBytes);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
            return object;
        } catch (Exception e) {
            log.error("[HBASE] convertToBean error, class : [{}]", clazz.getName());
        }
        return null;
    }

    private static <T> void setObjectFieldValue(T object, Field field, byte[] columnValueBytes) throws IllegalAccessException {
        Class<?> fieldType = field.getType();

        if (fieldType == String.class) {
            field.set(object, PhTypeUtil.toObject(columnValueBytes,PhType.VARCHAR));
        } else if (fieldType == int.class || fieldType == Integer.class) {
            field.set(object, PhTypeUtil.toObject(columnValueBytes,PhType.INTEGER));
        } else if (fieldType == long.class || fieldType == Long.class) {
            field.set(object, PhTypeUtil.toObject(columnValueBytes,PhType.BIGINT));
        } else if (fieldType == double.class || fieldType == Double.class) {
            field.set(object, PhTypeUtil.toObject(columnValueBytes,PhType.DOUBLE));
        } else if (fieldType == float.class || fieldType == Float.class) {
            field.set(object, PhTypeUtil.toObject(columnValueBytes,PhType.FLOAT));
        } else if (fieldType == boolean.class || fieldType == Boolean.class) {
            field.set(object, PhTypeUtil.toObject(columnValueBytes,PhType.BOOLEAN));
        } else if (fieldType.equals(Date.class)) {
            field.set(object, PhTypeUtil.toObject(columnValueBytes,PhType.DATE));
        }
        // 可根据需要添加其他类型的转换
    }

}
