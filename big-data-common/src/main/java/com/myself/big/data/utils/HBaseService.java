package com.myself.big.data.utils;


import com.myself.big.data.annotation.HbaseRowKeyAnnotation;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.collection.ListUtil;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * TODO 方法没有全部测试过
 */
@Slf4j
@Service
public class HBaseService {
    private Connection connection = null;

    private synchronized Connection getHbaseConnection() {
        try {
            if (connection == null) {
                Configuration configuration = HBaseConfiguration.create();
//                configuration.addResource(new Path(coreSitePath));
//                configuration.addResource(new Path(hdfsSitePath));
//                configuration.addResource(new Path(hbaseSitePath));
                connection = ConnectionFactory.createConnection(configuration);
            }
            return connection;
        } catch (IOException e) {
            log.error("[HBASE] init connect hbase error", e);
            return null;
        }
    }

    @PostConstruct
    public void initConnection() {
        getHbaseConnection();
    }


    /**
     * 表是否存在
     *
     * @param tableName 表
     * @return true false
     */
    public boolean isExistTable(String tableName) {
        try {
            log.info("[HBASE] isExistTable table [{}]", tableName);
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            try {
                return admin.tableExists(TableName.valueOf(tableName));
            } catch (TableNotFoundException e) {
                log.error("[HBASE] isExistTable table not found ", e);
                return false;
            }
        } catch (IOException e) {
            log.error("[HBASE] isExistTable table error ", e);
        }
        return false;
    }

    /**
     * 创建 HBase 表
     *
     * @param tableName      表名
     * @param columnFamilies 列族的数组
     */
    public boolean createTable(String tableName, List<String> columnFamilies) {
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                log.warn("[HBASE] createTable error , already exist");
                return false;
            }
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            columnFamilies.forEach(columnFamily -> {
                ColumnFamilyDescriptorBuilder cfDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                cfDescriptorBuilder.setMaxVersions(1);
                ColumnFamilyDescriptor familyDescriptor = cfDescriptorBuilder.build();
                tableDescriptor.setColumnFamily(familyDescriptor);
            });
            admin.createTable(tableDescriptor.build());
            return true;
        } catch (IOException e) {
            log.error("[HBASE] createTable error ", e);
        }
        return false;
    }

    /**
     * 删除 hBase 表
     *
     * @param tableName 表名
     */
    public boolean deleteTable(String tableName) {
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                log.warn("[HBASE] deleteTable but no exist table");
                return true;
            }
            // 删除表前需要先禁用表
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            IOUtils.closeStream(admin);
            return true;
        } catch (Exception e) {
            log.error("[HBASE] delete table error ", e);
        }
        return false;
    }

    /**
     * 根据 rowKey 列族 列 插入数据 一条value
     *
     * @param tableName        表名
     * @param rowKey           唯一标识
     * @param columnFamilyName 列族名
     * @param qualifier        列标识
     * @param value            数据
     */
    public void putRowByRCQ(String tableName, String rowKey, String columnFamilyName, String qualifier,
                            String value) {
        try {

            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            IOUtils.closeStream(table);
        } catch (IOException e) {
            log.error("[HBASE] putRowByRCQ error ", e);
        }
    }

    /**
     * 根据 rowkey 列族 插入一组列(列+value) 插入数据
     *
     * @param tableName        表名
     * @param rowKey           唯一标识
     * @param columnFamilyName 列族名
     * @param pairList         列标识和值的集合
     */
    public void putRowByRC(String tableName, String rowKey, String columnFamilyName, List<Pair<String, String>> pairList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            pairList.forEach(pair -> put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(pair.getKey()), Bytes.toBytes(pair.getValue())));
            table.put(put);
            IOUtils.closeStream(table);
        } catch (IOException e) {
            log.error("[HBASE] putRowByRC error ", e);
        }
    }


    /**
     * 根据列族批量插入一组bean对象
     *
     * @param tableName        表
     * @param columnFamilyName 列族
     * @param objects          java对象
     * @param <T>              泛型
     */
    public <T> void putRowByCBatch(String tableName, String columnFamilyName, List<T> objects) {
        try {
            TableName table = TableName.valueOf(tableName);
            BufferedMutator mutator = connection.getBufferedMutator(table);
            List<Put> putList = new ArrayList<>();
            for (T object : objects) {
                Put put = createPut(object, columnFamilyName); // 创建Put对象
                putList.add(put);
                if (putList.size() >= 1000) {
                    mutator.mutate(putList); // 执行批量写入操作
                    putList.clear(); // 清空putList
                }
            }
            // 处理剩余的putList
            if (!putList.isEmpty()) {
                mutator.mutate(putList);
            }
            IOUtils.closeStream(mutator); // 关闭mutator
        } catch (Exception e) {
            log.error("[HBASE] putRowByCBatch error ", e);
        }

    }

    private static <T> Put createPut(T object, String columnFamilyName) {
        try {
            Put put = new Put(Bytes.toBytes(Objects.requireNonNull(getRowKeyFromBean(object)))); // 创建Put对象并指定行键
            Field[] fields = object.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                if (field.get(object) == null) {
                    continue;
                }
                String columnName = field.getName();
                String columnValue;
                if (field.getType().equals(Date.class)) {
                    columnValue = String.valueOf(((Date) field.get(object)).getTime());
                } else {
                    columnValue = field.get(object).toString();
                }

                put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
            }
            return put;
        } catch (Exception e) {
            log.error("[HBASE] createPut error ", e);
        }
        return null;
    }

    /**
     * 根据对象上的注解组成rowKey 注解顺序排序 1 等级最高 [时间类型除外 该类型放在row key 的最后一位]
     *
     * @param object 对象类
     * @param <T>    形参
     * @return rowKey
     */
    private static <T> String getRowKeyFromBean(T object) {
        try {
            Field[] fields = object.getClass().getDeclaredFields();
            List<Field> annoFiles = new ArrayList<>();
            for (Field field : fields) {
                if (field.isAnnotationPresent(HbaseRowKeyAnnotation.class)) {
                    annoFiles.add(field);
                }
            }
            if (CollectionUtil.isEmpty(annoFiles)) {
                log.error("[HBASE] getRowKeyFromBean error, no appoint row key class : [{}]", object.getClass());
                return null;
            }
            // 按照 注解顺序排序 1 等级最高 时间类型除外 该类型放在row key 的最后一位
            ListUtil.sort(annoFiles, Comparator.comparingInt(o -> o.getAnnotation(HbaseRowKeyAnnotation.class).value()));
            StringBuilder stringBuffer = new StringBuilder();
            Field dateFiled = null;
            for (Field annoFile : annoFiles) {
                if (annoFile.getType().equals(Date.class)) {
                    dateFiled = annoFile;
                } else {
                    annoFile.setAccessible(true);
                    stringBuffer.append(annoFile.get(object) == null ? "0" : annoFile.get(object)).append("_");
                }
            }
            if (dateFiled != null) {
                dateFiled.setAccessible(true);
                stringBuffer.append(generateRowKey((Date) dateFiled.get(object)));
            } else {
                // 去掉最后一个 _
                stringBuffer.deleteCharAt(stringBuffer.length() - 1);
            }
            return stringBuffer.toString();
        } catch (Exception e) {
            log.error("[HBASE] getRowKeyFromBean error, Object : [{}]", JSONObject.toJSONString(object));
        }
        return null;
    }

    /**
     * 时间倒序
     *
     * @param date 时间
     * @return 倒序的时间戳
     */
    private static String generateRowKey(Date date) {
        return String.valueOf(Long.MAX_VALUE - date.getTime());
    }

    /**
     * 根据 rowKey 获取指定行的数据 唯一一条结果 或者为空
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     */
    public <T> T getRowByRowKey(String tableName, String rowKey, List<String> columnFamilyNames, Class<T> clazz) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            return HbaseResultToBeanUtils.convertToBean(table.get(get), columnFamilyNames, clazz);
        } catch (IOException e) {
            log.error("[HBASE] getRowByRowKey error, table [{}] , rowKey [{}]", tableName, rowKey, e);
        }
        return null;
    }

    /**
     * 获取指定行指定列 (cell) 的最新版本的数据
     *
     * @param tableName    表名
     * @param rowKey       唯一标识
     * @param columnFamily 列族
     * @param qualifier    列标识
     */
    public String getCellByRCQ(String tableName, String rowKey, String columnFamily, String qualifier) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            if (!get.isCheckExistenceOnly()) {
                get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                Result result = table.get(get);
                byte[] resultValue = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                return Bytes.toString(resultValue);
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error("[HBASE] getCellByRCQ error, table [{}] , rowKey [{}] , columnFamily [{}] , qualifier [{}]", tableName, rowKey, columnFamily, qualifier, e);
        }
        return null;
    }

    /**
     * 检索全表
     *
     * @param tableName 表名
     */
    public ResultScanner getScannerFromTableAll(String tableName) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            return table.getScanner(scan);
        } catch (IOException e) {
            log.error("[HBASE] getScannerFromTableAll error, table [{}] ", tableName, e);
        }
        return null;
    }

    /**
     * 检索表中指定数据
     *
     * @param tableName  表名
     * @param filterList 过滤器
     */
    public ResultScanner getScannerByFilter(String tableName, FilterList filterList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setFilter(filterList);
            return table.getScanner(scan);
        } catch (Exception e) {
            log.error("[HBASE] getScannerByFilter error, table [{}] ", tableName, e);
        }
        return null;
    }

    /**
     * 检索表中指定数据
     *
     * @param tableName         表名
     * @param filterList        过滤器
     * @param columnFamilyNames 列族
     * @param clazz             返回类型
     */
    public <T> List<T> getScannerByFilterToBean(String tableName, FilterList filterList, List<String> columnFamilyNames, Class<T> clazz) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setFilter(filterList);
            return convertToListBean(table.getScanner(scan), columnFamilyNames, clazz);
        } catch (Exception e) {
            log.error("[HBASE] getScannerByFilter error, table [{}] ", tableName, e);
        }
        return null;
    }


    /**
     * 检索表中指定数据 范围查询
     *
     * @param tableName         表名
     * @param startRowKey       起始 RowKey
     * @param endRowKey         终止 RowKey
     * @param filterList        过滤器
     * @param columnFamilyNames 列族
     * @param clazz             类
     */
    public <T> List<T> getScannerByRangeToBean(String tableName, String startRowKey, String endRowKey,
                                               FilterList filterList, List<String> columnFamilyNames, Class<T> clazz) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRowKey));
            scan.withStopRow(Bytes.toBytes(endRowKey));
            scan.setFilter(filterList);
            return convertToListBean(table.getScanner(scan), columnFamilyNames, clazz);
            //return table.getScanner(scan);
        } catch (IOException e) {
            log.error("[HBASE] getScannerByRange error, table [{}] , startRowKey [{}] , endRowKey [{}]", tableName, startRowKey, endRowKey, e);
        }
        return null;
    }

    private static <T> List<T> convertToListBean(ResultScanner scanner, List<String> columnFamilyNames, Class<T> clazz) {
        if (scanner != null) {
            List<T> allResult = new ArrayList<>();
            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                allResult.add(HbaseResultToBeanUtils.convertToBean(result, columnFamilyNames, clazz));
            }

            return allResult;
        }
        return null;
    }

    /**
     * 检索表中指定数据 范围查询
     *
     * @param tableName   表名
     * @param startRowKey 起始 RowKey
     * @param endRowKey   终止 RowKey
     * @param filterList  过滤器
     */

    public ResultScanner getScannerByRange(String tableName, String startRowKey, String endRowKey,
                                           FilterList filterList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            scan.setFilter(filterList);
            return table.getScanner(scan);
        } catch (IOException e) {
            log.error("[HBASE] getScannerByRange error, table [{}] , startRowKey [{}] , endRowKey [{}]", tableName, startRowKey, endRowKey, e);
        }
        return null;
    }

    /**
     * 删除指定行记录 删除一行数据
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     */
    public boolean deleteRowByRowKey(String tableName, String rowKey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            return true;
        } catch (IOException e) {
            log.error("[HBASE] deleteRowByRowKey error, table [{}] , rowKey [{}] , endRowKey [{}]", tableName, rowKey, e);
        }
        return false;
    }


    /**
     * 删除指定行的指定列
     *
     * @param tableName    表名
     * @param rowKey       唯一标识
     * @param columnFamily 列族
     * @param qualifier    列标识
     */
    public boolean deleteColumnByRCQ(String tableName, String rowKey, String columnFamily,
                                     String qualifier) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            table.delete(delete);
            table.close();
            return true;
        } catch (IOException e) {
            log.error("[HBASE] deleteColumnByRCQ error, table [{}] , rowKey [{}] , columnFamily [{}] , qualifier [{}]", tableName, rowKey, columnFamily, qualifier, e);
        }
        return true;
    }


}
