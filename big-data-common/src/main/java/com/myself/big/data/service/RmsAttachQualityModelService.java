package com.myself.big.data.service;


import com.myself.big.data.core.bean.RmsAttachQualityModelHbase;

import java.util.List;

public interface RmsAttachQualityModelService {

    /**
     * 创建日表
     * @param salt 分区
     * @param dayStr 日期
     */
    public void createRmsAttachQualityTable(int salt,String dayStr);


    /**
     * 指定索引并创建 本地索引  索引必须是表中的一个列名
     * 注意：创建本地索引的数据 只能使用Phoenix sql查询 原生的HBase API 查不到了
     *      删除了本地索引，那 HBase API 就又可以查询到原数据了
     * @param index 索引
     * @param salt 分区
     * @param dayStr 日期
     */
    public void appointRmsAttachQualityLocalIndex(String index,int salt, String dayStr);


    /**
     * 指定索引并创建 全局索引  索引必须是表中的一个列名
     * 注意：创建全局索引的数据 原生的HBase API 可以查到
     *                      但是主键查不到
     * @param index 索引
     * @param dayStr 日期
     */
    public void appointRmsAttachQualityGlobalIndex(String index,int salt, String dayStr);


    /**
     * 批量插入
     * @param qualityModelList 光猫下挂质量数据 注意这里的id取一个随机
     * @return 插入状态
     */
    public boolean batchInsertRmsAttachQualityTable(List<RmsAttachQualityModelHbase> qualityModelList , String dayStr);

    /**
     *
     * @param rmsMac mac
     * @param dayStr 日期
     * @return 查询结果
     */
    public List<RmsAttachQualityModelHbase> queryRmsAttachQualityModelsByRmsMac(String rmsMac,String dayStr);


}
