package com.myself.big.data.core.dao;


import com.myself.big.data.core.bean.RmsAttachQualityModelHbase;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.util.List;

@Mapper
@Repository
@Qualifier("phoenixSqlSessionFactory")
public interface RmsAttachQualityModelHbaseMapper {


    /**
     * 创建日表
     * @param dayStr 日期
     */
    public void createRmsAttachQualityTable(@Param("salt")int salt,@Param("dayStr") String dayStr);

    /**
     * 指定索引并创建-本地索引
     * @param index 索引
     * @param dayStr 日期
     */
    public void appointRmsAttachQualityLocalIndex(@Param("index")String index, @Param("salt")int salt,@Param("dayStr")String dayStr);

    /**
     * 批量插入
     * @param qualityModel 光猫下挂质量数据 注意这里的id取一个随机
     * @return 插入状态
     */
    public boolean upsertIntoRmsAttachQualityTable(@Param("item") RmsAttachQualityModelHbase qualityModel , @Param("dayStr")String dayStr);


    /**
     *
     * @param rmsMac 光猫mac
     * @param dayStr 日期
     * @return  所有光猫的下挂设备
     */
    public List<RmsAttachQualityModelHbase> queryRmsAttachQualityModelsByRmsMac(@Param("rmsMac") String rmsMac,@Param("dayStr")String dayStr);

    /**
     * 指定索引并创建-全局索引
     * @param index 索引
     * @param salt 分区
     * @param dayStr 日期
     */
    void appointRmsAttachQualityGlobalIndex(@Param("index")String index,@Param("salt")int salt, @Param("dayStr")String dayStr);

}
