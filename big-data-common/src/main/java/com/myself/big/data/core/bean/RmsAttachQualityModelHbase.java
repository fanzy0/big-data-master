package com.myself.big.data.core.bean;


import com.myself.big.data.annotation.HbaseRowKeyAnnotation;
import lombok.Data;

import java.util.Date;


@Data
public class RmsAttachQualityModelHbase extends RmsAttachQualityModel {

    // hbase中的rowKey
    private long rowKey;

    //帐号
    @HbaseRowKeyAnnotation(1)
    private String pppoe;

    //光猫mac
    @HbaseRowKeyAnnotation(2)
    private String rmsMac;
    //下挂终端mac
    @HbaseRowKeyAnnotation(3)
    private String attachMac;

    @HbaseRowKeyAnnotation(4)
    private Date addTime;


}
