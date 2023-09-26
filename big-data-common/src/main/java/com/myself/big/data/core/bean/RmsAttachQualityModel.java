package com.myself.big.data.core.bean;

import lombok.Data;

import java.util.Date;

/**
 * 光猫下挂终端质量表
 */
@Data
public class RmsAttachQualityModel {
    private long id;
    //帐号
    private String pppoe;
    //签约带宽
    private String signBandwidth;
    //光猫型号
    private String modelName;
    //光猫mac
    private String rmsMac;
    //下挂终端mac
    private String attachMac;

    //下挂终端型号
    private String attachModel;
    //下挂终端频段
    private String attachChannel;
    //连接协议
    //1代表Wi-Fi 4，
    //2代表Wi-Fi 5，
    //3代表Wi-Fi 6，
    //0代表没有上报数据
    private String attachProtocol;

    //信号强度
    private String rssi;
    //协商速率（Mbps）
    private String speed;
    //
    private Date addTime;

    //数据生成的小时
    private String hourStr;

    //终端类型
    private String attachType;
}
