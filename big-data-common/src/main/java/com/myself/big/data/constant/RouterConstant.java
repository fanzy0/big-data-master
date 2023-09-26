package com.myself.big.data.constant;

public interface RouterConstant {

    interface Quality {
        // TODO 按日存之后是否再分
        // 原始数据存放目录 注意需要加上后缀 日期 20230713
        String DATA_PATH = "/smartHome/router/quality/data/";

        // 计算7天下挂质量平均值结果的存放地址 注意需要加上后缀 日期 20230713
        String SEVEN_AVG_ANALYTICS_PATH = "/smartHome/router/quality/analytics/seven_avg/";

        // router下挂质量表
        String ATTACH_QUALITY_DAY = "router_attach_quality_day";
    }


}
