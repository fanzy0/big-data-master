package com.myself.big.data.constant;

public interface RmsConstant {

    interface Quality {
        // 原始数据存放目录 注意需要加上后缀 日期 20230713
        // /smartHome/rms/quality/data/20230721/00
        String DATA_PATH = "/smartHome/rms/quality/data/";

        // 计算7天下挂质量平均值结果的存放地址 注意需要加上后缀 日期 20230713
        String SEVEN_AVG_ANALYTICS_PATH = "/smartHome/rms/quality/analytics/seven_avg/";

        // rms下挂质量表
        String ATTACH_QUALITY_DAY = "tr_rms_attach_quality_";

        String RMS_ATTACH_QUALITY_COLUMN_FAMILY_NAMES ="columnFamilyNames";

        String RMS_ATTACH_QUALITY_COLUMN_FAMILY_INFO = "rms_attach_info";

        String RMS_ATTACH_QUALITY_COLUMN_FAMILY_INFO_DEF = "0";

    }

    interface QueryByParams {
        String QUERY_BY_RMS_MAC = "rmsMac";
        String QUERY_BY_RMS_MAC_RESULT_KEY = "ints:analytics:rms:quality:query:rms_mac:";
        String QUERY_BY_RMS_MAC_RESULT_PATH = "/smartHome/rms/quality/query/rms_mac/";
    }


}
