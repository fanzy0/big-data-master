package com.myself.big.data.utils;

import cn.hutool.core.lang.Snowflake;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Date;

/**
 * 雪花算法生成id
 */
@Component
@Slf4j
public class SnowflakeUtils {

    private static long workerId = 1;

    private static long dataCenterId = 1;


    /**
     * 开始时间2015-01-01
     */
    private static final long START_TIME = 1420041600000L;

    @PostConstruct
    void init() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            NetworkInterface network = NetworkInterface.getByInetAddress(ip);
            if (network == null) {
                dataCenterId = 1L;
            } else {
                byte[] mac = network.getHardwareAddress();
                dataCenterId = ((0x000000FF & (long) mac[mac.length - 1])
                        | (0x0000FF00 & (((long) mac[mac.length - 2]) << 8))) >> 6;
                dataCenterId = dataCenterId % (31L + 1);
            }
            log.info("当前机器 dataCenterId: {}", dataCenterId);
        } catch (Exception e) {
            log.warn("获取机器 dataCenterId 失败", e);
            dataCenterId = RandomUtils.nextLong(0, 31);
            log.info("当前机器 dataCenterId: {}", dataCenterId);
        }
        try {
            StringBuffer mpid = new StringBuffer();
            mpid.append(dataCenterId);
            String name = ManagementFactory.getRuntimeMXBean().getName();
            if (!name.isEmpty()) {
                /*
                 * GET jvmPid
                 */
                mpid.append(name.split("@")[0]);
            }
            /*
             * MAC + PID 的 hashcode 获取16个低位
             */
            workerId = (mpid.toString().hashCode() & 0xffff) % (31L + 1);

            log.info("当前机器 workerId: {}", workerId);
        } catch (Exception e) {
            log.warn("获取机器 workerId 失败", e);
            workerId = RandomUtils.nextLong(0, 31);
            log.info("当前机器 workerId: {}", workerId);
        }
        snowflake = new Snowflake(new Date(START_TIME), workerId, dataCenterId, true);
    }

    private static Snowflake snowflake = null;

    public Long generate() {
        return snowflake.nextId();
    }

    public Long getGenerateTime(long id) {
        return snowflake.getGenerateDateTime(id);
    }

    public Date getGenerateDateTime(long id) {
        return new Date(snowflake.getGenerateDateTime(id));
    }




}
