package com.myself.big.data.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableAsync
@EnableScheduling
public class PoolExecutor {

    private static final Logger log = LoggerFactory.getLogger(PoolExecutor.class);

    @Value("${analytic.poolExecutor.coreSize:30}")
    private int corePoolSize;

    @Value("${analytic.poolExecutor.maxPoolSize:50}")
    private int maxPoolSize;


    @Bean({"analyticsPool"})
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(this.corePoolSize);
        taskExecutor.setMaxPoolSize(this.maxPoolSize);
        taskExecutor.setQueueCapacity(0);
        taskExecutor.setKeepAliveSeconds(120);
        taskExecutor.setThreadNamePrefix("analyticsPool");
        taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        taskExecutor.setAwaitTerminationSeconds(120);
        taskExecutor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                try {
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    log.error("线程池添加任务失败", e);
                }
            }
        });
        log.warn("analytics pool: corePoolSize=" + taskExecutor.getCorePoolSize() + ",maxPoolSize=" + taskExecutor
                .getMaxPoolSize() + ",queueCapacity=" + 0);
        return taskExecutor;
    }
}
