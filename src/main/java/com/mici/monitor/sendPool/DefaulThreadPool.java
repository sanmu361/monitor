package com.mici.monitor.sendPool;

import com.google.common.util.concurrent.RateLimiter;
import com.mici.monitor.Metric;
import com.mici.monitor.MonitorConfig;
import com.mici.monitor.client.Monitor;
import com.mici.monitor.http.HttpClient;
import com.mici.monitor.queue.DataQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaulThreadPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaulThreadPool.class);
    private DataQueue dataQueue;
    private ExecutorService threadPool;
    private int batchPutConsumerThreadCount;
    private HttpClient httpclient;
    private MonitorConfig config;
    private RateLimiter rateLimiter;
    private CountDownLatch countDownLatch;

    public DefaulThreadPool(DataQueue buffer, HttpClient httpclient, RateLimiter rateLimiter, MonitorConfig config) {
        this.dataQueue = buffer;
        this.httpclient = httpclient;
        this.config = config;
        this.countDownLatch = new CountDownLatch(config.getBatchPutConsumerThreadCount());
        this.batchPutConsumerThreadCount = config.getBatchPutConsumerThreadCount();
        this.rateLimiter = rateLimiter;
        threadPool = Executors.newFixedThreadPool(batchPutConsumerThreadCount);
    }

    public void start() {
        for (int i = 0; i < batchPutConsumerThreadCount; i++) {
            threadPool.submit(new SendRannable(this.dataQueue, this.httpclient, this.config,this.countDownLatch,this.rateLimiter));
        }
    }

    public void stop() {
        this.stop(false);
    }

    public void stop(boolean force) {
        if (threadPool != null) {
            if (force) {
                // 强制退出不等待，截断消费者线程。
                threadPool.shutdownNow();
            } else {
                // 截断消费者线程。
                while(!threadPool.isShutdown() || !threadPool.isTerminated()){
                    threadPool.shutdownNow();
                }

                // 等待所有消费者线程结束。
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    LOGGER.error("An error occurred waiting for the consumer thread to close", e);
                }
            }
        }

        if (dataQueue != null) {
            dataQueue = null;
        }
    }
}
