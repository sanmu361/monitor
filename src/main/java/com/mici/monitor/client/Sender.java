package com.mici.monitor.client;

import com.google.common.util.concurrent.RateLimiter;
import com.mici.monitor.Metric;
import com.mici.monitor.MonitorConfig;
import com.mici.monitor.exception.HttpClientInitException;
import com.mici.monitor.http.HttpClient;
import com.mici.monitor.http.HttpClientFactory;
import com.mici.monitor.queue.DataQueue;
import com.mici.monitor.sendPool.DefaulThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;

public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);
    private final DataQueue queue;
    private final DefaulThreadPool defaulThreadPool;
    private final boolean httpCompress;
    private final HttpClient httpclient;
    private RateLimiter rateLimter;
    private final MonitorConfig config;

    public Sender(MonitorConfig config) throws HttpClientInitException {
        this.config = config;
        this.httpclient = HttpClientFactory.createHttpClient(config);
        this.httpCompress = config.isHttpCompress();
        boolean asyncPut = config.isAsyncPut();
        int maxTPS = config.getMaxTPS();
        if (maxTPS > 0) {
            this.rateLimter = RateLimiter.create(maxTPS);
        }

            this.queue = null;
            this.defaulThreadPool = null;

        this.httpclient.start();
        LOGGER.info("The hitsdb-client has started.");
    }

    public void close() throws IOException {
        // 优雅关闭
        this.close(false);
    }

    public void close(boolean force) throws IOException {
        if (force) {
            forceClose();
        } else {
            gracefulClose();
        }
        LOGGER.info("The hitsdb-client has closed.");
    }

    private void forceClose() throws IOException {
        boolean async = config.isAsyncPut();
        if (async) {
            // 消费者关闭
            this.defaulThreadPool.stop(true);
        }

        // 客户端关闭
        this.httpclient.close(true);
    }

    private void gracefulClose() throws IOException {
        boolean async = config.isAsyncPut();

        if (async) {
            // 停止写入
            this.queue.forbiddenSend();

            // 等待队列消费为空
            this.queue.waitEmpty();

            // 消费者关闭
            this.defaulThreadPool.stop();
        }

        // 客户端关闭
        this.httpclient.close();
    }

    public void put(Metric metric) {
        queue.send(metric);
    }

}
