package com.mici.monitor.sendPool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.util.concurrent.RateLimiter;
import com.mici.monitor.Metric;
import com.mici.monitor.MonitorConfig;
import com.mici.monitor.http.HttpClient;
import com.mici.monitor.http.HttpURL;
import com.mici.monitor.queue.DataQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SendRannable implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(SendRannable.class);

    /**
     * 缓冲队列
     */
    private final DataQueue dataQueue;

    /**
     * Http客户端
     */
    private final HttpClient hitsdbHttpClient;

    private final MonitorConfig monitorConfig;

    private final CountDownLatch countDownLatch;

    private int batchSize;

    private int batchPutTimeLimit;

    public SendRannable(DataQueue dataQueue, HttpClient httpclient, MonitorConfig config, CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        this.dataQueue = dataQueue;
        this.hitsdbHttpClient = httpclient;
        this.monitorConfig = config;
        this.countDownLatch = countDownLatch;
        this.batchSize = config.getBatchPutSize();
        this.batchPutTimeLimit = config.getBatchPutTimeLimit();
    }

    @Override
    public void run() {
        // 线程变量sb，paramsMap，waitPoint，readyClose 每个线程只有一组这样的变量。
        StringBuilder sb = new StringBuilder();

        Map<String, String> paramsMap = new HashMap<String, String>();

        Metric waitPoint = null;
        boolean readyClose = false;
        int waitTimeLimit = batchPutTimeLimit/3;
        long batchPutTimeLimitNano = batchPutTimeLimit*1000l;

        while (true) {
            if(readyClose && waitPoint == null) {
                break ;
            }

            long t0 = System.nanoTime();  // nano
            List<Metric> pointList = new ArrayList<Metric>(batchSize);
            if (waitPoint != null) {
                pointList.add(waitPoint);
                waitPoint = null;
            }

            for (int i = pointList.size(); i < batchSize; i++) {
                try {
                    Metric point = dataQueue.receive(waitTimeLimit);
                    if (point != null) {
                        pointList.add(point);
                    }

                    long t1 = System.nanoTime();  // nano
                    if(t1-t0 > batchPutTimeLimitNano) {
                        break;
                    }
                		/*
	                	Point point = dataQueue.receive(this.batchPutTimeLimit);
	                if(point == null) {
	                		break;
	                }
	                pointList.add(point);
	                long t1 = System.nanoTime();  // nano
	                if(t1-t0 > batchPutTimeLimitNano) {
	                    break;
	                }
                		*/
                } catch (InterruptedException e) {
                    readyClose = true;
                    logger.info("The thread {} is interrupted", Thread.currentThread().getName());
                    break;
                }
            }

            if (pointList.size() == 0 && !readyClose) {
                try {
                    Metric newPoint = dataQueue.receive();
                    waitPoint = newPoint;
                    continue ;
                } catch (InterruptedException e) {
                    readyClose = true;
                    logger.info("The thread {} is interrupted", Thread.currentThread().getName());
                }
            }

            if(pointList.size() == 0) {
                continue;
            }

            // 序列化
            String strJson = serialize(pointList, sb);

            // 发送
            sendHttpRequest(pointList,strJson,paramsMap);
        }

        if (readyClose) {
            this.countDownLatch.countDown();
            return ;
        }
    }

    private void sendHttpRequest(List<Metric> pointList,String strJson,Map<String,String> paramsMap) {
        String address = getAddressAndSemaphoreAcquire();

        // 发送

        try {
            hitsdbHttpClient.post(address+HttpURL.PUT, strJson);
        } catch (Exception ex) {
            logger.error("send metric error json = {}",strJson);
        }
    }

    private String getAddressAndSemaphoreAcquire() {

        return monitorConfig.getHost() + ":" + monitorConfig.getPort();
    }

    private String serialize(List<Metric> metricList, StringBuilder sb) {
        return JSON.toJSONString(metricList, SerializerFeature.DisableCircularReferenceDetect);
    }
}
