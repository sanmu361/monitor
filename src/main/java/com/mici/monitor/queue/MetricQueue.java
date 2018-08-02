package com.mici.monitor.queue;

import com.mici.monitor.Metric;
import com.mici.monitor.exception.BufferQueueFullException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MetricQueue implements DataQueue{
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricQueue.class);
    private final BlockingQueue<Metric> metricQueue;
    private final AtomicBoolean forbiddenWrite = new AtomicBoolean(false);
    private final int waitCloseTimeLimit;
    private final boolean backpressure;

    public MetricQueue(int size,int waitCloseTimeLimit,boolean backpressure) {
        this.metricQueue = new ArrayBlockingQueue<Metric>(size);
        this.waitCloseTimeLimit = waitCloseTimeLimit;
        this.backpressure = backpressure;
    }

    public void send(Metric metric) {
        if (forbiddenWrite.get()) {
            throw new IllegalStateException("client has been closed.");
        }

        if(this.backpressure){
            try {
                metricQueue.put(metric);
            } catch (InterruptedException e) {
                LOGGER.error("Client Thread been Interrupted.",e);
                return;
            }
        } else {
            try {
                metricQueue.add(metric);
            } catch(IllegalStateException exception) {
                throw new BufferQueueFullException("The buffer queue is full.",exception);
            }
        }
    }

    public Metric receive() throws InterruptedException {
        Metric metric = null;
        metric = metricQueue.take();
        return metric;
    }

    public Metric receive(int timeout) throws InterruptedException {
        Metric point = metricQueue.poll(timeout, TimeUnit.MILLISECONDS);
        return point;
    }

    @Override
    public void forbiddenSend() {
        forbiddenWrite.compareAndSet(false, true);
    }

    @Override
    public void waitEmpty() {
        // 等待为空之前，必须已经设置了禁止写入
        if (forbiddenWrite.get()) {
            try {
                Thread.sleep(waitCloseTimeLimit);
            } catch (InterruptedException e) {
                LOGGER.warn("The method waitEmpty() is being illegally interrupted");
            }

            while (true) {
                boolean empty = metricQueue.isEmpty();
                if (empty) {
                    return ;
                } else {
                    try {
                        Thread.sleep(waitCloseTimeLimit);
                    } catch (InterruptedException e) {
                        LOGGER.warn("The waitEmpty() method is being illegally interrupted");
                    }
                }
            }
        } else {
            throw new IllegalStateException(
                    "The queue is still allowed to write data. you must first call the forbiddenSend() method");
        }


    }

    @Override
    public boolean isEmpty() {
        return metricQueue.isEmpty();
    }
}
