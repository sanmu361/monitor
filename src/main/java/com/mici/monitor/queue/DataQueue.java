package com.mici.monitor.queue;

import com.mici.monitor.Metric;

public interface DataQueue {
    void send(Metric metric);

    Metric receive() throws InterruptedException;

    Metric receive(int timeout) throws InterruptedException;

    void forbiddenSend();

    void waitEmpty();

    boolean isEmpty();
}
