package com.mici.monitor.client;

import com.mici.monitor.Metric;
import com.mici.monitor.MonitorConfig;

import javax.annotation.Resource;

public class Monitor {
    @Resource
    MonitorConfig config;

    void count(String name){
        Sender sender = new Sender(config);
        sender.put(new Metric(name,1));
    }
}
