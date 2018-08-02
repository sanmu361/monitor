package com.mici.monitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class Metric {

    private String name;
    private long timestamp;
    private Object value;
    private Map<String,String> tags = Maps.newHashMap();
    private String granularity;
    private String json;
    private Long version;

    public Metric(String name,String value,Map<String,String> tags){
        this.name = name;
        this.value = value;
        this.tags.putAll(tags);
        this.timestamp = System.currentTimeMillis();

    }

    public Metric addTag(String name ,String value){
        Preconditions.checkArgument(StringUtils.isNotEmpty(name));
        Preconditions.checkArgument(StringUtils.isNotEmpty(value));
        tags.put(name,value);

        return this;
    }

    public Metric addTags(Map<String,String> tags){
        Preconditions.checkArgument(MapUtils.isNotEmpty(tags));
        this.tags.putAll(tags);
        return this;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public void setGranularity(String granularity) {
        this.granularity = granularity;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Object getValue() {
        return value;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public String getGranularity() {
        return granularity;
    }

    public String getJson() {
        return json;
    }

    public Long getVersion() {
        return version;
    }
}
