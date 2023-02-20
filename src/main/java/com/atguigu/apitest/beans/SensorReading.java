package com.atguigu.apitest.beans;

public class SensorReading {

    private String id;
    private Long timestamp;
    private double temperature;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public SensorReading(Long timestamp, String id, double temperature) {
        this.timestamp = timestamp;
        this.id = id;
        this.temperature = temperature;
    }

    public SensorReading(String id, Long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }
}
