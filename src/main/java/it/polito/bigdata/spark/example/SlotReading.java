package it.polito.bigdata.spark.example;

import java.io.Serializable;
@SuppressWarnings("serial")
public class SlotReading implements Serializable {
    private int stationId;
    private double totalReading;
    private double criticalReading;
    private String full_date;

    @Override
    public String toString() {
        return "SlotReading{" +
                "stationId=" + stationId +
                ", totalReading=" + totalReading +
                ", criticalReading=" + criticalReading +
                ", full_date='" + full_date + '\'' +
                '}';
    }

    public String getFull_date() {
        return full_date;
    }

    public void setFull_date(String full_date) {
        this.full_date = full_date;
    }

    public SlotReading(int stationId, double totalReading, double criticalReading, String full_date) {
        this.stationId = stationId;
        this.totalReading = totalReading;
        this.criticalReading = criticalReading;
        this.full_date = full_date;
    }

    public SlotReading() {
    }

    public SlotReading(int stationId, double totalReading, double criticalReading) {
        this.stationId = stationId;
        this.totalReading = totalReading;
        this.criticalReading = criticalReading;
    }

    public int getStationId() {
        return stationId;
    }

    public void setStationId(int stationId) {
        this.stationId = stationId;
    }

    public double getTotalReading() {
        return totalReading;
    }

    public void setTotalReading(double totalReading) {
        this.totalReading = totalReading;
    }

    public double getCriticalReading() {
        return criticalReading;
    }

    public void setCriticalReading(double criticalReading) {
        this.criticalReading = criticalReading;
    }

}
