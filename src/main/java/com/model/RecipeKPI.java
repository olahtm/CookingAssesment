package com.model;

import javafx.util.Pair;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class RecipeKPI extends KPIJson {

    private String videoID;
    private Long duration;

    private Long minStartTime;
    private Long maxEndTime;

    public RecipeKPI(String kpiName) {
        super(kpiName);
    }

    public String getVideoID() {
        return videoID;
    }

    public void setVideoID(String videoID) {
        this.videoID = videoID;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public Long getMinStartTime() {
        return minStartTime;
    }

    public void setMinStartTime(Long minStartTime) {
        this.minStartTime = minStartTime;
    }

    public Long getMaxEndTime() {
        return maxEndTime;
    }

    public void setMaxEndTime(Long maxEndTime) {
        this.maxEndTime = maxEndTime;
    }

    @Override
    public String toString() {
        return "Recipe{" +
                "videoID='" + videoID + '\'' +
                ", duration=" + duration +
                ", startTime=" + minStartTime +
                ", endTime=" + maxEndTime +
                '}';
    }

    @Override
    public  JSONObject convertToJson() throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("videoID",videoID);
        jsonObject.put("minStartTime",minStartTime);
        jsonObject.put("maxEndTime",maxEndTime);
        jsonObject.put("duration",duration);
        return jsonObject;
    }
}
