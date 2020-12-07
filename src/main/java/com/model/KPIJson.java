package com.model;

import javafx.util.Pair;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public abstract class KPIJson {

    private String kpiName;

    public KPIJson(String kpiName) {
        this.kpiName = kpiName;
    }

    public abstract JSONObject convertToJson() throws JSONException;

    public String getKpiName() {
        return kpiName;
    }

    public void setKpiName(String kpiName) {
        this.kpiName = kpiName;
    }
}
