package com.model;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;

public class TopIngredientsKPI extends KPIJson{

    private List<IngredientWithStats> ingredientsWithStats;


    @Override
    public JSONObject convertToJson() throws JSONException {

        JSONArray jsonArray = new JSONArray();
        for (IngredientWithStats ingredientWithStats: ingredientsWithStats
             ) {
            jsonArray.put(ingredientWithStats.convertToJSON());
        }
        JSONObject result = new JSONObject();
        result.put("ingredientsStats",jsonArray);

        return result;
    }

    public TopIngredientsKPI(String kpiName, List<IngredientWithStats> ingredientsWithStats) {
        super(kpiName);
        this.ingredientsWithStats = ingredientsWithStats;
    }

    public List<IngredientWithStats> getIngredientsWithStats() {
        return ingredientsWithStats;
    }

    public void setIngredientsWithStats(List<IngredientWithStats> ingredientsWithStats) {
        this.ingredientsWithStats = ingredientsWithStats;
    }
}
