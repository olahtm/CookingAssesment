package com.model;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class IngredientWithStats {

    private String ingredientName;
    private Long recipeCount;
    private Long distinctRecipeCount;

    public IngredientWithStats(String ingredientName, Long recipeCount, Long distinctRecipeCount) {
        this.ingredientName = ingredientName;
        this.recipeCount = recipeCount;
        this.distinctRecipeCount = distinctRecipeCount;
    }

    public JSONObject convertToJSON() throws JSONException {
        JSONObject result = new JSONObject();
        result.put("ingredient",ingredientName);
        result.put("recipeCount",recipeCount);
        result.put("distinctRecipeCount",distinctRecipeCount);
        return result;
    }

    public String getIngredientName() {
        return ingredientName;
    }

    public void setIngredientName(String ingredientName) {
        this.ingredientName = ingredientName;
    }

    public Long getRecipeCount() {
        return recipeCount;
    }

    public void setRecipeCount(Long recipeCount) {
        this.recipeCount = recipeCount;
    }

    public Long getDistinctRecipeCount() {
        return distinctRecipeCount;
    }

    public void setDistinctRecipeCount(Long distinctRecipeCount) {
        this.distinctRecipeCount = distinctRecipeCount;
    }
}
