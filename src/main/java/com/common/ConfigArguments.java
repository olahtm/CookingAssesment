package com.common;

import com.beust.jcommander.Parameter;

import java.io.Serializable;

/**
 * General config arguments used for apps
 */
public class ConfigArguments implements Serializable {

    @Parameter(names = {"--config", "-C"}, description = "Config File Name")
    private String configFileName = "config";

    @Parameter(names ={"--ingredients"}, description = "Ingredients to search")
    private String ingredients;

    public void setConfigFileName(String configFileName) {
        this.configFileName = configFileName;
    }

    public String getConfigFileName() {
        return configFileName;
    }

    public String getIngredients() {
        return ingredients;
    }

    public void setIngredients(String ingredients) {
        this.ingredients = ingredients;
    }
}
