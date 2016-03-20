package com.redislabs.research;

import com.google.gson.Gson;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Document represents a single indexed document or entity in the engine
 */
public class Document implements Serializable {

    private String id;
    private double score;
    private Map<String, Object> properties;

    public Document(String id, double score) {
        this.id = id;
        this.score = score;
        properties = new HashMap<>();
    }

    public Document(String id) {
        this.id = id;
        this.score = 1.0;
        properties = new HashMap<>();
    }



    public Document set(String key, Object value) {
        properties.put(key, value);
        return this;
    }
    /**
     * return the property value inside a key
     *
     * @param key key of the property
     * @return
     */
    public Object property(String key) {
        return properties.get(key);
    }


    /**
     * @return the document's score
     */
    public double getScore() {
        return score;
    }

    /**
     * Set the document's score
     * @param score new score to set
     * @return the document itself
     */
    public Document setScore(double score) {
        this.score = score;
        return this;
    }
    /**
     * @return the document's id
     */
    public String getId() {
        return id;
    }

    private static Gson gson = new Gson();
    @Override
    public String toString() {
        return gson.toJson(this);
    }

    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }
}
