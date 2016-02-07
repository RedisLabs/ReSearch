package com.redislabs.research;

import java.io.Serializable;
import java.util.Map;

/**
 * Document represents a single indexed document or entity in the engine
 */
public class Document implements Serializable {

    private String id;
    private Map<String, Object> properties;

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
     * Get the document's id
     *
     * @return
     */
    public String id() {
        return id;
    }

}
