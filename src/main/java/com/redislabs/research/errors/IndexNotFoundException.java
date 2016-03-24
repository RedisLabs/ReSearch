package com.redislabs.research.errors;

/**
 * Created by dvirsky on 24/03/16.
 */
public class IndexNotFoundException extends SearchException{
    public IndexNotFoundException(String indexName) {
        super("Index "+ indexName + " not found");
    }
}
