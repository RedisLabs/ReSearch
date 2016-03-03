package com.redislabs.research.errors;

/**
 * The base exception for search exceptions
 */
public class SearchException extends RuntimeException {
    public SearchException(Exception e) {
        super(e);
    }
    public SearchException(String messae) {
        super(messae);
    }
}
