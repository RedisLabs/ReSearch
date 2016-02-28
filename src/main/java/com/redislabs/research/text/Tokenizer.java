package com.redislabs.research.text;

/**
 * Created by dvirsky on 28/02/16.
 */
public interface Tokenizer {
    Iterable<Token> tokenize(String text);
}
