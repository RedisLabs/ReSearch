package com.redislabs.research.text;

import java.util.List;

/**
 * Created by dvirsky on 28/02/16.
 */
public interface Tokenizer {
    List<Token> tokenize(String text);
}
