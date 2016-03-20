package com.redislabs.research.text;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by dvirsky on 20/03/16.
 */
public class TokenSet extends HashMap<String, Token> {


    private int maxOffset = 0;
    private double totalFreq = 0;

    public void addAll(Collection<Token> tokens, double factor) {
        int baseOffset = maxOffset +1;
        for (Token t : tokens) {
            t.frequency *= factor;
            totalFreq += t.frequency;

            Token et = get(t.text);
            if (et != null) {
                et.merge(t, baseOffset);
            } else {
                put(t.text, t);
            }
            maxOffset = Math.max(maxOffset, Collections.max(t.offsets));
        }
    }

    public void normalize() {

        if (totalFreq > 0) {
            for (Token t : values()) {
                t.frequency /= totalFreq;
            }
        }
    }
}
