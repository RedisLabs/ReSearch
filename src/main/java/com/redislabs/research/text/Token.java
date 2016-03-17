package com.redislabs.research.text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by dvirsky on 28/02/16.
 */
public class Token {
    public String text;
    public Double frequency;
    public List<Integer> offsets;

    public Token(String s) {
        text = s;
        frequency = 0D;
        offsets = new ArrayList<>(1);
    }

    public Token(String s, double freq, Integer ...offsets) {
        text = s;
        frequency = freq;
        this.offsets = Arrays.asList(offsets);
    }
}
