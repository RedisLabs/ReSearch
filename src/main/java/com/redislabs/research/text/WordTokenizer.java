package com.redislabs.research.text;


import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * Created by dvirsky on 28/02/16.
 */
public class WordTokenizer implements Tokenizer {

    TextNormalizer normalizer;
    public WordTokenizer(TextNormalizer nrml) {
        normalizer = nrml;
    }

    @Override
    public Iterable<Token> tokenize(String text) {
        StringTokenizer tkn = new StringTokenizer(text, " \t\n\r\f\"\'.,!@#$%^&*()[]{}\\/<>-_|:;~");

        HashMap<String, Token> tokens = new HashMap<>();
        int offset = 0;
        while (tkn.hasMoreTokens()) {

            String s = normalizer.normalize(tkn.nextToken());

            Token t = tokens.get(s);
            boolean isnew = false;
            if (t == null) {
                t = new Token(s);
                isnew = true;
            }

            t.frequency +=1;
            t.offsets.add(offset);
            offset++;

            if (isnew) {
                tokens.put(s, t);
            }


        }

        if (offset > 0 ) {
            for (Token t : tokens.values()) {
                t.frequency /= offset;
            }
        }

        return tokens.values();
    }
}
