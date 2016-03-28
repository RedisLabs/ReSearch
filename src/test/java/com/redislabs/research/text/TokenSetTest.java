package com.redislabs.research.text;

import junit.framework.TestCase;

import java.util.List;

/**
 * Created by dvirsky on 20/03/16.
 */
public class TokenSetTest extends TestCase {

    public void testAddAll() throws Exception {

        Tokenizer tokenizer = new WordTokenizer(new NaiveNormalizer(), false, null);

        List<Token> tokens = tokenizer.tokenize("hello world");
        assertEquals(2, tokens.size());
        assertEquals(1d, tokens.get(0).frequency);
        TokenSet ts = new TokenSet();
        ts.addAll(tokens, 1);
        assertEquals(2, ts.size());

        tokens = tokenizer.tokenize("world police");
        ts.addAll(tokens, 2);
        assertEquals(3, ts.size());
        assertEquals(3d, ts.get("world").frequency);
        assertEquals(1d, ts.get("hello").frequency);

        ts.normalize(ts.getTotalFreq());

        assertEquals(0.166d, ts.get("hello").frequency, 0.001);
        assertEquals(0.5d, ts.get("world").frequency, 0.001);



    }


}