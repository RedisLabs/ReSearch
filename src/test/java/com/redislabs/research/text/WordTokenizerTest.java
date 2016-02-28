package com.redislabs.research.text;

import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by dvirsky on 28/02/16.
 */
public class WordTokenizerTest extends TestCase {

    public void testTokenize() {

        String s = "Hello \"world\", I will miss? you... world? you are still here? Hello?";

        WordTokenizer tknz = new WordTokenizer(new NaiveNormalizer());

        Iterable<Token> tokens = tknz.tokenize(s);

        Map<String, Token> expected = new HashMap<String, Token>();
        expected.put("hello", new Token("hello",  0.16666667, new Integer[]{0, 11}));
        expected.put("are", new Token("are",  0.083333336, new Integer[]{8}));
        expected.put("hehehe", new Token("hehehe", 0.076923, new Integer[]{12}));
        expected.put("will", new Token("will",  0.083333336, new Integer[]{3}));
        expected.put("here", new Token("here",0.083333336, new Integer[]{10}));
        expected.put("still", new Token("still", 0.083333336, new Integer[]{9}));
        expected.put("you", new Token("you",0.16666667, new Integer[]{5, 7}));
        expected.put("miss", new Token("miss", 0.083333336, new Integer[]{4}));
        expected.put("world", new Token("world", 0.16666667, new Integer[]{1, 6}));
        expected.put("i", new Token("i", 0.083333336,new Integer[]{2}));


        for (Token t : tokens) {
            Token tt = expected.get(t.text);
            assertNotNull(tt);
            assertEquals(t.text, tt.text);
            assertEquals(t.frequency.floatValue(), tt.frequency.floatValue());

            for (int i = 0; i < t.offsets.size(); i++) {
                assertEquals(t.offsets.get(i), tt.offsets.get(i));
            }


        }

    }
}