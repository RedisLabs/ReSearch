package com.redislabs.research.redis;

import com.redislabs.research.text.NaiveNormalizer;
import com.redislabs.research.text.Token;
import com.redislabs.research.text.WordTokenizer;
import junit.framework.TestCase;

import java.util.List;

/**
 * Created by dvirsky on 28/02/16.
 */
public class FullTextFacetedIndexTest extends TestCase {

    public void testIndexTokens() throws Exception {

        String s = "Hello \"world\", I will miss? you... world? you are still here? Hello?";

        WordTokenizer tknz = new WordTokenizer(new NaiveNormalizer());

        Iterable<Token> tokens = tknz.tokenize(s);

        FullTextFacetedIndex idx = new FullTextFacetedIndex("redis://localhost:6379", "test", null, new WordTokenizer(new NaiveNormalizer()));

        idx.indexTokens("1", 1d, tokens);

        List<String> res = idx.executeQuery("hello world");
        assertTrue(res.contains("1"));
        assertTrue(res.size()==1);



    }
}