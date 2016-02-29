package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;
import com.redislabs.research.text.NaiveNormalizer;
import com.redislabs.research.text.Token;
import com.redislabs.research.text.WordTokenizer;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.List;

/**
 * Created by dvirsky on 28/02/16.
 */
public class FullTextFacetedIndexTest extends TestCase {

//    public void testIndexTokens() throws Exception {
//
//        String s = "Hello \"world\", I will miss? you... world? you are still here? Hello?";
//
//        WordTokenizer tknz = new WordTokenizer(new NaiveNormalizer());
//
//        FullTextFacetedIndex idx = new FullTextFacetedIndex("redis://localhost:6379", "test", null, new WordTokenizer(new NaiveNormalizer()));
//
//        idx.indexStringField("1", 0.2d, s, null);
//
//        List<String> res = idx.executeQuery("hello world");
//        assertTrue(res.contains("1"));
//        assertTrue(res.size()==1);
//
//    }

    public void testIndex() {


        Spec spec = new Spec(Spec.fulltext("foo", 1.0), Spec.numeric("bar"));
        FullTextFacetedIndex idx = new FullTextFacetedIndex("redis://localhost:6379", "test",
                spec, new WordTokenizer(new NaiveNormalizer()));

        Document[] docs = {
                new Document("doc1", 0.1).set("foo", "hello world").set("bar", Math.PI),
                new Document("doc2", 0.2).set("foo", "hello werld").set("bar", Math.PI+1),
                new Document("doc3", 0.3).set("foo", "jello world").set("bar", Math.PI-1),
        };

        try {
            idx.index(docs);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        Query q = new Query("test").filterMatches("foo", "hello");
        try {
            List<String> ids = idx.get(q);
            assertTrue(ids.size() == 2);
            assertTrue(ids.contains("doc1"));
            assertTrue(ids.contains("doc2"));
            assertTrue(ids.get(0).equals("doc2")); //doc2 has higher score

            q = new Query("test").filterMatches("foo", "HELLO? world");
            ids = idx.get(q);
            assertTrue(ids.size() == 1);
            assertTrue(ids.contains("doc1"));

            q = new Query("test").filterMatches("foo", "hello world")
                    .filterBetween("bar", Math.PI, Math.PI+0.5);
            ids = idx.get(q);
            assertEquals(1, ids.size());
            assertTrue(ids.contains("doc1"));



        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }




    }
}