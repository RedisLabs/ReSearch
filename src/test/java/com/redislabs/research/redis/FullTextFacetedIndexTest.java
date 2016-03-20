package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Index;
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


        Spec spec = new Spec(Spec.fulltext("foo", "foo"), Spec.numeric("bar"),
                        Spec.geo("location", Encoders.Geohash.PRECISION_4KM));

        FullTextFacetedIndex idx = null;
        try {
            idx = new FullTextFacetedIndex("redis://localhost:6379", "test",
                    spec, new WordTokenizer(new NaiveNormalizer(), false, null));
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        Document[] docs = {
                new Document("doc1", 0.1).set("foo", "hello world").set("bar", Math.PI)
                        .set("location",  new Double[]{40.7142700, -74.0059700}),
                new Document("doc2", 0.2).set("foo", "hello werld").set("bar", Math.PI+1)
                        .set("location",  new Double[]{40.7842700, -74.0159700}),
                new Document("doc3", 0.3).set("foo", "jello world").set("bar", Math.PI-1)
                        .set("location",  new Double[]{40.7242700, -74.0259700}),
        };

        try {
            idx.index(docs);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        Query q = new Query("test").filterMatches("foo", "hello");
        try {
            List<Index.Entry> ids = idx.get(q);
            assertTrue(ids.size() == 2);
            assertTrue(ids.contains(new Index.Entry("doc1",0)));
            assertTrue(ids.contains(new Index.Entry("doc2",0)));
            assertTrue(ids.get(0).id.equals("doc2")); //doc2 has higher score

            q = new Query("test").filterMatches("foo", "HELLO? world");
            ids = idx.get(q);
            assertEquals(1, ids.size());
            assertTrue(ids.contains(new Index.Entry("doc1",0)));

            q = new Query("test").filterMatches("foo", "hello world")
                    .filterBetween("bar", Math.PI, Math.PI+0.5);
            ids = idx.get(q);
            assertEquals(1, ids.size());
            assertTrue(ids.contains(new Index.Entry("doc1",0)));



            q = new Query("test").filterMatches("foo", "hello")
                    .filterRadius("location", 40.7842700, -74.0159700, 4000d);

            for (int i = 0; i < 1; i++) {
                long st = System.currentTimeMillis();
                ids = idx.get(q);
                long elapsed = System.currentTimeMillis() - st;
                if (i % 100 == 1) {
                    System.out.printf("Elapsed :%d\n", elapsed);
                }
            }

            assertEquals(2, ids.size());
            assertTrue(ids.contains(new Index.Entry("doc1",0)));
            assertTrue(ids.contains(new Index.Entry("doc1",0)));



        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }


}