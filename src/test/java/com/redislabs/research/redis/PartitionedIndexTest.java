package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Index;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.List;

/**
 * Created by dvirsky on 21/02/16.
 */
public class PartitionedIndexTest extends TestCase {

    public void testPartitionFor() throws Exception {
        Spec spec = new Spec(Spec.prefix("foo", false));

        PartitionedIndex pi = PartitionedIndex.newSimple("foo", spec, 3, 500, 3,  "redis://localhost:6379");
        assertEquals(3, pi.partitions.length);

        assertEquals(2, pi.partitionFor("foo"));
        assertEquals(1, pi.partitionFor("barbar"));

    }

    public void testIndex() throws Exception {

        Spec spec = new Spec(Spec.prefix("foo", true));
        PartitionedIndex pi = PartitionedIndex.newSimple("foo", spec, 3, 500, 3, "redis://localhost:6379");
        Document[] docs = new Document[] {
                new Document("doc1").setScore(1.0f).set("foo", "hello world").set("bar", Math.PI),
                new Document("doc2").setScore(3.0f).set("foo", "hello werld wat wat in the butt").set("bar", Math.PI+1),
                new Document("doc3").setScore(2.0f).set("foo", "jello world").set("bar", Math.PI-1),
        };

        pi.index(docs);


        try {
            List<Index.Entry> ids = pi.get(new Query("myindex").filterPrefix("foo", "hell"));

            assertTrue(ids.contains(new Index.Entry("doc1",0)));
            assertTrue(ids.contains(new Index.Entry("doc2",0)));
            assertFalse(ids.contains(new Index.Entry("doc3",0)));

            ids = pi.get(new Query("myindex").filterPrefix("foo", "world"));

            assertTrue(ids.contains(new Index.Entry("doc1",0)));
            assertFalse(ids.contains(new Index.Entry("doc2",0)));
            assertTrue(ids.contains(new Index.Entry("doc3",0)));

        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            //pi.drop();
        }

    }


}