package com.redislabs.research.redis;

import com.redislabs.research.Document;
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
                new Document("doc1").set("foo", "hello world").set("bar", Math.PI),
                new Document("doc2").set("foo", "hello werld").set("bar", Math.PI+1),
                new Document("doc3").set("foo", "jello world").set("bar", Math.PI-1),
        };

        pi.index(docs);


        try {
            List<String> ids = pi.get(new Query("myindex").filterPrefix("foo", "hell"));

            assertTrue(ids.contains("doc1"));
            assertTrue(ids.contains("doc2"));
            assertFalse(ids.contains("doc3"));

            ids = pi.get(new Query("myindex").filterPrefix("foo", "world"));

            assertTrue(ids.contains("doc1"));
            assertFalse(ids.contains("doc2"));
            assertTrue(ids.contains("doc3"));

        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            pi.drop();
        }

    }


}