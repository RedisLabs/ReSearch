package com.redislabs.research.redis;

import com.redislabs.research.Document;
import junit.framework.TestCase;

import java.util.List;

/**
 * Created by dvirsky on 21/02/16.
 */
public class JSONStoreTest extends TestCase {

    public void testJSONStore() {

        Document[] docs = new Document[] {
                new Document("doc1").set("foo", "foo1").set("bar", "bar1"),
                new Document("doc2").set("foo", "foo2").set("bar", "bar2"),
                new Document("doc3").set("foo", "foo3").set("bar", "bar3"),
        };

        JSONStore s = new JSONStore("redis://localhost:6379");

        s.store(docs);

        List<Document> ldocs = s.load("doc1", "doc2", "doc3", "doc4");

        assertEquals(3, ldocs.size());
        assertEquals("doc1", ldocs.get(0).getId());
        assertEquals("foo1", ldocs.get(0).property("foo"));
        assertEquals("bar1", ldocs.get(0).property("bar"));

        assertEquals("doc2", ldocs.get(1).getId());
        assertEquals("foo2", ldocs.get(1).property("foo"));
        assertEquals("bar2", ldocs.get(1).property("bar"));

        assertEquals("doc3", ldocs.get(2).getId());
        assertEquals("foo3", ldocs.get(2).property("foo"));
        assertEquals("bar3", ldocs.get(2).property("bar"));


        int rc = s.delete("doc1", "doc2", "doc3", "doc4");
        assertEquals(3, rc);
        ldocs = s.load("doc1", "doc2", "doc3", "doc4");
        assertEquals(0, ldocs.size());
    }
}