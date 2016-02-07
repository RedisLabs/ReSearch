package com.redislabs.research;

import junit.framework.TestCase;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by dvirsky on 07/02/16.
 */
public class QueryTest extends TestCase {
    public void testQuery() {

        Query q = new Query("foo");

        assertEquals("foo", q.indexName);

        q.filterEquals("bar", "baz")
                .filterIn("baz", 1,2,3)
                .filterPrefix("xxx", "yyy")
                .sortBy("aaa", true)
                .limit(10, 20);

        assertEquals(3, q.filters.size());
        assertEquals("bar", q.filters.get(0).property);
        assertEquals(Query.Op.Equals, q.filters.get(0).op);
        assertEquals("baz", q.filters.get(0).values[0]);

        assertEquals("baz", q.filters.get(1).property);
        assertEquals(Query.Op.In, q.filters.get(1).op);
        assertEquals(3, q.filters.get(1).values.length);

        assertEquals("xxx", q.filters.get(2).property);
        assertEquals(Query.Op.Prefix, q.filters.get(2).op);
        assertEquals("yyy", q.filters.get(2).values[0]);

        assertEquals("aaa", q.sort.by);
        assertTrue(q.sort.ascending);
        assertTrue(10 == q.sort.offset);
        assertTrue(20 == q.sort.limit);
    }
}