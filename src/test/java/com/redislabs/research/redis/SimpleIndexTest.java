package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;
import com.sun.org.apache.xerces.internal.impl.dv.util.HexBin;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dvirsky on 08/02/16.
 */
public class SimpleIndexTest extends TestCase {

    public void testEncode() throws Exception {

        Document doc = new Document("123").set("foo", "hello world").set("bar", 123);

        Spec spec = new Spec(Spec.prefix("foo"), Spec.numeric("bar"));

        SimpleIndex idx = new SimpleIndex("localhost:6379", "myindex", spec);
        List<byte[]> entries = idx.encode(doc);
        for (byte[] entry : entries) {
            System.out.println(HexBin.encode(entry).toString());
        }
    }

    public void testIndex() {


        Spec spec = new Spec(Spec.prefix("foo"));

        SimpleIndex idx = new SimpleIndex("localhost:6379", "myindex", spec);

        Document[] docs = new Document[] {
                new Document("doc1").set("foo", "hello world").set("bar", Math.PI),
                new Document("doc2").set("foo", "hello werld").set("bar", Math.PI+1),
                new Document("doc3").set("foo", "jello world").set("bar", Math.PI-1),
        };

        idx.index(docs);


        try {
            List<String> ids = idx.get(new Query("myindex").filterPrefix("foo", "hell"));

            assertTrue(ids.contains("doc1"));
            assertTrue(ids.contains("doc2"));
            assertFalse(ids.contains("doc3"));

        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            idx.drop();
        }

    }


    public void testRange() {

        Spec spec = new Spec( Spec.numeric("bar"), Spec.prefix("foo"));
        SimpleIndex idx = new SimpleIndex("localhost:6379", "myindex", spec);

        Query q = new Query("myindex");

        q.filterEquals("bar", 3)
                .filterEquals("foo", "hell");
        try {

            SimpleIndex.Range rng = idx.getRange(q);

            System.out.println(new String(rng.from));
            System.out.println(new String(rng.to));
        } catch (IOException e) {
            fail();
        }



    }

    public void testCartesianProduct() throws Exception {

        List<List<byte[]>> raw = new ArrayList<>();
        List<byte[]> ls1 = new ArrayList();
        ls1.add("hello".getBytes());
        ls1.add("world".getBytes());
        List<byte[]> ls2 = new ArrayList();
        ls2.add("foo".getBytes());
        ls2.add("bar".getBytes());
        raw.add(ls1);
        raw.add(ls2);

        List<List<byte[]>> res = SimpleIndex.cartesianProduct(raw);

        String[] expected = new String[]{"hello||foo||", "hello||bar||", "world||foo||", "world||bar||"};
        int i = 0;
        for (List<byte[]> lst : res) {
            StringBuffer buf = new StringBuffer();
            for (byte []bs : lst) {
                buf.append(new String(bs));
                buf.append("||");
            }

            System.out.println(buf.toString());
            assertEquals(expected[i], buf.toString());
            i++;
        }

    }
}