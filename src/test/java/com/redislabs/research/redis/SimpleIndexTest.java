package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Spec;
import com.sun.org.apache.xerces.internal.impl.dv.util.HexBin;
import junit.framework.TestCase;

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
        Document doc = new Document("123").set("foo", "hello world").set("bar", Math.PI);

        Spec spec = new Spec(Spec.prefix("foo"), Spec.numeric("bar"));

        SimpleIndex idx = new SimpleIndex("localhost:6379", "myindex", spec);

        idx.index(doc);
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