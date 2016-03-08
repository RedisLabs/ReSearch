package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Index;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;

import com.sun.org.apache.xerces.internal.impl.dv.util.HexBin;
import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dvirsky on 08/02/16.
 */
public class SimpleIndexTest extends TestCase {

    public void testEncode() throws Exception {

        Document doc = new Document("123").setScore(0.512).set("foo", "hello world").set("bar", 123);

        Spec spec = new Spec(Spec.prefix("foo",false), Spec.numeric("bar"));

        SimpleIndex idx = new SimpleIndex("redis://localhost:6379", "myindex", spec);
        List<byte[]> entries = idx.encode(doc);
        assertEquals(1, entries.size());

        Index.Entry ent = idx.extractEntry(entries.get(0));
        assertEquals(doc.getId(), ent.id);
        assertEquals(doc.getScore(), ent.score);



    }

    public void testIndex() {


        double x = 1;
        long bits = Double.doubleToRawLongBits(x);
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.putLong(bits);

        ByteBuffer bb8 = ByteBuffer.wrap(bb.array());
        bb8.order(ByteOrder.BIG_ENDIAN);
        bits = bb8.getLong();
        x = Double.longBitsToDouble(bits);


        Spec spec = new Spec(Spec.prefix("foo", false));

        SimpleIndex idx = new SimpleIndex("redis://localhost:6379", "myindex", spec);

        Document[] docs = {
                new Document("doc1").setScore(1.0).set("foo", "hello world").set("bar", Math.PI),
                new Document("doc2").setScore(2.0).set("foo", "hello werld").set("bar", Math.PI+1),
                new Document("doc3").setScore(1.0).set("foo", "jello world").set("bar", Math.PI-1),
        };

        try {
            idx.index(docs);
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            List<Index.Entry> ids = idx.get(new Query("myindex").filterPrefix("foo", "hell"));

            assertTrue(ids.contains(new Index.Entry("doc1",0)));
            assertTrue(ids.contains(new Index.Entry("doc2",0)));
            assertFalse(ids.contains(new Index.Entry("doc3",0)));
            assertEquals(ids.get(0).id, "doc2");
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            idx.drop();
        }

    }


    public void testRange() {

        Spec spec = new Spec( Spec.numeric("bar"), Spec.prefix("foo", false));
        SimpleIndex idx = new SimpleIndex("redis://localhost:6379", "myindex", spec);

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


        q = new Query("myindex");

        q.filterGreaterThan("bar", 1234);
        try {

            SimpleIndex.Range rng = idx.getRange(q);

            assertEquals("2800000000000004D27C", HexBin.encode(rng.from));
            assertEquals("28FFFFFFFFFFFFFFFF7CFF", HexBin.encode(rng.to));

        } catch (IOException e) {
            fail();
        }

        q = new Query("myindex");

        q.filterGreaterEqual("bar", 1234);
        try {

            SimpleIndex.Range rng = idx.getRange(q);

            assertEquals("5B00000000000004D27C", HexBin.encode(rng.from));
            assertEquals("28FFFFFFFFFFFFFFFF7CFF", HexBin.encode(rng.to));

        } catch (IOException e) {
            fail();
        }

        q = new Query("myindex");

        q.filterLessThan("bar", 1234);
        try {

            SimpleIndex.Range rng = idx.getRange(q);

            assertEquals("5B00000000000000007C", HexBin.encode(rng.from));
            assertEquals("2800000000000004D27CFF", HexBin.encode(rng.to));

        } catch (IOException e) {
            fail();
        }

        q = new Query("myindex");

        q.filterLessEqual("bar", 1234);
        try {

            SimpleIndex.Range rng = idx.getRange(q);

            assertEquals("5B00000000000000007C", HexBin.encode(rng.from));
            assertEquals("5B00000000000004D27CFF", HexBin.encode(rng.to));

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


    public void testGeoIndex() {

        Spec spec = new Spec(new Spec.GeoField("bar", Encoders.Geohash.PRECISION_4KM),
                            Spec.prefix("foo", false));
        SimpleIndex idx = new SimpleIndex("redis://localhost:6379", "myindex", spec);
        idx.drop();

        Document[] docs = new Document[] {
                new Document("doc1").set("foo", "hello world").set("bar", new Double[]{32.0667, 34.8000}),
                new Document("doc2").set("foo", "rello werld").set("bar", new Double[]{32.0677, 34.8010}),
                new Document("doc3").set("foo", "jello world").set("bar", new Double[]{-32.0667, -34.8000}),
        };

        try {
            idx.index(docs);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        Query q = new Query("myindex");

        q.filterNear("bar", 32.0667, 34.8000);
        List<Index.Entry> ids = null;
        try {
            ids = idx.get(q);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        assertEquals(2, ids.size());

        q.filterPrefix("foo", "hell");
        try {
            ids = idx.get(q);

        } catch (IOException e) {
            fail(e.getMessage());
        }
        assertEquals(1, ids.size());
    }
}