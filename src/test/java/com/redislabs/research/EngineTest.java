package com.redislabs.research;

import com.redislabs.research.redis.JSONStore;
import com.redislabs.research.redis.PartitionedIndex;
import junit.framework.TestCase;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * Created by dvirsky on 24/03/16.
 */
public class EngineTest extends TestCase {

    private Engine engine;

    private Document[] documents = new Document[] {
        new Document("doc1").setScore(1.0f).set("foo", "hello world").set("bar", Math.PI),
        new Document("doc2").setScore(3.0f).set("foo", "hello werld").set("bar", Math.PI+1),
        new Document("doc3").setScore(2.0f).set("foo", "jello world").set("bar", Math.PI-1),
    };


    @Override
    public void setUp() throws IOException {

        Spec spec = new Spec(Spec.prefix("foo", true));

        engine = new Engine(new JSONStore("redis://localhost:6379"),
                PartitionedIndex.newSimple("myidx", spec, 3, 500, 3, "redis://localhost:6379")
        );


    }


    private void assertAllDocsFound() {

        for (Document doc : documents) {

            Query  q = new Query("myidx").filterEquals("foo", doc.property("foo"));
            List<String> ids = engine.searchIds(q);
            assertEquals(1, ids.size());
            assertEquals(doc.getId(), ids.get(0));

            List<Document> ds = engine.search(q);
            assertEquals(1, ds.size());
            assertEquals(doc.getId(), ds.get(0).getId());

        }
    }

    public void testIndexing() {

        try {
            engine.put(documents);
            assertAllDocsFound();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            engine.dropIndex("myidx");
        }

    }


    class DocStream implements  DocumentStream {

        public int i = 0;
        @Override
        public Document next() throws EOFException {
            if (i >= documents.length) {
                throw new EOFException();
            }
            return documents[i++];
        }

        @Override
        public boolean hasNext() {
            //this condition is wrong on purpose - to test the EOFException handling
            return i<=documents.length;
        }
    };

    public void testDocumentStream() {

        DocStream ds = new DocStream();

        try {
            engine.put(ds, 2);
            assertEquals(3, ds.i);
            assertAllDocsFound();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }finally {
           engine.dropIndex("myidx");
        }

    }



}