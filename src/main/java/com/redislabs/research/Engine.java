package com.redislabs.research;

import com.redislabs.research.errors.IndexNotFoundException;
import com.redislabs.research.errors.SearchException;
import redis.clients.jedis.exceptions.JedisAskDataException;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;

/**
 * Engine is a high level API encapsulating indexing, storing and retrieving document in a unified way.
 *
 * It's optional, you can use indexes directly, this layer is purely convenience
 */
public class Engine {

    protected Map<String, Index> indexes;

    protected DocumentStore store;



    /**
     * Create a new engine. Pass a document store and the indexes you want to use.
     */
    public Engine(DocumentStore store, Index ...indexes) {

        this.store = store;
        this.indexes = new HashMap<>(indexes.length);
        for (Index i : indexes) {
            this.indexes.put(i.id(), i);
        }
    }

    /**
     * Put save a list of documents into the store and indexes them in the engine's indexes
     * @param docs a list of a list of documents to index
     * @throws IOException
     */
    public void put(Document... docs) throws IOException {

        store.store(docs);

        index(docs);

    }

    /**
     * Indexes indexes the documents in all the engine's indexes, but DOES NOT save the documents to the store.
     * This should be used only if you're indexing another collection and not interested in fetching documents back from
     * the store
     * @param docs a list of documents to index
     * @throws IOException
     */
    public void index(Document ...docs) throws IOException {
        for (Index idx : indexes.values()) {
            idx.index(docs);
        }
    }

    /**
     * Iteratively index a DocumentStream - without saving the documents to the store, just putting them in the engine's
     * indexes.
     * @param stream a DocumentStream providing documents to index
     * @param chunkSize the size of the document chunk we want to flush as one transaction to redis.
     *                  This should be no more than a few thousands
     * @throws IOException
     */
    public void index(DocumentStream stream, int chunkSize) throws IOException {

        handleStream(stream,chunkSize, false);
    }
    /**
     * Iteratively save index a DocumentStream - also saving the documents to the store
     * @param stream a DocumentStream providing documents to index
     * @param chunkSize the size of the document chunk we want to flush as one transaction to redis.
     *                  This should be no more than a few thousands
     * @throws IOException
     */
    public void put(DocumentStream stream, int chunkSize) throws IOException {
        handleStream(stream,chunkSize, true);
    }

    /**
     * Internal handling of doc stream - used for either indexing only or saving+indexing
     */
    private void handleStream(DocumentStream stream, int chunkSize, boolean doStore) throws IOException {


        Document[] docs = new Document[chunkSize];
        int i = 0;
        while (stream.hasNext()) {
            try {
                docs[i] = stream.next();
            } catch (EOFException e) { //valid case where next() has been called when hasNext() hasn't
                break;
            }
            i++;

            // evert <chunkSize> iterations - flush all documents
            if (i == chunkSize) {
                if (doStore) {
                    put(docs);
                } else {
                    index(docs);
                }
                i=0;
            }
        }
        // flush anything remaining
        if (i > 0) {
            docs = Arrays.copyOfRange(docs, 0, i);
            if (doStore) {
                put(docs);
            } else {
                index(docs);
            }
        }
    }

    /**
     * Search the engine based on the query parameters, but return only a list of ids.
     * This should be used if you are using an external database to store the entries and the library for
     * indexing only
     * @param query a search query to use
     * @return a list of ids matching the query parameters
     * @throws SearchException
     */
    public List<String> searchIds(Query query) throws SearchException {

        Index idx = indexes.get(query.indexName);
        if (idx == null) {
            throw new IndexNotFoundException(query.indexName);
        }

        List<String>docIds;
        List<Index.Entry> entries;
        try {
            entries = idx.get(query);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SearchException(e);
        }
        if (entries == null || entries.isEmpty()) {
            return null;
        }

        docIds = new ArrayList<>(entries.size());
        for (Index.Entry e : entries) {
            docIds.add(e.id);
        }
        return docIds;
    }

    /**
     * Search the engine using the criteria specified
     * @param query a search query to use
     * @return a list of documents matching the query, or null if none were found
     * @throws SearchException
     */
    public List<Document> search(Query query) throws SearchException {


        List<String> docIds = searchIds(query);
        if (docIds == null) {
            return null;
        }
        try {
            return store.load(docIds);
        } catch (Exception e) {
            throw new SearchException(e);
        }

    }

    /**
     * Drop an index by name
     * @param indexName the name of the index to drop
     */
    public void dropIndex(String indexName) {
        Index idx = indexes.get(indexName);
        if (idx == null) {
            throw new IndexNotFoundException(indexName);
        }

        idx.drop();
    }
}
