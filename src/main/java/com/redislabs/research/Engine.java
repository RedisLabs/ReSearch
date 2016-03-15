package com.redislabs.research;

import com.redislabs.research.errors.SearchException;
import redis.clients.jedis.exceptions.JedisAskDataException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dvirsky on 03/03/16.
 */
public class Engine {

    protected Map<String, Index> indexes;

    protected DocumentStore store;


    public Engine(DocumentStore store, Index ...indexes) {

        this.store = store;
        this.indexes = new HashMap<>(indexes.length);
        for (Index i : indexes) {
            this.indexes.put(i.id(), i);
        }
    }

    public void put(Document... docs) throws IOException {

        store.store(docs);

        for (Index idx : indexes.values()) {
            idx.index(docs);
        }

    }


    public List<Document> search(Query query) throws SearchException {

        Index idx = indexes.get(query.indexName);
        if (idx == null) {
            throw new SearchException("No such index " + query.indexName);
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

        try {
            return store.load(docIds);
        } catch (Exception e) {
            throw new SearchException(e);
        }

    }
}
