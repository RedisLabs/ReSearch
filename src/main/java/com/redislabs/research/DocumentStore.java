package com.redislabs.research;

import java.util.List;

/**
 * DocumentStore defines a simple key/value storage facility for documents indexed in the index.
 * This is not what indexes them, just the storage mechanism for complete documents
 */
public interface DocumentStore {
    /**
     * JSONStore a list of documents in the store
     * @param docs the documents to be stored
     */
    void store(Document ...docs);

    /**
     * Load documents from the store based on ids
     * @param ids the ids of the docs to load
     * @return the documents found in the store
     */
    List<Document> load(List<String> ids);

    /**
     * Delete documents from the store by getId
     * @param ids
     * @return the number of documents actually deleted
     */
    int delete(String ...ids);
}
