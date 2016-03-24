package com.redislabs.research;

import java.io.EOFException;

/**
 * A DocumentStream is an interface that can be implemented by the user, in order to support indexing
 * documents from the user's data source.
 *
 * Example usages can be fetching documents from a redis database, or an external  API.
 *
 * The DocumentStream must return true on hasNext() as long as the stream will yield more documents.
 * You can throw and EOF error in next() to stop iteration.
 */
public interface DocumentStream {
    /**
     * Get the next document from the stream
     * @return a document object to be indexed.
     * @throws EOFException if next() has been called after you're done. Normally you should use hasNex()
     */
    Document next() throws EOFException;

    /**
     * Tell the engine whether there are more documents to read from the stream
     * @return true if we are not done
     */
    boolean hasNext();
}
