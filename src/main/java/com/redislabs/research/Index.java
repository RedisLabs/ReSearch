package com.redislabs.research;

import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.IOException;
import java.util.List;

/**
 * The basic interface for all indexes
 */
public interface Index {
    Boolean index(Document ...docs) throws IOException;
    List<String> get(Query q) throws IOException, InterruptedException;
    Boolean delete(String... ids);
    Boolean drop();
}
