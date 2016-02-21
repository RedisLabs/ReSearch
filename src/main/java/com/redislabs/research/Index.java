package com.redislabs.research;

import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.IOException;
import java.util.List;

/**
 * Created by dvirsky on 07/02/16.
 */
public interface Index {
    Boolean index(Document ...docs);
    List<String> get(Query q) throws IOException;
    Boolean delete(String... ids);
    Boolean drop();
}
