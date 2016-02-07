package com.redislabs.research;

import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.List;

/**
 * Created by dvirsky on 07/02/16.
 */
public interface Index {
    Boolean index(Document[] docs);
    List<String> get(Query q);
    Boolean delete(String... ids);
    Boolean drop();
}
