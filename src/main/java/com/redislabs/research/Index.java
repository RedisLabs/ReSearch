package com.redislabs.research;

import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.IOException;
import java.util.List;

/**
 * The basic interface for all indexes
 */
public interface Index {

    public class Entry implements Comparable<Entry> {
        public String id;
        public double score;
        public Entry(String id, double score) {
            this.id = id;
            this.score = score;
        }
        @Override
        public boolean equals(Object other) {
            return other instanceof Entry && ((Entry) other).id.equals(id);
        }


        @Override
        public int compareTo(Entry o) {
            return this.score == o.score ? 0 : (score < o.score ? -1 : 1);
        }
    }

    Boolean index(Document ...docs) throws IOException;
    List<Entry> get(Query q) throws IOException, InterruptedException;
    Boolean delete(String... ids);
    Boolean drop();
    String id();
}
