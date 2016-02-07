package com.redislabs.research;

import java.util.List;

/**
 * Created by dvirsky on 07/02/16.
 */
public class RedisIndex implements Index {
    @Override
    public Boolean index(Document[] docs) {
        return null;
    }

    @Override
    public List<String> get(Query q) {
        return null;
    }

    @Override
    public Boolean delete(String... ids) {
        return null;
    }

    @Override
    public Boolean drop() {
        return null;
    }
}
