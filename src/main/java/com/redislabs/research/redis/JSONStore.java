package com.redislabs.research.redis;

import com.google.gson.Gson;
import com.redislabs.research.Document;
import com.redislabs.research.DocumentStore;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dvirsky on 21/02/16.
 */
public class JSONStore implements DocumentStore {

    private final JedisPool pool;
    private final Gson gson;

    public JSONStore(String redisURI) {
        JedisPoolConfig conf = new JedisPoolConfig();
        conf.setMaxTotal(100);
        pool = new JedisPool(conf, URI.create(redisURI));
        gson = new Gson();

    }

    private String key(String id) {
        return "d:" + id;
    }

    @Override
    public void store(Document... docs) {

        Jedis conn = pool.getResource();
        Pipeline pipe = conn.pipelined();
        try {
            for (Document doc : docs) {

                String encoded = gson.toJson(doc);
                pipe.set(key(doc.getId()), encoded);

            }
            pipe.sync();
        } finally {
            conn.close();
        }
    }

    /**
     * load documents by ids
     *
     * @param ids the ids of the docs to load
     * @return a list of documents loaded
     */
    @Override
    public List<Document> load(List<String> ids) {

        List<Document> ret;

        String[] keys = new String[ids.size()];
        int i = 0;
        for (String id : ids) {
            keys[i] = key(id);
            i++;
        }

        List<String> objs;
        try (Jedis conn = pool.getResource()) {
            //decode rets
            objs = conn.mget(keys);
        }

        ret = new ArrayList<>(objs.size());
        for (String encoded : objs) {
            if (encoded == null || encoded.isEmpty()) {
                continue;
            }
            ret.add(gson.fromJson(encoded, Document.class));
        }


        return ret;

    }

    @Override
    public int delete(String... ids) {
        String[] keys = new String[ids.length];
        int i = 0;
        for (String id : ids) {
            keys[i] = key(id);
            i++;
        }


        try (Jedis conn = pool.getResource()) {
            return conn.del(keys).intValue();
        }


    }
}
