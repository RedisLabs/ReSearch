package com.redislabs.research.redis;

import com.redislabs.research.Index;
import com.redislabs.research.Spec;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.net.URI;

/**
 * Created by dvirsky on 28/02/16.
 */
public abstract class BaseIndex implements Index {
    protected JedisPool pool;
    Spec spec;
    String name;

    public BaseIndex(String name, Spec spec, String redisURI) {
        this.name = name;
        this.spec = spec;
        pool = new JedisPool(new JedisPoolConfig(), URI.create(redisURI));
    }
}
