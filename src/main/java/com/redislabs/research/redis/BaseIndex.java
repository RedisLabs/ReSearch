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
    protected final String redisURI;
    protected JedisPool pool;
    Spec spec;
    String name;

    public BaseIndex(String name, Spec spec, String redisURI) {
        this.name = name;
        this.spec = spec;
        this.redisURI = redisURI.replace("redis://", "");
        JedisPoolConfig conf = new JedisPoolConfig();
        conf.setMaxTotal(500);
        conf.setTestOnBorrow(false);
        conf.setTestOnReturn(false);
        conf.setTestOnCreate(false);
        conf.setTestWhileIdle(false);
        conf.setMinEvictableIdleTimeMillis(60000);
        conf.setTimeBetweenEvictionRunsMillis(30000);
        conf.setNumTestsPerEvictionRun(-1);

        conf.setFairness(true);

        pool = new JedisPool(conf, URI.create(redisURI));
    }

    @Override
    public String id() {
        return name;
    }
}
