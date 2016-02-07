package com.redislabs.research.redis;

/**
 * Created by dvirsky on 07/02/16.
 */
public interface Encoder<T> {
    byte[] encode(T obj);
}
