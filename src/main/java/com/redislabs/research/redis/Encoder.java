package com.redislabs.research.redis;

import java.util.List;

/**
 * Created by dvirsky on 07/02/16.
 */
public interface Encoder<T> {
    List<byte[]> encode(T obj);
}
