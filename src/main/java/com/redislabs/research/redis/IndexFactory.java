package com.redislabs.research.redis;

import com.redislabs.research.Index;
import com.redislabs.research.Spec;

import java.io.IOException;

/**
 * Created by dvirsky on 03/03/16.
 */
public interface IndexFactory {

    Index create(String name, Spec spec, String redisURI) throws IOException;
}
