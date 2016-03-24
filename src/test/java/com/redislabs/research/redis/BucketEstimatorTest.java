package com.redislabs.research.redis;

import junit.framework.TestCase;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by dvirsky on 24/03/16.
 */
public class BucketEstimatorTest extends TestCase {

    public void testEstimator() {

        SimpleIndex.BucketEstimator be = new SimpleIndex.BucketEstimator(SimpleIndex.BucketEstimator.DEFAULT_SAMPLE_RATE, SimpleIndex.BucketEstimator.DEFAULT_BUFFER_SIZE);

        for (int i = 0; i < 10000; i++) {
            be.sample((float)Math.random());
        }
        assertEquals(0, be.getBucket(1));
        assertEquals(1, be.getBucket(0.99f));
        assertEquals(2, be.getBucket(0.96f));
        assertEquals(3, be.getBucket(0.51f));
        assertEquals(4, be.getBucket(0.0f));

    }

}