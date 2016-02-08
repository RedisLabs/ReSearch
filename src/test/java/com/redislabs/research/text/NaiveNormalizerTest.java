package com.redislabs.research.text;

import junit.framework.TestCase;

/**
 * Created by dvirsky on 08/02/16.
 */
public class NaiveNormalizerTest extends TestCase {
    public void testNormalizer() {

        NaiveNormalizer n = new NaiveNormalizer();

        String norm = n.normalize("  Hello.. .World,,,,.,.");

        assertEquals("hello world", norm);

        assertEquals("שלום שלום שלום", n.normalize("שלום   ]- --  שלום שלום!"));

        assertEquals("el nino", n.normalize("   El-Niño"));



    }
}