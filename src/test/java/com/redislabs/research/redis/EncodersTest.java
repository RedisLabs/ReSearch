package com.redislabs.research.redis;

import com.sun.org.apache.xerces.internal.impl.dv.util.HexBin;
import junit.framework.TestCase;

/**
 * Created by dvirsky on 07/02/16.
 */
public class EncodersTest extends TestCase {

    public void testNumericEncoder() {


        Encoders.Numeric enc = new Encoders.Numeric();
        byte []bs = enc.encode((3.4456));
        System.out.print(HexBin.encode(bs));


    }
    //c00b9096bb98c7e3
}