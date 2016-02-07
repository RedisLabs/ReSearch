package com.redislabs.research.redis;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by dvirsky on 07/02/16.
 */
public class Encoders {

    public static class Numeric implements Encoder<Number> {

        @Override
        public byte[] encode(Number num) {

            long val = 0;
            ByteBuffer buf = ByteBuffer.allocate(Long.SIZE/Byte.SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);
            if (num instanceof Double) {
                buf.putDouble((Double) num);

            } else if (num instanceof Float) {
                buf.putDouble((double) (float) num);
            } else if (num instanceof Integer) {
                buf.putLong(num.longValue());
            } else if (num instanceof Long) {
                buf.putLong((Long) num);
            }


            return buf.array();

        }
    }

}
