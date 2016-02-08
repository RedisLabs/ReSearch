package com.redislabs.research.redis;

import com.redislabs.research.text.NaiveNormalizer;
import com.redislabs.research.text.TextNormalizer;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dvirsky on 07/02/16.
 */
public class Encoders {

    /**
     * Numeric is the encoder that encodes numeric types in big-endian, lexicographical order
     */
    public static class Numeric implements Encoder<Number> {

        @Override
        public List<byte[]> encode(Number num) {

            long val = 0;
            ByteBuffer buf = ByteBuffer.allocate(Long.SIZE/Byte.SIZE);
            buf.order(ByteOrder.BIG_ENDIAN);
            if (num instanceof Double) {
                // TODO - this is wrong for negatives, fix this
                buf.putDouble((Double) num);
            } else if (num instanceof Float) {
                buf.putDouble((double) (float) num);
            } else if (num instanceof Integer) {
                buf.putLong(num.longValue());
            } else if (num instanceof Long) {
                buf.putLong((Long) num);
            }


            ArrayList ret = new ArrayList<>(1);
            ret.add(buf.array());
            return ret;

        }
    }


    public static class Prefix implements Encoder<String> {

        private TextNormalizer normalizer = new NaiveNormalizer();
        private  boolean extractSuffixes;
        public Prefix(TextNormalizer normalizer, boolean suffixes) {
            this.normalizer = normalizer;
            this.extractSuffixes = suffixes;

        }

        @Override
        public List<byte[]> encode(String s) {

            ArrayList<byte[]> ret = new ArrayList<>(2);
            String normalized = normalizer.normalize(s);
            ret.add(normalized.getBytes());

            // optionally find all suffixes that begin with a space and encode them as well
            if (extractSuffixes && normalized.indexOf(' ') > 0) {


                int index = normalized.indexOf(' ');
                while (index > 0 && index < normalized.length()) {

                    ret.add(normalized.substring(index+1).getBytes());
                    index = normalized.indexOf(' ', index+1);
                }
            }
            return ret;
        }
    }

}
