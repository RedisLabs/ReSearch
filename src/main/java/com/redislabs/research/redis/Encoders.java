package com.redislabs.research.redis;

import ch.hsr.geohash.GeoHash;
import com.redislabs.research.text.NaiveNormalizer;
import com.redislabs.research.text.TextNormalizer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
                buf.putLong(Double.doubleToRawLongBits((Double) num));
            } else if (num instanceof Float) {
                // we replace the buffer with a buffer for floats only
                buf = ByteBuffer.allocate(Integer.SIZE/Byte.SIZE);
                buf.order(ByteOrder.BIG_ENDIAN);
                buf.putInt(Float.floatToIntBits((Float) num));
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


    public static class Geohash implements Encoder<Double[]> {


        public final static int PRECISION_156KM = 3;
        public final static int PRECISION_40KM = 4;
        public final static int PRECISION_4KM = 5;
        public final static int PRECISION_1KM = 6;
        public final static int PRECISION_150M = 7;

        private int precision;

        public Geohash(int precision) {
            this.precision = precision;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        @Override
        public List<byte[]> encode(Double[] latlon) {
            ArrayList<byte[]> ret = new ArrayList<>(1);
            try {
                GeoHash h = GeoHash.withCharacterPrecision(latlon[0], latlon[1], precision);
                ret.add(h.toBase32().getBytes());
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            return ret;

        }
    }

}
