package com.redislabs.research.redis;

import com.matttproud.quantile.Estimator;
import com.matttproud.quantile.Quantile;
import com.redislabs.research.Index;
import redis.clients.jedis.*;
import com.redislabs.research.Document;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;
import com.redislabs.research.text.NaiveNormalizer;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.*;


/**
 * SimpleIndex is the basic, prefix or exact value index, withuot partitioning.
 * Partitioning is built by creating multiple instances of SimpleIndex for each partition
 */
public class SimpleIndex extends BaseIndex {


    public static final byte SEPARATOR = ':';

    public static class Factory implements IndexFactory {
        @Override
        public Index create(String name, Spec spec, String redisURI) {
            return new SimpleIndex(redisURI, name, spec);
        }
    }

    // a map of type to encoder
    private Map<String, Encoder> encoders = new HashMap<>();

    private Encoder scoreEncoder = new Encoders.Numeric();

    /**
     * Constructor
     *
     * @param redisURI the redis server we connect to
     * @param name     the index name
     * @param spec     index spec
     */
    public SimpleIndex(String redisURI, String name, Spec spec) {
        super(name, spec, redisURI);

        createEncoders(spec);

    }

    private void createEncoders(Spec spec) {

        for (Spec.Field f : spec.fields) {

            switch (f.type) {
                case Prefix:
                    encoders.put(f.name, new Encoders.Prefix(new NaiveNormalizer(), ((Spec.PrefixField) f).indexSuffixes));
                    break;
                case Numeric:
                    encoders.put(f.name, new Encoders.Numeric());
                    break;
                case Geo:
                    encoders.put(f.name, new Encoders.Geohash(((Spec.GeoField) f).precision));
                    break;
                default:
                    throw new RuntimeException("Invalid index type " + f.type.toString());
            }

        }


    }


    /**
     * index a set of coduments in the index. This is done with a single pipeline for speed
     *
     * @param docs a list of documents to be indexed
     */
    @Override
    public Boolean index(Document... docs) throws IOException {


        Jedis conn = pool.getResource();
        Pipeline pipe = conn.pipelined();
        List<byte[]> entries;
        for (Document doc : docs) {
            try {
                entries = encode(doc);
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }

            for (byte[] entry : entries) {
                pipe.zadd(name.getBytes(), 0, entry);
            }

        }
        pipe.sync();
        conn.close();


        return true;

    }

    /**
     * Takes the raw encoded query from the sorted set and extracts id and score by decoding it.
     * e.g. "world::?\xf0\x00\x00\x00\x00\x00\x00|doc1" --> Entry(id: doc1, score: 1.0)
     *
     * @param raw the raw entry in the sorted set
     * @return a new Entry object
     */
    Entry extractEntry(byte[] raw) {


        int idx = -1;
        int idIdx = -1;
        for (int i = 0; i < raw.length; i++) {
            if (raw[i] == SEPARATOR && i < raw.length - 1 && raw[i + 1] == SEPARATOR) {
                idx = i + 2;
                break;
            }
        }
        if (idx == -1) {
            throw new RuntimeException("Invalid id entry: " + new String(raw));
        }
        for (int i = idx; i < raw.length; i++) {
            if (raw[i] == SEPARATOR) {
                idIdx = i + 1;
                break;
            }
        }
        if (idIdx == -1) {
            throw new RuntimeException("Invalid id entry: " + new String(raw));
        }


        String id = new String(raw, idIdx, raw.length - idIdx);
        ByteBuffer bb = ByteBuffer.wrap(raw, idx, idIdx - idx - 1);

        double score = Float.intBitsToFloat(bb.getInt());

        return new Entry(id, score);

    }

    /**
     * Get ids of documents indexed in the index
     *
     * @param q the query to look for
     * @return a list of string ids
     * @throws IOException
     */
    @Override
    public List<Index.Entry> get(Query q) throws IOException {

        // extract the ids from the entries
        List<Entry> ids = new ArrayList<>(q.sort.limit);

        for (byte i =0 ; i < bestim.QUANTILES.length; i++) {
            // Get the redis ranges to look for
            Range rng = new Range(q, i);
            Set<byte[]> entries;

            // get the ids - either with limit or not
            try (Jedis conn = pool.getResource()) {
                if (q.sort != null && q.sort.limit != null && q.sort.offset != null) {
                    entries = conn.zrangeByLex(name.getBytes(), rng.from, rng.to, q.sort.offset, q.sort.limit);
                } else {
                    entries = conn.zrangeByLex(name.getBytes(), rng.from, rng.to);
                }
            }
            for (byte[] entry : entries) {
                //String se = new String(entry);
                ids.add(extractEntry(entry));
            }

            if (ids.size() >= q.sort.limit) {
                break;
            }
        }




        return ids;

    }

    /**
     * Delete document-ids from the index
     *
     * @param ids the list of ids to delete
     */
    @Override
    public Boolean delete(String... ids) {
        // @TODO not implemented
        return false;
    }

    /**
     * Drop the index completely
     */
    @Override
    public Boolean drop() {
        Jedis conn = pool.getResource();
        Boolean ret = conn.del(name) != 0;
        conn.close();
        return ret;
    }

    private BucketEstimator bestim = new BucketEstimator(BucketEstimator.DEFAULT_SAMPLE_RATE,
            BucketEstimator.DEFAULT_BUFFER_SIZE);
    /**
     * Encode a document's values into ZSET values to be indexed
     *
     * @param doc the cdocument to encode
     * @return the index entries for this document.
     * @throws IOException
     */
    List<byte[]> encode(Document doc) throws IOException {

        List<List<byte[]>> encoded = new ArrayList<>(spec.fields.size());
        bestim.sample(doc.getScore());
        for (Spec.Field field : spec.fields) {

            Object prop = doc.property(field.name);


            Encoder enc = encoders.get(field.name);
            if (enc == null) {
                throw new RuntimeException("Missing encoder for " + field.name);
            }

            List<byte[]> bytes;
            if (field.type == Spec.IndexingType.Prefix) {
                List<byte[]> tmpBytes = enc.encode(Objects.requireNonNull(prop));
                bytes = new ArrayList<>(tmpBytes.size());


                for (byte[] entry : tmpBytes) {
                    byte[] pentry = new byte[entry.length + 1];
                    pentry[0] = bestim.getBucket(doc.getScore());
                    System.arraycopy(entry, 0, pentry, 1, entry.length);
                    bytes.add(pentry);
                }
            } else {
                bytes = enc.encode(Objects.requireNonNull(prop));
            }

            encoded.add(bytes);

        }

        // now merge everything

        List<List<byte[]>> product = cartesianProduct(encoded);
        List<byte[]> ret = new ArrayList<>(product.size());

        for (List<byte[]> lst : product) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            for (byte[] bs : lst) {
                buf.write(bs);
                buf.write(SEPARATOR);
            }
            // append the getId to the entry
            buf.write(SEPARATOR);

            List<byte[]> sce = scoreEncoder.encode((Number)doc.getScore());
            buf.write(sce.get(0));
            buf.write(SEPARATOR);
            buf.write(doc.getId().getBytes());


            ret.add(buf.toByteArray());
        }


        return ret;

    }

    class BucketEstimator {
        private Estimator<Float> estimator;
        private Quantile[] quantiles;
        private float sampleRate;
        private int bufferSize;
        private int numSamples;

        //this is the streaming quantile estimator's buffer capacity
        public static final int DEFAULT_BUFFER_SIZE = 4096;

        // this is our sample rate, used only after we've reached the buffer size
        public static final float DEFAULT_SAMPLE_RATE = 0.05f;

        public BucketEstimator(float sampleRate, int bufferSize) {
            this.sampleRate = sampleRate;
            this.bufferSize =  bufferSize;
            this.numSamples = 0;

            quantiles = new Quantile[] {
                    new Quantile(0.50, 0.050),
                    new Quantile(0.90, 0.01),
                    new Quantile(0.95, 0.01),
                    new Quantile(0.99, 0.01)};

             estimator= new Estimator<>(bufferSize, quantiles);

        }

        public void sample(float score) {
            if (numSamples++ <= bufferSize || Math.random() <= sampleRate) {
                estimator.insert(score);
            }
        }

        private final double[] QUANTILES = {0.99, 0.95, 0.90, 0.5};
        public byte getBucket(float score) {

            byte b = 0;
            for (double q : QUANTILES) {
                if (score >= estimator.query(q)) {
                    return b;
                }
                b++;
            }
            return b;
        }
    }





    /// here for testing purposes
    Range getRange(Query q, byte bucket) throws IOException {
        return new Range(q, bucket);
    }

    /**
     * Range represents a lexicographical range
     */
    class Range {
        byte[] from;
        byte[] to;
        private byte scoreBucket;


        public Range(Query q, byte scoreBucket) throws IOException, RuntimeException {

            this.scoreBucket = scoreBucket;
            // we start with [ ... ( but we may change that later based on the filters
            boolean lowerInclusive = true;
            boolean upperInclusive = false;
            ByteArrayOutputStream frbuf = new ByteArrayOutputStream();
            frbuf.write('?');

            ByteArrayOutputStream tobuf = new ByteArrayOutputStream();
            tobuf.write('?');
            boolean cont = true;
            for (Spec.Field field : spec.fields) {
                if (!cont) {
                    break;
                }
                Query.Filter flt = null;
                for (Query.Filter f : q.filters) {
                    if (f.property.equals(field.name)) {
                        flt = f;
                        break;
                    }
                }

                if (flt == null) {
                    break;
                }

                Encoder enc = encoders.get(field.name);
                if (enc == null) {
                    throw new RuntimeException("No encoder for field type " + field.name);
                }

                switch (flt.op) {
                    case Equals:
                        encodeEqualRange(frbuf, tobuf, flt, enc, field.type);
                        break;
                    case Between:
                        encodeBetweenRange(frbuf, tobuf, flt, enc, field.type);
                        cont = false;
                        break;
                    case Prefix:
                        encodePrefixRange(frbuf, tobuf, flt, enc);
                        //we can't continue after a prefix
                        cont = false;
                        break;
                    case Near:
                        encodeNearRange(frbuf, tobuf, flt, enc);
                        break;
                    // TODO - implement those...
                    case Greater:
                        encodeGreaterRange(frbuf, tobuf, flt, enc, field.type);
                        lowerInclusive = false;
                        cont = false;
                        break;
                    case GreaterEquals:
                        encodeGreaterRange(frbuf, tobuf, flt, enc, field.type);
                        lowerInclusive = true;
                        cont = false;
                        break;
                    case Less:
                        encodeLessRange(frbuf, tobuf, flt, enc, field.type);
                        upperInclusive = false;
                        cont = false;
                        break;
                    case LessEqual:
                        encodeLessRange(frbuf, tobuf, flt, enc, field.type);
                        upperInclusive = true;
                        cont = false;
                        break;
                    case Radius:

                    default:
                        throw new RuntimeException("No way to encode range for filter op " + flt.op.toString());
                }


            }
            tobuf.write(255);

            from = frbuf.toByteArray();
            to = tobuf.toByteArray();

            from[0] = (byte) (lowerInclusive ? '[' : '(');
            to[0] = (byte) (upperInclusive ? '[' : '(');

        }


        private void encodeNearRange(ByteArrayOutputStream frbuf, ByteArrayOutputStream tobuf, Query.Filter flt, Encoder enc) throws IOException {
            List<byte[]> encoded;
            if (flt.values.length != 2 || !(flt.values instanceof Double[])) {
                throw new RuntimeException("Near filter accepts two doubles only!");
            }
            encoded = enc.encode(flt.values);
            tobuf.write(encoded.get(0));
            tobuf.write(SEPARATOR);
            frbuf.write(encoded.get(0));
            frbuf.write(SEPARATOR);
        }

        private void encodePrefixRange(ByteArrayOutputStream frbuf, ByteArrayOutputStream tobuf,
                                       Query.Filter flt, Encoder enc) throws IOException {

            if (flt.values.length != 1) {
                throw new RuntimeException("Only one value allowed for PREFIX filter");
            }
            byte[] encoded = (byte[]) enc.encode(flt.values[0]).get(0);


            frbuf.write(scoreBucket);

            frbuf.write(encoded);
            tobuf.write(scoreBucket);
            tobuf.write(encoded);
        }

        private void encodeBetweenRange(ByteArrayOutputStream frbuf, ByteArrayOutputStream tobuf, Query.Filter flt, Encoder enc, Spec.IndexingType type) throws IOException {
            List<byte[]> encoded;
            if (flt.values.length != 2) {
                throw new RuntimeException("Exactly two value allowed for BETWEEN filter");
            }
            encoded = enc.encode(flt.values[0]);

            if (type == Spec.IndexingType.Prefix) {
                frbuf.write(scoreBucket);
            }
            frbuf.write(encoded.get(0));
            frbuf.write(SEPARATOR);
            encoded = enc.encode(flt.values[1]);
            if (type == Spec.IndexingType.Prefix) {
                tobuf.write(scoreBucket);
            }
            tobuf.write(encoded.get(0));
            tobuf.write(SEPARATOR);
        }

        private void encodeEqualRange(ByteArrayOutputStream frbuf, ByteArrayOutputStream tobuf, Query.Filter flt, Encoder enc, Spec.IndexingType type) throws IOException {
            List<byte[]> encoded;
            if (flt.values.length != 1) {
                throw new RuntimeException("Only one value allowed for EQ filter");
            }
            encoded = enc.encode(flt.values[0]);
            if (type == Spec.IndexingType.Prefix) {
                tobuf.write(scoreBucket);
            }
            tobuf.write(encoded.get(0));
            tobuf.write(SEPARATOR);
            if (type == Spec.IndexingType.Prefix) {
                frbuf.write(scoreBucket);
            }
            frbuf.write(encoded.get(0));
            frbuf.write(SEPARATOR);
        }


        /**
         * Encode a lexical range for a GT filter. basically we encode the range as value - \xff\xff\xff\xff\xff
         *
         * @param frbuf the "from" range byte buffer
         * @param tobuf the "to" range byte buffer
         * @param flt the filter we are encoding
         * @param enc the encoder we are using
         * @param type
         * @throws IOException
         */
        private void encodeGreaterRange(ByteArrayOutputStream frbuf, ByteArrayOutputStream tobuf, Query.Filter flt, Encoder enc, Spec.IndexingType type) throws IOException {
            List<byte[]> encoded;
            if (flt.values.length != 1) {
                throw new RuntimeException("Exactly one value allowed for GT filter");
            }

            encoded = enc.encode(flt.values[0]);
            byte[] bs = encoded.get(0);
            if (type == Spec.IndexingType.Prefix) {
                frbuf.write(scoreBucket);
            }
            frbuf.write(bs);
            frbuf.write(SEPARATOR);
            // write 0xff * the size of the encoded value
            // TODO - do it properly for numeric types
            for (int n = 0; n < bs.length; n++) {
                bs[n] = (byte) 0xff;
            }
            if (type == Spec.IndexingType.Prefix) {
                tobuf.write(scoreBucket);
            }
            tobuf.write(bs);
            tobuf.write(SEPARATOR);
        }

        private void encodeLessRange(ByteArrayOutputStream frbuf, ByteArrayOutputStream tobuf, Query.Filter flt, Encoder enc, Spec.IndexingType type) throws IOException {
            List<byte[]> encoded;
            if (flt.values.length != 1) {
                throw new RuntimeException("Exactly one value allowed for GT filter");
            }
            encoded = enc.encode(flt.values[0]);
            byte[] bs = encoded.get(0);

            if (type == Spec.IndexingType.Prefix) {
                tobuf.write(scoreBucket);
            }
            tobuf.write(bs);
            tobuf.write(SEPARATOR);


            // write 0x00 * the size of the encoded value
            for (int n = 0; n < bs.length; n++) {
                bs[n] = (byte) 0x00;
            }
            if (type == Spec.IndexingType.Prefix) {
                frbuf.write(scoreBucket);
            }
            frbuf.write(bs);
            frbuf.write(SEPARATOR);
        }

    }


    // TODO - move this to some util
    public static <T> List<List<T>> cartesianProduct(List<List<T>> sets) {
        List<List<T>> ret = new ArrayList<>();

        for (List<T> set : sets) {
            if (ret.isEmpty()) {
                for (T t : set) {
                    List<T> tuple = new ArrayList<>();
                    tuple.add(t);
                    ret.add(tuple);
                }
            } else {
                List<List<T>> tmp = new ArrayList<>();

                for (List<T> subList : ret) {
                    for (T t : set) {
                        List<T> lst = new ArrayList<>();
                        lst.addAll(subList);
                        lst.add(t);
                        tmp.add(lst);
                    }
                }

                ret = tmp;
            }
        }

        return ret;
    }
}
