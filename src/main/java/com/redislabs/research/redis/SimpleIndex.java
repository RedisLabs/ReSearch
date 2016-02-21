package com.redislabs.research.redis;
import redis.clients.jedis.*;
import com.redislabs.research.Document;
import com.redislabs.research.Index;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;
import com.redislabs.research.text.NaiveNormalizer;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * SimpleIndex is the basic, prefix or exact value index, withuot partitioning.
 * Partitioning is built by creating multiple instances of SimpleIndex for each partition
 */
public class SimpleIndex implements Index {


    Spec spec;
    String name;
    private JedisPool pool;

    // a map of type to encoder
    private static final Map<Spec.IndexingType, Encoder> encoders = new HashMap<Spec.IndexingType, Encoder>(){{
        put(Spec.IndexingType.Prefix, new Encoders.Prefix(new NaiveNormalizer(), false));
        put(Spec.IndexingType.Numeric, new Encoders.Numeric());
    }};


    /**
     * Constructor
     * @param redisURI the redis server we connect to
     * @param name the index name
     * @param spec index spec
     */
    public SimpleIndex(String redisURI, String name, Spec spec) {

        this.spec = spec;
        this.name = name;
        pool = new JedisPool(new JedisPoolConfig(), URI.create(redisURI));

    }


    /**
     * index a set of coduments in the index. This is done with a single pipeline for speed
     * @param docs a list of documents to be indexed
     * @return
     */
    @Override
    public Boolean index(Document ...docs) throws IOException {


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
                pipe.zadd(name, 0, new String(entry));
            }

        }
        pipe.sync();
        conn.close();


        return true;

    }

    /**
     * Get ids of documents indexed in the index
     * @param q the query to look for
     * @return a list of string ids
     * @throws IOException
     */
    @Override
    public List<String> get(Query q) throws IOException {

        // Get the redis ranges to look for
        Range rng = new Range(q);
        Jedis conn = pool.getResource();
        Set<String> entries;

        // get the ids - either with limit or not
        if (q.sort != null && q.sort.limit != null && q.sort.offset != null) {
            entries = conn.zrangeByLex(name, new String(rng.from), new String(rng.to), q.sort.offset, q.sort.limit);
        } else {
            entries = conn.zrangeByLex(name, new String(rng.from), new String(rng.to));
        }

        // extract the ids from the entries
        List<String>ids = new ArrayList<>(entries.size());
        for (String entry : entries) {
            ids.add(entry.substring(entry.lastIndexOf("||") + 2));
        }

        conn.close();
        return ids;

    }

    /**
     * Delete document-ids from the index
     * @param ids the list of ids to delete
     * @return
     * @TODO not implemented
     */
    @Override
    public Boolean delete(String... ids) {
        return null;
    }

    /**
     * Drop the index completely
     * @return
     */
    @Override
    public Boolean drop() {
        Jedis conn = pool.getResource();
        Boolean ret = conn.del(name) != 0;
        conn.close();
        return ret;
    }

    /**
     * Encode a document's values into ZSET values to be indexed
     * @param doc
     * @return
     * @throws IOException
     */
    List<byte[]> encode(Document doc) throws IOException {

        List<List<byte[]>> encoded = new ArrayList<>(spec.fields.size());
        for (Spec.Field field : spec.fields) {

            Object prop = doc.property(field.name);
            if (prop == null) {
                throw new RuntimeException("missing property in document: " + field.name);
            }

            Encoder enc = encoders.get(field.type);
            if (enc == null) {
                throw new RuntimeException("Missing encoder type " + field.type.toString());
            }

            encoded.add(enc.encode(prop));

        }

        // now merge everything

        List<List<byte[]>> product = cartesianProduct(encoded);
        List<byte[]> ret = new ArrayList<>(product.size());

        for (List<byte[]> lst : product) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();


            for (byte[] bs : lst) {


                buf.write(bs);
                buf.write('|');

            }
            // append the id to the entry
            buf.write('|');
            buf.write(doc.id().getBytes());
            ret.add(buf.toByteArray());
        }


        return ret;

    }


    /// here for testing purposes
    Range getRange(Query q) throws IOException {
        return new Range(q);
    }
    /**
     * Range represents a lexicographical range
     */
    class Range {
        byte []from;
        byte []to;


        public Range(Query q) throws IOException, RuntimeException {

            ByteArrayOutputStream frbuf = new ByteArrayOutputStream();
            frbuf.write('[');
            ByteArrayOutputStream tobuf = new ByteArrayOutputStream();
            tobuf.write('(');
            boolean cont = true;
            for (Spec.Field field : spec.fields) {
                if (!cont) {
                    break;
                }
                Query.Filter flt = null;
                for (Query.Filter f : q.filters) {
                    if (f.property == field.name) {
                        flt = f;
                        break;
                    }
                }

                if (flt == null) {
                    break;
                }

                Encoder enc = encoders.get(field.type);
                if (enc == null) {
                    throw new RuntimeException("No encoder for field type "+field.type.toString());
                }
                List<byte[]> encoded;
                switch (flt.op) {
                    case Equals:
                        if (flt.values.length != 1) {
                            throw new RuntimeException("Only one value allowed for EQ filter");
                        }
                        encoded = enc.encode(flt.values[0]);
                        tobuf.write(encoded.get(0));
                        tobuf.write('|');
                        frbuf.write(encoded.get(0));
                        frbuf.write('|');
                        break;
                    case Between:
                        if (flt.values.length != 2) {
                            throw new RuntimeException("Exactly two value allowed for BETWEEN filter");
                        }
                        encoded = enc.encode(flt.values[0]);
                        frbuf.write(encoded.get(0));
                        frbuf.write('|');
                        encoded = enc.encode(flt.values[1]);
                        tobuf.write(encoded.get(0));
                        tobuf.write('|');
                        break;
                    case Prefix:
                        if (flt.values.length != 1) {
                            throw new RuntimeException("Only one value allowed for PREFIX filter");
                        }
                        encoded = enc.encode(flt.values[0]);
                        tobuf.write(encoded.get(0));
                        frbuf.write(encoded.get(0));

                        //we can't continue after a prefix
                        cont = false;
                        break;

                    // TODO - implement those...
                    case Greater:
                    case GreaterEquals:
                    case Less:
                    case LessEqual:
                    case Radius:

                    default:
                        throw new RuntimeException("No way to encode range for filter op " + flt.op.toString());
                }


            }
            tobuf.write(255);

            from = frbuf.toByteArray();
            to = tobuf.toByteArray();

        }
    }



    // TODO - move this to some util
    public static <T> List<List<T>> cartesianProduct(List<List<T>> sets) {
        List<List<T>> ret = new ArrayList<>();

        for (List<T> set : sets) {
            if (ret.isEmpty()) {
                for (T t : set) {
                   List<T> tuple = new ArrayList<T>();
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
