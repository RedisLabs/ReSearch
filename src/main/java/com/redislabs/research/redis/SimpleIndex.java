package com.redislabs.research.redis;
import redis.clients.jedis.*;
import com.redislabs.research.Document;
import com.redislabs.research.Index;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;
import com.redislabs.research.text.NaiveNormalizer;
import redis.clients.util.Pool;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by dvirsky on 07/02/16.
 */
public class SimpleIndex implements Index {


    Spec spec;
    String name;
    private JedisPool pool;

    // a map of type to encoder
    private static final Map<Spec.IndexingType, Encoder> encoders = new HashMap<Spec.IndexingType, Encoder>(){{
        put(Spec.IndexingType.Prefix, new Encoders.Prefix(new NaiveNormalizer(), true));
        put(Spec.IndexingType.Numeric, new Encoders.Numeric());
    }};






    public SimpleIndex(String redisURI, String name, Spec spec) {

        this.spec = spec;
        this.name = name;
        pool = new JedisPool(new JedisPoolConfig(), "localhost");

    }



    @Override
    public Boolean index(Document ...docs) {


        Pipeline pipe = pool.getResource().pipelined();
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

        return true;

    }

    @Override
    public List<String> get(Query q) {
        return null;
    }

    @Override
    public Boolean delete(String... ids) {
        return null;
    }

    @Override
    public Boolean drop() {
        return null;
    }

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
            buf.write(doc.id().getBytes());
            ret.add(buf.toByteArray());
        }


        return ret;

    }

    // TODO - move this to some util
    public static <T> List<List<T>> cartesianProduct(List<List<T>> sets) {
        List<List<T>> tuples = new ArrayList<>();

        for (List<T> set : sets) {
            if (tuples.isEmpty()) {
                for (T t : set) {
                   List<T> tuple = new ArrayList<T>();
                    tuple.add(t);
                    tuples.add(tuple);
                }
            } else {
                List<List<T>> newTuples = new ArrayList<>();

                for (List<T> subTuple : tuples) {
                    for (T t : set) {
                        List<T> tuple = new ArrayList<>();
                        tuple.addAll(subTuple);
                        tuple.add(t);
                        newTuples.add(tuple);
                    }
                }

                tuples = newTuples;
            }
        }

        return tuples;
    }
}
