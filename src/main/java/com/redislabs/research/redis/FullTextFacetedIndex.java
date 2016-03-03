package com.redislabs.research.redis;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;
import ch.hsr.geohash.queries.GeoHashCircleQuery;
import com.redislabs.luascript.LuaScript;
import com.redislabs.research.Document;
import com.redislabs.research.Index;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;
import com.redislabs.research.dep.Hashids;
import com.redislabs.research.text.NaiveNormalizer;
import com.redislabs.research.text.Token;
import com.redislabs.research.text.Tokenizer;
import com.redislabs.research.text.WordTokenizer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ZParams;

import java.io.IOException;
import java.util.*;
import java.util.zip.CRC32;

/**
 * This is a full text index operating on intersection of faceted sorted sets
 *
 * Text fields are tokenized and inserted in
 */
public class FullTextFacetedIndex extends BaseIndex {


    public static class Factory implements IndexFactory {

        private Tokenizer tokenizer;
        public Factory(Tokenizer tokenizer) {
            this.tokenizer = tokenizer;
        }

        /**
         * Constructor with defaults
         */
        public Factory() {
            this(new WordTokenizer(new NaiveNormalizer()));
        }

        @Override
        public Index create(String name, Spec spec, String redisURI) throws IOException {
            return new FullTextFacetedIndex(redisURI, name, spec, tokenizer);
        }
    }


    private Tokenizer tokenizer;

    private LuaScript zrangeByScoreStore;

    public FullTextFacetedIndex(String redisURI, String name, Spec spec, Tokenizer tokenizer) throws IOException {
        super(name, spec, redisURI);
        this.tokenizer = tokenizer;

        initScripts(redisURI);
    }

    private void initScripts(String redisURI) throws IOException {
        zrangeByScoreStore = LuaScript.fromResource("lua/zrangebyscore_store.lua",redisURI);
    }

    @Override
    public Boolean index(Document... docs) throws IOException {

        Jedis conn = pool.getResource();
        Pipeline pipe = conn.pipelined();

        for (Document doc : docs) {
            try {
                for (Spec.Field field : spec.fields) {

                    switch (field.type) {
                        case FullText:
                            String text = (String) doc.property(field.name);
                            if (text != null && !text.isEmpty()) {
                                indexStringField(doc.getId(), doc.getScore(), text, pipe);
                            }
                            break;
                        case Numeric:

                            Number num = (Number)doc.property(field.name);
                            if (num != null) {
                                indexNumeric(field.name, doc.getId(), num, pipe);
                            }
                            break;
                        case Geo:
                            Spec.GeoField gf = (Spec.GeoField)field;
                            Double[] latlon = (Double[])doc.property(field.name);
                            if (latlon.length != 2) {
                                throw new RuntimeException("Invalid length for lat,lon pair");
                            }
                            indexGeoPoint(doc.getId(), latlon[0], latlon[1], gf.precision, pipe);
                            break;
                        default:
                            throw new RuntimeException("Unsuppported index spec type for " + field.name  + ": " + field.type.toString());
                    }

                }
            }catch (Exception ex) {
                ex.printStackTrace(System.err);
            }


        }
        try {
            pipe.sync();
        }finally {
            conn.close();
        }
        return true;
    }

    @Override
    public List<String> get(Query q) throws IOException, InterruptedException {

        QueryExecutionPlan qxp = new QueryExecutionPlan(q);

        return qxp.execute();
    }

    @Override
    public Boolean delete(String... ids) {
        return null;
    }

    @Override
    public Boolean drop() {
        return false;
    }

    private String tokenKey(String token) {
        return "f:"+name+ ":"+token;
    }

    private String fieldKey(String field) {
        return "k:"+name+ ":"+field;
    }

    private String geoKey(String geoHash) {
        return "g:"+name+ ":"+geoHash;
    }



    void indexNumeric(String fieldName, String docId, Number value, Pipeline pipe) {
        Jedis conn = null;
        if (pipe == null) {
            conn = pool.getResource();
            pipe = conn.pipelined();
        }

        pipe.zadd(fieldKey(fieldName), value.doubleValue(), docId);

        // only if the connection was not provided to us - commit everything
        if (conn != null) {
            pipe.sync();
            conn.close();
        }
    }

    void indexStringField(String docId, Double docScore, String text, Pipeline pipe) {
        Iterable<Token> tokens = tokenizer.tokenize(text);
        Jedis conn = null;
        if (pipe == null) {
            conn = pool.getResource();
            pipe = conn.pipelined();
        }


        for (Token tok : tokens) {
            pipe.zadd(tokenKey(tok.text), docScore*tok.frequency, docId);
        }

        // only if the connection was not provided to us - commit everything
        if (conn != null) {
            pipe.sync();
            conn.close();
        }
    }

    void indexGeoPoint(String docId, Double lat, Double lon, int precision, Pipeline pipe) {
        GeoHash coarse = GeoHash.withCharacterPrecision(lat, lon, precision);
        GeoHash fine = GeoHash.withBitPrecision(lat, lon, 53);

        pipe.zadd(geoKey(coarse.toBase32()), fine.longValue(), docId);

    }

    private class QueryExecutionPlan {

        Step root;
        Query query;

        public QueryExecutionPlan(Query query) {

            root = new IntersectStep();
            this.query = query;
            for (Query.Filter flt : query.filters) {
                Spec.Field field = null;
                for (Spec.Field f : spec.fields ) {
                    if (f.name.equals(flt.property)) {
                        field = f;
                        break;
                    }
                }

                switch (field.type) {

                    case FullText:
                        if (flt.op != Query.Op.Matches) {
                            throw new RuntimeException("Only MATCH filters are allowed for full text filters");
                        }
                        if (flt.values.length != 1) {
                            throw new RuntimeException("Only one string value is allowed for fulltext filters");
                        }
                        root.addChild(new IntersectStep((String)flt.values[0]));
                        break;
                    case Numeric:
                        Double min = null, max = null;
                        switch (flt.op) {
                            case Between:
                                min = (Double) flt.values[0];
                                max = (Double) flt.values[1];
                                break;
                            case Greater:
                                min = (Double) flt.values[0];
                                break;
                            case Equals:
                                min = (Double) flt.values[0];
                                max = min;
                                break;
                            default:
                                throw new RuntimeException("Unsupported numeric op!");


                        }

                        root = new RangeIntersectStep(flt.property, min,max, root);
                        break;
                    case Geo:
                        if (flt.op != Query.Op.Radius) {
                            throw new RuntimeException("Unsupported op for geo index: " + flt.op.toString());
                        }
                        double lat = (double)flt.values[0];
                        double lon = (double)flt.values[1];
                        double radius = (double)flt.values[2];
                        root.addChild(new GeoRadiusStep(lat, lon, radius, ((Spec.GeoField)field).precision));
                        break;
                    default:
                        throw new RuntimeException("Unsupported field type: " + field.type.toString());


                }
            }
            //System.out.println(root.toString());

        }

        public List<String> execute() {
            Jedis conn = pool.getResource();
            Pipeline pipe = conn.pipelined();

            String tmpKey = root.execute(pipe);


            pipe.zrevrange(tmpKey, query.sort.offset, query.sort.offset + query.sort.limit);

            List<Object> res = pipe.syncAndReturnAll();
            conn.close();


            Set<String> ids = (Set<String>) res.get(res.size()-1);

            return new ArrayList<>(ids);
        }


    }


    private abstract class Step {

        List<Step> children;


        @Override
        public String toString() {
            return toString("");
        }

        public String toString(String tabs) {

            String childrenstr = "";

            for (Step c : children) {
                childrenstr += c.toString(tabs + "  ") + ",\n";
            }
            return String.format("%s%s {\n%s%s}", tabs, getClass().getSimpleName(), childrenstr, tabs );
        }


        public Step(Step ...children) {
            this.children = new LinkedList<>(Arrays.asList(children));
        }

        String[] executeChildren(Pipeline pipe) {
            String[] keys = new String[children.size()];

            for (int i = 0; i < children.size(); i++) {
                keys[i] = children.get(i).execute(pipe);

            }
            return keys;
        }

        protected void addChild(Step child) {
            children.add(child);

        }

        abstract String execute(Pipeline pipe);

        protected String makeTmpKey(String ...subKeys) {

            Hashids hids = new Hashids();
            if (subKeys.length == 0) {
                return hids.encrypt(UUID.randomUUID().getLeastSignificantBits());
            }

            CRC32 crc = new CRC32();
            for (String key : subKeys) {
                crc.update(key.getBytes());
            }
            return hids.encrypt(crc.getValue());
        }

    }

    private class IntersectStep extends Step {


        public IntersectStep(Step ...children) {
            super(children);
        }

        public IntersectStep(String text) {
            super();

            for (Token tok : tokenizer.tokenize(text)) {
                addChild(new TokenStep(tok));
            }
        }

        @Override
        String execute(Pipeline pipe) {

            String[] tmpKeys = executeChildren(pipe);

            String tk = makeTmpKey(tmpKeys);
            pipe.zinterstore(tk, new ZParams(), tmpKeys);
            pipe.expire(tk, 60);
            return tk;
        }
    }

    private class RangeIntersectStep extends Step {

        private String fieldName;
        private Double min;
        private Double max;

        public RangeIntersectStep(String fieldName, Double min, Double max, Step ...children) {
            super(children);
            this.fieldName = fieldName;
            this.min = min;
            this.max = max;
        }



        @Override
        String execute(Pipeline pipe) {

            String tk = fieldKey(fieldName);
            if (!children.isEmpty()) {
                String[] tmpKeys = executeChildren(pipe);

                tk = makeTmpKey(tmpKeys);

                pipe.zinterstore(tk, new ZParams().weightsByDouble(1, 0), fieldKey(fieldName), tmpKeys[0]);
                pipe.expire(tk, 60);
            }

            String tk_ = tk + "_";
            pipe.del(tk_);

            zrangeByScoreStore.execute(pipe, 2, tk, tk_,
                    min != null ? min.toString() : "-inf",
                    max != null ? max.toString() : "+inf");
            pipe.expire(tk_, 60);
            return tk_;
        }




    }

    private class UnionStep extends Step {

        public UnionStep(Step ...children) {
            super(children);
        }

        @Override
        String execute(Pipeline pipe) {

            String[] tmpKeys = executeChildren(pipe);

            String tk = makeTmpKey(tmpKeys);
            pipe.zunionstore(tk, new ZParams(), tmpKeys);
            pipe.expire(tk, 60);
            return tk;
        }
    }

    private class TokenStep extends Step {
        private final Token token;

        @Override
        public String toString(String tabs) {
            return String.format("%sTokenStep('%s')", tabs, token.text);
        }
        public TokenStep(Token tok) {
            super();
            this.token = tok;
        }
        @Override
        String execute(Pipeline pipe) {
            return tokenKey(token.text);
        }


    }


    private class GeoRadiusStep extends Step {

        private final double lat;
        private final double lon;
        private final double radius;
        private final int precision;


        @Override
        public String toString(String tabs) {
            return String.format("%sGeoRadiusStep(%.03f,%.03f => %.02fm))", tabs, lat, lon, radius);
        }
        public GeoRadiusStep(double lat, double lon, double radius, int precision) {
            super();
            this.lat = lat;
            this.lon = lon;
            this.radius = radius;
            this.precision = precision;
        }


        @Override
        String execute(Pipeline pipe) {

            // Since the circle query doesn't have pre-defined precision, we need to calculate all the
            // relevant geohashes in our precision manually
            Set<String> hashKeys = getSearchHashes();


            // now let's union them
            String[] keysArr = hashKeys.toArray(new String[hashKeys.size()]);
            double[] scoresArr = new double[hashKeys.size()];//all weights are zero
            String tmpKey = makeTmpKey(keysArr);

            pipe.zunionstore(tmpKey, new ZParams().aggregate(ZParams.Aggregate.MAX)
                    .weightsByDouble(scoresArr), keysArr);
            return tmpKey;


        }

        /**
         * Calculate the geo hash cells we need to query in order to cover all points within this query
         * @return
         */
        private Set<String> getSearchHashes() {
            GeoHashCircleQuery q = new GeoHashCircleQuery(new WGS84Point(lat, lon), radius);

            // Since the circle query doesn't have pre-defined precision, we need to calculate all the
            // relevant geohashes in our precision manually
            GeoHash center = GeoHash.withCharacterPrecision(lat, lon, precision);

            Set<String> hashKeys = new HashSet<>(8);
            Set<GeoHash> seen = new HashSet<>(8);
            Queue<GeoHash> candidates = new LinkedList<>();

            candidates.add(center);

            while (!candidates.isEmpty()) {
                GeoHash gh = candidates.remove();
                hashKeys.add(geoKey(gh.toBase32()));

                GeoHash[] neighbors = gh.getAdjacent();
                for (GeoHash neighbor : neighbors) {
                    if (seen.add(neighbor) && q.contains(neighbor)) {
                            candidates.add(neighbor);
                        }
                    }

            }
            return hashKeys;
        }


    }

}
