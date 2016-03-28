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
import com.redislabs.research.text.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.zip.CRC32;

/**
 * This is a full text index operating on intersection of faceted sorted sets
 * <p/>
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
            this(new WordTokenizer(new NaiveNormalizer(), false, null));
        }

        @Override
        public Index create(String name, Spec spec, String redisURI) throws IOException {
            return new FullTextFacetedIndex(redisURI, name, spec, tokenizer);
        }
    }


    private Tokenizer tokenizer;

    private LuaScript zrangeByScoreStore;


    private LuaScript intersectTokens;

    public FullTextFacetedIndex(String redisURI, String name, Spec spec, Tokenizer tokenizer) throws IOException {
        super(name, spec, redisURI);
        this.tokenizer = tokenizer;

        initScripts(redisURI);
    }

    private void initScripts(String redisURI) throws IOException {
        zrangeByScoreStore = LuaScript.fromSource("local tbl = redis.call('ZRANGEBYSCORE', KEYS[1],  ARGV[1], ARGV[2], 'WITHSCORES')\n" +
                "\n" +
                "for i,_ in ipairs(tbl) do\n" +
                "    if i % 2 == 1 then\n" +
                "        tbl[i], tbl[i+1] =tbl[i+1], tbl[i]\n" +
                "    end\n" +
                "end\n" +
                "redis.call('ZADD', KEYS[2], unpack(tbl))", redisURI);

        intersectTokens = LuaScript.fromSource("local tbl = {}\n" +
                "local numKeys = table.getn(ARGV)\n" +
                "\n" +
                "-- we call ZCARD to get the number of docs the word appears in \n" +
                "for i,elem in ipairs(ARGV) do\n" +
                "    tbl[i] = ARGV[i]\n" +
                "    tbl[i+numKeys+1] = math.log(1000000/(1+redis.call('ZCARD', elem)))\n" +
                "end\n" +
                "tbl[numKeys+1] = 'WEIGHTS'\n" +
                "table.insert(tbl, 'AGGREGATE')\n" +
                "table.insert(tbl, 'SUM')\n" +
                "\n" +
                "local rc = redis.call('ZINTERSTORE', KEYS[1], numKeys, unpack(tbl))\n" +
                "redis.expire(KEYS[1], 60)\n" +
                "return redis.status_reply(rc)\n", redisURI);
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

                            indexFulltextFields((Spec.FulltextField) field, doc, pipe);
                            break;
                        case Numeric:

                            Number num = (Number) doc.property(field.name);
                            if (num != null) {
                                indexNumeric(field.name, doc.getId(), num, pipe);
                            }
                            break;
                        case Geo:
                            Spec.GeoField gf = (Spec.GeoField) field;
                            Double[] latlon = (Double[]) doc.property(field.name);
                            if (latlon.length != 2) {
                                throw new RuntimeException("Invalid length for lat,lon pair");
                            }
                            indexGeoPoint(doc.getId(), latlon[0], latlon[1], gf.precision, pipe);
                            break;
                        default:
                            throw new RuntimeException("Unsuppported index spec type for " + field.name + ": " + field.type.toString());
                    }

                }
            } catch (Exception ex) {
                ex.printStackTrace(System.err);
            }


        }
        try {
            pipe.sync();
        } finally {
            conn.close();
        }
        return true;
    }

    @Override
    public List<Index.Entry> get(Query q) throws IOException, InterruptedException {

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
        return "f:" + name + ":" + token;
    }

    private String fieldKey(String field) {
        return "k:" + name + ":" + field;
    }

    private String geoKey(String geoHash) {
        return "g:" + name + ":" + geoHash;
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

    void indexFulltextFields(Spec.FulltextField spec, Document doc, Pipeline pipe) {

        Jedis conn = null;
        if (pipe == null) {
            conn = pool.getResource();
            pipe = conn.pipelined();
        }

        TokenSet mergedTokens = new TokenSet();

        for (Map.Entry<String, Double> entry : spec.fields.entrySet()) {

            if (doc.hasProperty(entry.getKey())) {

                Object p = doc.property(entry.getKey());
                if (p != null && p instanceof String) {
                    List<Token> tokens = tokenizer.tokenize((String)p);
                    mergedTokens.addAll(tokens, entry.getValue());
                }
            }

        }
        mergedTokens.normalize(mergedTokens.getMaxFreq());

        for (Token tok : mergedTokens.values()) {
            pipe.zadd(tokenKey(tok.text), doc.getScore() * (0.5 + 0.5*tok.frequency), doc.getId());
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

            root = null;
            this.query = query;
            for (Query.Filter flt : query.filters) {
                Spec.Field field = null;
                for (Spec.Field f : spec.fields) {
                    if (f.matches(flt.property)) {
                        field = f;
                        break;
                    }
                }

                Step s = null;
                switch (field.type) {

                    case FullText:
                        if (flt.op != Query.Op.Matches) {
                            throw new RuntimeException("Only MATCH filters are allowed for full text filters, got " + flt.op.toString());
                        }
                        if (flt.values.length != 1) {
                            throw new RuntimeException("Only one string value is allowed for fulltext filters");
                        }
                        s = new TextIntersectStep((String) flt.values[0]);

                        break;
                    case Numeric:
                        Double min, max = null;
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
                        s = new RangeStep(flt.property, min, max);

                        break;
                    case Geo:
                        if (flt.op != Query.Op.Radius) {
                            throw new RuntimeException("Unsupported op for geo index: " + flt.op.toString());
                        }
                        double lat = (double) flt.values[0];
                        double lon = (double) flt.values[1];
                        double radius = (double) flt.values[2];
                        s = new GeoRadiusStep(lat, lon, radius, ((Spec.GeoField) field).precision);

                        break;
                    default:
                        throw new RuntimeException("Unsupported field type: " + field.type.toString());
                }

                if (root == null) {
                    root = s;

                } else if(s != null) {
                    root.addChild(s);
                }
            }
            //System.out.println(root.toString());

        }

        public List<Entry> execute() {
            Jedis conn = pool.getResource();
            Pipeline pipe = conn.pipelined();

            String tmpKey = root.execute(pipe);


            pipe.zrevrangeWithScores(tmpKey, query.sort.offset, query.sort.offset + query.sort.limit);

            List<Object> res = pipe.syncAndReturnAll();
            conn.close();


            Set<Tuple> ids = (Set<Tuple>) res.get(res.size() - 1);
            List<Entry> entries = new ArrayList<>(ids.size());
            for (Tuple t : ids) {
                entries.add(new Entry(t.getElement(), t.getScore()));
            }
            return entries;
        }


    }


    private abstract class Step {

        List<Step> children;

        static final protected int DEFAULT_EXPIRATION = 60;

        @Override
        public String toString() {
            return toString("");
        }

        public String toString(String tabs) {

            String childrenstr = "";

            for (Step c : children) {
                childrenstr += c.toString(tabs + "  ") + ",\n";
            }
            return String.format("%s%s {\n%s%s}", tabs, getClass().getSimpleName(), childrenstr, tabs);
        }


        public Step(Step... children) {
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

        protected String makeTmpKey(String... subKeys) {

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

        double weight() {
            return 1.0;
        }

        /**
         * Get the respective weights of the children to use for aggregated actions
         *
         * @return
         */
        protected double[] childrenWeights() {
            double[] ret = new double[children.size()];
            for (int i = 0; i < children.size(); i++) {
                ret[i] = children.get(i).weight();
            }
            return ret;
        }

    }

    private class IntersectStep extends Step {


        public IntersectStep(Step... children) {
            super(children);
        }


        @Override
        String execute(Pipeline pipe) {

            String[] tmpKeys = executeChildren(pipe);

            String tk = makeTmpKey(tmpKeys);
            pipe.zinterstore(tk, new ZParams().weightsByDouble(childrenWeights()), tmpKeys);
            pipe.expire(tk, DEFAULT_EXPIRATION);
            return tk;
        }
    }

    private class TextIntersectStep extends Step {

        List<Token> tokens;
        String raw;

        /**
         * Create an intersection step between multiple token indexes
         *
         * @param text
         */
        public TextIntersectStep(String text) {
            super();
            raw = text;
            tokens = tokenizer.tokenize(text);

        }

        @Override
        String execute(Pipeline pipe) {

            if (tokens.size() == 1) {
                return tokenKey(tokens.get(0).text);
            }

            String[] keys = new String[tokens.size()+1];
            keys[0] = makeTmpKey(raw);
            for (int i = 0; i < tokens.size(); i++) {
                keys[i+1] = tokenKey(tokens.get(i).text);
            }

            intersectTokens.execute(pipe, 1, keys);
            pipe.expire(keys[0], DEFAULT_EXPIRATION);
            return keys[0];
        }
    }

    private class RangeStep extends Step {

        private String fieldName;
        private Double min;
        private Double max;

        public RangeStep(String fieldName, Double min, Double max) {

            this.fieldName = fieldName;
            this.min = min;
            this.max = max;
        }


        @Override
        String execute(Pipeline pipe) {

            String tk = makeTmpKey();
            // create a key with the given range we want, using the lua script
            zrangeByScoreStore.execute(pipe, 2, fieldKey(fieldName), tk,
                    min != null ? min.toString() : "-inf",
                    max != null ? max.toString() : "+inf");

            pipe.expire(tk, DEFAULT_EXPIRATION);
            return tk;
        }

        @Override
        double weight() {
            return 0;
        }

    }

    private class UnionStep extends Step {

        public UnionStep(Step... children) {
            super(children);
        }

        @Override
        String execute(Pipeline pipe) {

            String[] tmpKeys = executeChildren(pipe);

            String tk = makeTmpKey(tmpKeys);
            pipe.zunionstore(tk, new ZParams().weightsByDouble(childrenWeights()), tmpKeys);
            pipe.expire(tk, DEFAULT_EXPIRATION);
            return tk;
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

        @Override
        double weight() {
            return 0d;
        }

        /**
         * Calculate the geo hash cells we need to query in order to cover all points within this query
         *
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
