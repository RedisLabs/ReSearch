package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Index;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;
import com.redislabs.research.dep.Hashids;
import com.redislabs.research.text.TextNormalizer;
import com.redislabs.research.text.Token;
import com.redislabs.research.text.Tokenizer;
import com.sun.deploy.util.StringUtils;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import com.sun.org.apache.xerces.internal.impl.xs.identity.Field;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ZParams;
import sun.security.provider.MD5;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.zip.CRC32;

/**
 * This is a full text index operating on intersection of faceted sorted sets
 *
 * Text fields are tokenized and inserted in
 */
public class FullTextFacetedIndex extends BaseIndex {

    private Tokenizer tokenizer;

    public FullTextFacetedIndex(String redisURI, String name, Spec spec, Tokenizer tokenizer) {
        super(name, spec, redisURI);
        this.tokenizer = tokenizer;
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

    List<String> executeQuery(String query) {

        List<Token> tokens = tokenizer.tokenize(query);

        Step[] tsteps = new Step[tokens.size()];
        for (int i =0 ; i < tokens.size(); i++) {
            tsteps[i] = new TokenStep(tokens.get(i));
        }
        Step root = new IntersectStep(tsteps);

        Jedis conn = pool.getResource();
        Pipeline pipe = conn.pipelined();

        String tmpKey = root.execute(pipe);

        pipe.zrange(tmpKey, 0, 10);
        List<Object> res = pipe.syncAndReturnAll();
        conn.close();


        Set<String> ids = (Set<String>) res.get(res.size()-1);

        return new ArrayList<>(ids);



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

                    default:
                        throw new RuntimeException("Unsupported field type: " + field.type.toString());


                }
            }

        }

        public List<String> execute() {
            Jedis conn = pool.getResource();
            Pipeline pipe = conn.pipelined();

            String tmpKey = root.execute(pipe);

            pipe.zrevrange(tmpKey, query.sort.offset, query.sort.offset+query.sort.limit);
            List<Object> res = pipe.syncAndReturnAll();
            conn.close();


            Set<String> ids = (Set<String>) res.get(res.size()-1);

            return new ArrayList<>(ids);
        }


    }


    private abstract class Step {

        List<Step> children;



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

        protected String makeTmpKey(String []subKeys) {

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

            String[] tmpKeys = super.executeChildren(pipe);

            String tk = makeTmpKey(tmpKeys);
            pipe.zinterstore(tk, new ZParams(), tmpKeys);
            pipe.expire(tk, 60);
            return tk;
        }
    }

    private class UnionStep extends Step {

        public UnionStep(Step ...children) {
            super(children);
        }

        @Override
        String execute(Pipeline pipe) {

            String[] tmpKeys = super.executeChildren(pipe);

            String tk = makeTmpKey(tmpKeys);
            pipe.zunionstore(tk, new ZParams(), tmpKeys);
            pipe.expire(tk, 60);
            return tk;
        }
    }

    private class TokenStep extends Step {
        private final Token token;

        public TokenStep(Token tok) {
            super();
            this.token = tok;
        }
        @Override
        String execute(Pipeline pipe) {
            return tokenKey(token.text);
        }


    }


}
