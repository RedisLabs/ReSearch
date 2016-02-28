package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Index;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;
import com.redislabs.research.text.TextNormalizer;
import com.redislabs.research.text.Token;
import com.redislabs.research.text.Tokenizer;
import com.sun.deploy.util.StringUtils;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ZParams;
import sun.security.provider.MD5;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
        return null;
    }

    @Override
    public List<String> get(Query q) throws IOException, InterruptedException {
        return null;
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

    void indexTokens(String docId, Double docScore, Iterable<Token> tokens) {

        Jedis conn = pool.getResource();
        Pipeline pipe = conn.pipelined();

        for (Token tok : tokens) {
            pipe.zadd(tokenKey(tok.text), docScore*tok.frequency, docId);
        }
        pipe.sync();
        conn.close();
    }


    private abstract class Step {

        Step []children;

        public Step(Step []children) {
            this.children = children;
        }

        String[] executeChildren(Pipeline pipe) {
            String[] keys = new String[children.length];

            for (int i = 0; i < children.length; i++) {
                keys[i] = children[i].execute(pipe);

            }

            return keys;


        }

        abstract String execute(Pipeline pipe);

        protected String makeTmpKey(String []subKeys) {

            if (subKeys.length == 0) {
                return UUID.randomUUID().toString();
            }
            MessageDigest d;
            try {
                d = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

            for (String k : subKeys) {
                d.update(k.getBytes());
            }

            return new String(d.digest());
        }

    }

    private class IntersectStep extends Step {


        public IntersectStep(Step[] children) {
            super(children);
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

        public UnionStep(Step[] children) {
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
            super(null);
            this.token = tok;
        }
        @Override
        String execute(Pipeline pipe) {
            return tokenKey(token.text);
        }
    }


}
