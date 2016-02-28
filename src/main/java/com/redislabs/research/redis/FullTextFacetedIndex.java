package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Index;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;
import com.redislabs.research.text.TextNormalizer;
import com.redislabs.research.text.Tokenizer;

import java.io.IOException;
import java.util.List;

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



}
