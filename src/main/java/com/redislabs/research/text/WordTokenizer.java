package com.redislabs.research.text;


import java.util.*;

/**
 * Created by dvirsky on 28/02/16.
 */
public class WordTokenizer implements Tokenizer {

    TextNormalizer normalizer;
    private Set<String> stopwords;

    final static private String[] DEFAULT_STOPWORDS = {
            "a", "an", "and", "are", "as", "at", "be", "but", "by",
            "for", "if", "in", "into", "is", "it",
            "no", "not", "of", "on", "or", "such",
            "that", "the", "their", "then", "there", "these",
            "they", "this", "to", "was", "will", "with"
    };

    private boolean normalizeFrequencies;

    public WordTokenizer(TextNormalizer nrml, String[] stopwords) {
        this(nrml, true, stopwords);
    }

    public WordTokenizer(TextNormalizer nrml, boolean normalizeFrequencies, String[] stopwords) {
        normalizer = nrml;
        this.normalizeFrequencies = normalizeFrequencies;
        this.stopwords = new HashSet<>(Arrays.asList(stopwords != null ? stopwords : DEFAULT_STOPWORDS));

    }
    public WordTokenizer(TextNormalizer nrml) {
        this(nrml, DEFAULT_STOPWORDS);
    }

    @Override
    public List<Token> tokenize(String text) {
        StringTokenizer tkn = new StringTokenizer(text, " \t\n\r\f\"\'.,!@#$?%^&*()[]{}\\/<>-_|:;~");

        HashMap<String, Token> tokens = new HashMap<>();
        int offset = 0;
        while (tkn.hasMoreTokens()) {

            String s = normalizer.normalize(tkn.nextToken());
            if (stopwords.contains(s)) {
                continue;
            }

            Token t = tokens.get(s);
            boolean isnew = false;
            if (t == null) {
                t = new Token(s);
                isnew = true;
            }

            t.frequency +=1;
            t.offsets.add(offset);
            offset++;

            if (isnew) {
                tokens.put(s, t);
            }


        }

        if (offset > 0 && normalizeFrequencies) {
            for (Token t : tokens.values()) {
                t.frequency /= offset;
            }
        }

        return new ArrayList<>(tokens.values());
    }
}
