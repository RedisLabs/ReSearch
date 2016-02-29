package com.redislabs.research;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by dvirsky on 07/02/16.
 */
public class Spec {

    public enum IndexingType {
        FullText,
        Prefix,
        Geo,
        Numeric,
    }

    public static class Field {
        public String name;
        public IndexingType type;

        public Field(String name, IndexingType type) {
            this.name = name;
            this.type = type;
        }


    }

    public static class PrefixField extends  Field {

        public boolean indexSuffixes;

        public PrefixField(String name, boolean indexSuffixes) {
            super(name, IndexingType.Prefix);
            this.indexSuffixes = indexSuffixes;
        }
    }

    public static PrefixField prefix(String name, boolean indexSuffixes) {
        return new PrefixField(name, indexSuffixes);
    }

    public static class GeoField extends Field {

        public int precision;
        public GeoField(String name, int precision) {
            super(name, IndexingType.Geo);
            this.precision = precision;
        }
    }

    public static Field geo(String name, int precision) {
        return new GeoField(name, precision);
    }

    public static Field numeric(String name) {
        return new Field(name, IndexingType.Numeric);
    }


    public static class FulltextField extends Field {

        public double weight;
        public FulltextField(String name, double weight) {
            super(name, IndexingType.FullText);
            this.weight = weight;
        }
    }

    public static Field fulltext(String name, double weight) {
        return new FulltextField(name, weight);
    }


    public List<Field> fields;
    public Spec(Field ...fields) {

        this.fields = Arrays.asList(fields);

    }
}
