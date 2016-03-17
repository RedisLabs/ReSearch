package com.redislabs.research;

import java.util.*;

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

        public boolean matches(String fieldName) {
            return this.name.equals(fieldName);
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


    /**
     * FullText field spec.
     * It is unique in that the name represnets the index and not the field itself.
     * You can pass it a list of fields to index, or a map of field=>weight if you want
     * weighted indexing of the fields
     */
    public static class FulltextField extends Field {

        public Map<String,Double> fields;

        public FulltextField(String name, Map<String, Double> weightedFields) {
            super(name, IndexingType.FullText);
            this.fields = weightedFields;
        }
        public FulltextField(String name, String ...fields) {
            super(name, null);
            this.fields = new HashMap<String,Double>();
            for (String f : fields) {
                this.fields.put(f, 1d);
            }
        }

        public double getFieldWeight(String fieldName) {
            Double w = this.fields.get(fieldName);
            return w == null ? 0 : w;
        }

        /**
         * A fullText index matches a field if it's either the same name or if the field list contains the field name
         * @param fieldName
         * @return
         */
        @Override
        public boolean matches(String fieldName) {
            return super.matches(fieldName) || fields.containsKey(fieldName);
        }
    }

    public static Field fulltext(String name, String ...fields) {
        return new FulltextField(name, fields);
    }


    public static Field fulltext(String name, Map<String, Double> weightedFields) {
        return new FulltextField(name, weightedFields);
    }


    public List<Field> fields;
    public Spec(Field ...fields) {

        this.fields = Arrays.asList(fields);

    }
}
