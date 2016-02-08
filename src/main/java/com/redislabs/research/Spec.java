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
    public static Field prefix(String name) {
        return new Field(name, IndexingType.Prefix);
    }
    public static Field numeric(String name) {
        return new Field(name, IndexingType.Numeric);
    }

    public List<Field> fields;
    public Spec(Field ...fields) {

        this.fields = Arrays.asList(fields);

    }
}
