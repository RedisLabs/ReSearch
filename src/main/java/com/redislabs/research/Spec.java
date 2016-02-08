package com.redislabs.research;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by dvirsky on 07/02/16.
 */
class Spec {

    enum IndexingType {
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

    public List<Field> fields;
    public Spec(Field ...fields) {

        this.fields = Arrays.asList(fields);

    }
}
