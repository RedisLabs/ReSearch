package com.redislabs.research;

import java.util.LinkedList;
import java.util.List;

/**
 * Query represents query parameters and filters to load results from the engine
 */
public class Query {

    /**
     * Filtering ops
     */
    public enum Op {
        Equals,
        In,
        Prefix,
        Greater,
        GreaterEquals,
        Less,
        LessEqual,
        Between,
        Radius,
        Near,
    }

    /**
     * Filter represents a filtering ruls in a query
     * @param <T>
     */
    public static class Filter<T> {

        public String property;
        public Op op;
        public T[] values;

        /**
         * Create a new filter for a property with a given op and a value.
         * This should not be used directly in most cases, there are specific shortcut
         * methods for specific filters
         * @param property the name of the property filtered by
         * @param op the filtering op
         * @param values the values filtering by - one in the case of EQ and the like
         */
        public Filter(String property, Op op, T ...values) {
            this.property = property;
            this.op = op;
            this.values = values;
        }

    }


    /**
     * Defines how the result is sorted and paged
     */
    public static class Sorting {
        /** what fields are we sorting by */
        public String by;
        /** is the sorting ascending or not */
        public Boolean ascending;
        /** If set, paging offset */
        public Integer offset;
        /** If set, paging limit */
        public Integer limit;

        public static Sorting Default() {
            Sorting ret = new Sorting();
            ret.offset = 0;
            ret.limit = 10;
            ret.by = "";
            ret.ascending = true;
            return ret;
        }
    }

    /**
     * The query's filter list. We only support AND operation on all those filters
     */
    public List<Filter> filters;

    /**
     * The name of the index we are querying
     */
    public String indexName;

    /**
     * The sorting parameters
     */
    public Sorting sort;

    /**
     * Create a new index
     * @param indexName the name of the index we want to query
     */
    public Query(String indexName) {
        this.indexName = indexName;
        filters = new LinkedList<>();
        sort = Sorting.Default();
    }

    /**
     * Set the sorting parameters of the query
     * @param by the field we are sorting by
     * @param ascending whether the sorting is ascending or not
     * @return the query itself, for builder-style syntax
     */
    public Query sortBy(String by, Boolean ascending) {
        sort.by = by;
        sort.ascending = ascending;
        return this;
    }

    /**
     * Limit the results to a certain offset and limit
     * @param offset the first result to show, zero based indexing
     * @param limit how many results we want to show
     * @return the query itself, for builder-style syntax
     */
    public Query limit(Integer offset, Integer limit ) {
        sort.offset = offset;
        sort.limit = limit;
        return this;
    }

    /**
     * Create an Eq filter, filtering records where property==value
     * @param property the name of the property filtered by
     * @param value the value it's equal to
     * @return the query itself, for builder-style syntax
     */
    public  Query filterEquals(String property, Object value) {
        filters.add(new Filter<>(property, Op.Equals, value));
        return this;
    }

    /**
     * Create a prefix filter, filtering only records where property starts with prefix
     * @param property  the name of the property filtered by
     * @param prefix the prefix to search for
     * @return the query itself, for builder-style syntax
     */
    public Query filterPrefix(String property, String prefix) {
        filters.add(new Filter<String>(property, Op.Prefix, prefix));
        return this;
    }

    /**
     * Create an IN filter, filtering only records where property is one of the given values
     * @param property the name of the property filtered by
     * @param values the values we want to include
     * @return the query itself, for builder-style syntax
     */
    public Query filterIn(String property, Object ...values) {
        filters.add(new Filter<>(property, Op.In, values));
        return this;
    }

    /**
     * Create a NEAR filter - it's a rough proximity geo filter, with no exact radius - but rather a GEOAHASH match.
     * i.e. the filter will be positive if the given lat,lon and documents' lat,lon are in the same geobox as
     * defined in the index spec
     * @param property the name of the property filtered by
     * @param lat latitude to match
     * @param  lon longitude to match
     * @return the query itself, for builder-style syntax
     */
    public Query filterNear(String property, double lat, double lon) {
        filters.add(new Filter<>(property, Op.Near, lat, lon));
        return this;
    }

    /**
     * Create a GREATER THAN filter
     * @param property the name of the property filtered by
     * @param value number or string to compare
     */
    public Query filterGreaterThan(String property, Object value) {
        filters.add(new Filter<>(property, Op.Greater, value));
        return this;
    }

    /**
     * Create a GREATER THAN OR EQUAL filter
     * @param property the name of the property filtered by
     * @param value number or string to compare
     */
    public Query filterGreaterEqual(String property, Object value) {
        filters.add(new Filter<>(property, Op.GreaterEquals, value));
        return this;
    }

    /**
     * Create a LESS THAN filter
     * @param property the name of the property filtered by
     * @param value number or string to compare
     */
    public Query filterLessThan(String property, Object value) {
        filters.add(new Filter<>(property, Op.Less, value));
        return this;
    }

    /**
     * Create a LESS THAN OR EQUAL filter
     * @param property the name of the property filtered by
     * @param value number or string to compare
     */
    public Query filterLessEqual(String property, Object value) {
        filters.add(new Filter<>(property, Op.LessEqual, value));
        return this;
    }

}
