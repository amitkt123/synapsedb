package io.synapsedb.core.search;

import io.synapsedb.core.query.QueryBuilder;
import io.synapsedb.core.query.QueryContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a search request with query, pagination, sorting, and filtering.
 * This is the high-level API for executing searches.
 *
 * @author Amit Tiwari
 */
public class SearchRequest {

    private QueryBuilder query;
    private int from = 0;
    private int size = 10;
    private List<SortField> sortFields = new ArrayList<>();
    private List<String> fieldsToReturn = new ArrayList<>();
    private QueryContext context;
    private Map<String, Object> parameters = new HashMap<>();

    // Advanced options
    private boolean trackTotalHits = true;
    private Integer timeout;
    private boolean explain = false;

    public SearchRequest() {
    }

    public SearchRequest(QueryBuilder query) {
        this.query = query;
    }

    // Fluent API

    public SearchRequest query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    /**
     * Set the starting position (for pagination)
     */
    public SearchRequest from(int from) {
        this.from = from;
        return this;
    }

    /**
     * Set the number of results to return
     */
    public SearchRequest size(int size) {
        this.size = size;
        return this;
    }

    /**
     * Add a sort field
     */
    public SearchRequest sort(String field, SortOrder order) {
        this.sortFields.add(new SortField(field, order));
        return this;
    }

    /**
     * Specify which fields to return (source filtering)
     */
    public SearchRequest fields(String... fields) {
        for (String field : fields) {
            this.fieldsToReturn.add(field);
        }
        return this;
    }

    public SearchRequest context(QueryContext context) {
        this.context = context;
        return this;
    }

    public SearchRequest timeout(int timeoutMs) {
        this.timeout = timeoutMs;
        return this;
    }

    public SearchRequest explain(boolean explain) {
        this.explain = explain;
        return this;
    }

    public SearchRequest parameter(String key, Object value) {
        this.parameters.put(key, value);
        return this;
    }

    // Getters

    public QueryBuilder getQuery() {
        return query;
    }

    public int getFrom() {
        return from;
    }

    public int getSize() {
        return size;
    }

    public List<SortField> getSortFields() {
        return new ArrayList<>(sortFields);
    }

    public List<String> getFieldsToReturn() {
        return new ArrayList<>(fieldsToReturn);
    }

    public QueryContext getContext() {
        return context;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public boolean isExplain() {
        return explain;
    }

    public boolean isTrackTotalHits() {
        return trackTotalHits;
    }

    public Map<String, Object> getParameters() {
        return new HashMap<>(parameters);
    }

    @Override
    public String toString() {
        return "SearchRequest{" +
                "query=" + (query != null ? query.getQueryType() : "null") +
                ", from=" + from +
                ", size=" + size +
                ", sorts=" + sortFields.size() +
                '}';
    }

    /**
     * Represents a sort field
     */
    public static class SortField {
        private final String field;
        private final SortOrder order;

        public SortField(String field, SortOrder order) {
            this.field = field;
            this.order = order;
        }

        public String getField() {
            return field;
        }

        public SortOrder getOrder() {
            return order;
        }

        @Override
        public String toString() {
            return field + ":" + order;
        }
    }

    /**
     * Sort order
     */
    public enum SortOrder {
        ASC,
        DESC
    }
}

