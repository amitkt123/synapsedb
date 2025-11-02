package io.synapsedb.query.builder;

import io.synapsedb.query.QueryBuilder;
import io.synapsedb.query.validation.ValidationResult;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for boolean queries that combine multiple queries with AND/OR/NOT logic.
 *
 * Example: Find Electronics under $1000
 *   BoolQueryBuilder.must(term("category", "Electronics"))
 *                   .must(range("price").lt(1000))
 * @author Amit Tiwari
 */
public class BoolQueryBuilder extends QueryBuilder {

    private final List<QueryBuilder> mustClauses = new ArrayList<>();
    private final List<QueryBuilder> shouldClauses = new ArrayList<>();
    private final List<QueryBuilder> mustNotClauses = new ArrayList<>();
    private final List<QueryBuilder> filterClauses = new ArrayList<>();

    private Integer minimumShouldMatch = null;

    public BoolQueryBuilder() {
    }

    // Fluent API for adding clauses

    /**
     * Add a MUST clause (AND logic) - document MUST match this
     */
    public BoolQueryBuilder must(QueryBuilder query) {
        mustClauses.add(query);
        return this;
    }

    /**
     * Add a SHOULD clause (OR logic) - document SHOULD match this (scoring)
     */
    public BoolQueryBuilder should(QueryBuilder query) {
        shouldClauses.add(query);
        return this;
    }

    /**
     * Add a MUST_NOT clause (NOT logic) - document MUST NOT match this
     */
    public BoolQueryBuilder mustNot(QueryBuilder query) {
        mustNotClauses.add(query);
        return this;
    }

    /**
     * Add a FILTER clause - document must match but doesn't affect scoring
     */
    public BoolQueryBuilder filter(QueryBuilder query) {
        filterClauses.add(query);
        return this;
    }

    /**
     * Set minimum number of should clauses that must match
     */
    public BoolQueryBuilder minimumShouldMatch(int count) {
        this.minimumShouldMatch = count;
        return this;
    }

    @Override
    public Query toLuceneQuery() {
        BooleanQuery.Builder luceneBuilder = new BooleanQuery.Builder();

        // Add MUST clauses
        for (QueryBuilder qb : mustClauses) {
            luceneBuilder.add(qb.toLuceneQuery(), BooleanClause.Occur.MUST);
        }

        // Add SHOULD clauses
        for (QueryBuilder qb : shouldClauses) {
            luceneBuilder.add(qb.toLuceneQuery(), BooleanClause.Occur.SHOULD);
        }

        // Add MUST_NOT clauses
        for (QueryBuilder qb : mustNotClauses) {
            luceneBuilder.add(qb.toLuceneQuery(), BooleanClause.Occur.MUST_NOT);
        }

        // Add FILTER clauses
        for (QueryBuilder qb : filterClauses) {
            luceneBuilder.add(qb.toLuceneQuery(), BooleanClause.Occur.FILTER);
        }

        // Set minimum should match
        if (minimumShouldMatch != null) {
            luceneBuilder.setMinimumNumberShouldMatch(minimumShouldMatch);
        }

        Query query = luceneBuilder.build();

        // Apply boost if set
        if (boost != 1.0f) {
            return new org.apache.lucene.search.BoostQuery(query, boost);
        }

        return query;
    }

    @Override
    public ValidationResult validate() {
        ValidationResult.Builder builder = new ValidationResult.Builder();

        // Check if at least one clause is provided
        if (mustClauses.isEmpty() && shouldClauses.isEmpty() &&
            mustNotClauses.isEmpty() && filterClauses.isEmpty()) {
            builder.addError("clauses", "Boolean query must have at least one clause");
        }

        // Check if MUST_NOT is used alone (invalid)
        if (mustClauses.isEmpty() && shouldClauses.isEmpty() &&
            filterClauses.isEmpty() && !mustNotClauses.isEmpty()) {
            builder.addError("clauses", "Boolean query cannot have only MUST_NOT clauses. Add at least one MUST or SHOULD clause.");
        }

        // Validate each sub-query
        validateSubQueries(mustClauses, "must", builder);
        validateSubQueries(shouldClauses, "should", builder);
        validateSubQueries(mustNotClauses, "mustNot", builder);
        validateSubQueries(filterClauses, "filter", builder);

        // Warning for too many clauses
        int totalClauses = mustClauses.size() + shouldClauses.size() +
                          mustNotClauses.size() + filterClauses.size();
        if (totalClauses > 20) {
            builder.addWarning("clauses",
                "Boolean query has " + totalClauses + " clauses. Consider simplifying for better performance.");
        }

        return builder.build();
    }

    private void validateSubQueries(List<QueryBuilder> queries, String clauseType, ValidationResult.Builder builder) {
        for (int i = 0; i < queries.size(); i++) {
            ValidationResult subResult = queries.get(i).validate();
            if (!subResult.isValid()) {
                builder.addError(clauseType + "[" + i + "]",
                    "Invalid sub-query: " + subResult.getErrorMessage());
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("BoolQuery{");
        if (!mustClauses.isEmpty()) sb.append("must=").append(mustClauses.size()).append(", ");
        if (!shouldClauses.isEmpty()) sb.append("should=").append(shouldClauses.size()).append(", ");
        if (!mustNotClauses.isEmpty()) sb.append("mustNot=").append(mustNotClauses.size()).append(", ");
        if (!filterClauses.isEmpty()) sb.append("filter=").append(filterClauses.size()).append(", ");
        sb.append("boost=").append(boost);
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toJson() {
        StringBuilder sb = new StringBuilder("{\"type\":\"bool\",");

        if (!mustClauses.isEmpty()) {
            sb.append("\"must\":[");
            for (int i = 0; i < mustClauses.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(mustClauses.get(i).toJson());
            }
            sb.append("],");
        }

        if (!shouldClauses.isEmpty()) {
            sb.append("\"should\":[");
            for (int i = 0; i < shouldClauses.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(shouldClauses.get(i).toJson());
            }
            sb.append("],");
        }

        if (!mustNotClauses.isEmpty()) {
            sb.append("\"must_not\":[");
            for (int i = 0; i < mustNotClauses.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(mustNotClauses.get(i).toJson());
            }
            sb.append("],");
        }

        sb.append("\"boost\":").append(boost);
        sb.append("}");

        return sb.toString();
    }

    @Override
    public String getQueryType() {
        return "bool";
    }

    // Getters

    public List<QueryBuilder> getMustClauses() {
        return new ArrayList<>(mustClauses);
    }

    public List<QueryBuilder> getShouldClauses() {
        return new ArrayList<>(shouldClauses);
    }

    public List<QueryBuilder> getMustNotClauses() {
        return new ArrayList<>(mustNotClauses);
    }

    public List<QueryBuilder> getFilterClauses() {
        return new ArrayList<>(filterClauses);
    }
}

