package io.synapsedb.core.query;

import io.synapsedb.core.query.validation.ValidationResult;

import io.synapsedb.core.query.builder.BoolQueryBuilder;
import io.synapsedb.core.query.builder.FuzzyQueryBuilder;
import io.synapsedb.core.query.builder.MatchQueryBuilder;
import io.synapsedb.core.query.builder.PrefixQueryBuilder;
import io.synapsedb.core.query.builder.RangeQueryBuilder;
import io.synapsedb.core.query.builder.TermQueryBuilder;
import io.synapsedb.core.query.builder.WildcardQueryBuilder;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

/**
 * Factory class for creating query builders.
 * Provides convenient static methods for building queries in a fluent, readable way.
 *
 * This is the entry point for the Query Framework - it makes building queries simple:
 *
 * Example usage:
 * <pre>
 *   // Simple term query
 *   QueryBuilder query = QueryBuilders.term("category", "Electronics");
 *
 *   // Full-text search
 *   QueryBuilder query = QueryBuilders.match("description", "wireless headphones");
 *
 *   // Complex boolean query
 *   QueryBuilder query = QueryBuilders.bool()
 *       .must(QueryBuilders.term("category", "Electronics"))
 *       .should(QueryBuilders.match("description", "premium"))
 *       .mustNot(QueryBuilders.term("status", "discontinued"));
 * </pre>
 *
 * This follows the same pattern as Elasticsearch's QueryBuilders,
 * making it familiar to developers who've used ES.
 *
 * @author Amit Tiwari
 */
public final class QueryBuilders {

    // Private constructor to prevent instantiation
    private QueryBuilders() {
        throw new AssertionError("QueryBuilders is a utility class and should not be instantiated");
    }

    /**
     * Create a term query for exact matching.
     * Use this for keyword fields like categories, tags, IDs, status codes.
     *
     * Example: QueryBuilders.term("category", "Electronics")
     *
     * @param field The field name to search in
     * @param value The exact value to match
     * @return A TermQueryBuilder for fluent configuration
     */
    public static TermQueryBuilder term(String field, String value) {
        return new TermQueryBuilder(field, value);
    }

    /**
     * Create a match query for full-text search.
     * Use this for text fields where you want analyzed/tokenized matching.
     *
     * Example: QueryBuilders.match("description", "wireless headphones")
     *
     * @param field The field name to search in
     * @param text The text to search for (will be analyzed)
     * @return A MatchQueryBuilder for fluent configuration
     */
    public static MatchQueryBuilder match(String field, String text) {
        return new MatchQueryBuilder(field, text);
    }

    /**
     * Create a boolean query for combining multiple conditions.
     * Supports must (AND), should (OR), mustNot (NOT), and filter clauses.
     *
     * Example:
     * <pre>
     *   QueryBuilders.bool()
     *       .must(QueryBuilders.term("category", "Electronics"))
     *       .should(QueryBuilders.match("description", "premium"))
     *       .mustNot(QueryBuilders.term("status", "discontinued"))
     * </pre>
     *
     * @return A BoolQueryBuilder for fluent configuration
     */
    public static BoolQueryBuilder bool() {
        return new BoolQueryBuilder();
    }

    /**
     * Create a match-all query that matches every document.
     * Useful for testing or when you want all documents with certain filters.
     *
     * Example: QueryBuilders.matchAll()
     *
     * @return A QueryBuilder that matches all documents
     */
    public static QueryBuilder matchAll() {
        return new MatchAllQueryBuilder();
    }

    /**
     * Create a range query for numeric fields.
     * Supports int, long, and double types.
     *
     * Examples:
     * - QueryBuilders.range("price").gte(100).lte(500)
     * - QueryBuilders.range("age").gt(18)
     * - QueryBuilders.range("temperature").lt(100)
     *
     * @param field The numeric field name
     * @return A RangeQueryBuilder for fluent configuration
     */
    public static RangeQueryBuilder range(String field) {
        return new RangeQueryBuilder(field);
    }

    /**
     * Create a prefix query for "starts with" matching.
     * More efficient than wildcard for prefix-only searches.
     *
     * Examples:
     * - QueryBuilders.prefix("name", "John")
     * - QueryBuilders.prefix("email", "admin")
     *
     * @param field The field name
     * @param prefix The prefix to match
     * @return A PrefixQueryBuilder for fluent configuration
     */
    public static PrefixQueryBuilder prefix(String field, String prefix) {
        return new PrefixQueryBuilder(field, prefix);
    }

    /**
     * Create a wildcard query with pattern matching.
     * Supports * (any characters) and ? (single character).
     *
     * Examples:
     * - QueryBuilders.wildcard("name", "John*") // starts with
     * - QueryBuilders.wildcard("email", "*@gmail.com") // ends with
     * - QueryBuilders.wildcard("description", "*wireless*") // contains
     *
     * Note: Leading wildcards (*term) are slow. Use prefix() for "starts with".
     *
     * @param field The field name
     * @param pattern The wildcard pattern
     * @return A WildcardQueryBuilder for fluent configuration
     */
    public static WildcardQueryBuilder wildcard(String field, String pattern) {
        return new WildcardQueryBuilder(field, pattern);
    }

    /**
     * Create a fuzzy query for typo-tolerant matching.
     * Uses edit distance to find similar terms.
     *
     * Examples:
     * - QueryBuilders.fuzzy("name", "Jon") // matches "John", "Joan"
     * - QueryBuilders.fuzzy("product", "wireles").maxEdits(1)
     *
     * @param field The field name
     * @param term The term to match (with fuzzy tolerance)
     * @return A FuzzyQueryBuilder for fluent configuration
     */
    public static FuzzyQueryBuilder fuzzy(String field, String term) {
        return new FuzzyQueryBuilder(field, term);
    }

    /**
     * Simple wrapper for MatchAllDocsQuery.
     * Internal class - not meant to be exposed directly.
     */
    private static class MatchAllQueryBuilder extends QueryBuilder {

        @Override
        public Query toLuceneQuery() {
            return new MatchAllDocsQuery();
        }

        @Override
        public ValidationResult validate() {
            return new ValidationResult.Builder().build();
        }

        @Override
        public String toString() {
            return "MatchAllQuery{}";
        }

        @Override
        public String toJson() {
            return "{\"type\":\"match_all\"}";
        }

        @Override
        public String getQueryType() {
            return "match_all";
        }
    }
}

