package io.synapsedb.query.builder;

import io.synapsedb.query.QueryBuilder;
import io.synapsedb.query.validation.ValidationResult;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.Query;

/**
 * Builder for wildcard queries with pattern matching.
 * Supports * (matches any character sequence) and ? (matches single character)
 *
 * Examples:
 * - Starts with: wildcard("name", "John*")
 * - Ends with: wildcard("email", "*@gmail.com")
 * - Contains: wildcard("description", "*wireless*")
 * - Pattern: wildcard("code", "A??B*") // A + 2 chars + B + anything
 *
 * Performance Note:
 * - Leading wildcards (*term) are SLOW - they can't use index efficiently
 * - For "starts with", use PrefixQueryBuilder instead (much faster)
 * - For exact match, use TermQueryBuilder
 *
 * @author Amit Tiwari
 */
public class WildcardQueryBuilder extends QueryBuilder {

    private final String field;
    private final String pattern;

    public WildcardQueryBuilder(String field, String pattern) {
        this.field = field;
        this.pattern = pattern;
    }

    @Override
    public Query toLuceneQuery() {
        Query query = new WildcardQuery(new Term(field, pattern));

        if (boost != 1.0f) {
            return new org.apache.lucene.search.BoostQuery(query, boost);
        }

        return query;
    }

    @Override
    public ValidationResult validate() {
        ValidationResult.Builder builder = new ValidationResult.Builder();

        if (field == null || field.trim().isEmpty()) {
            builder.addError("field", "Field name is required for wildcard query");
        }

        if (pattern == null || pattern.isEmpty()) {
            builder.addError("pattern", "Wildcard pattern is required and cannot be empty");
        }

        if (pattern != null) {
            // Check for leading wildcard (performance issue)
            if (pattern.startsWith("*") || pattern.startsWith("?")) {
                builder.addWarning("pattern",
                    "Leading wildcard (* or ?) can be very slow. Consider alternative queries if possible.");
            }

            // Check if it's actually a prefix query in disguise
            if (pattern.endsWith("*") && !pattern.substring(0, pattern.length() - 1).contains("*")
                && !pattern.substring(0, pattern.length() - 1).contains("?")) {
                builder.addWarning("pattern",
                    "This is a simple prefix query. Use PrefixQueryBuilder for better performance.");
            }

            // Check if no wildcards at all
            if (!pattern.contains("*") && !pattern.contains("?")) {
                builder.addWarning("pattern",
                    "No wildcards found in pattern. Use TermQueryBuilder for exact matching (more efficient).");
            }

            // Check for only wildcards
            if (pattern.replace("*", "").replace("?", "").isEmpty()) {
                builder.addWarning("pattern",
                    "Pattern contains only wildcards - will match everything and be very slow.");
            }
        }

        return builder.build();
    }

    @Override
    public String toString() {
        return String.format("WildcardQuery{field='%s', pattern='%s', boost=%.2f}",
            field, pattern, boost);
    }

    @Override
    public String toJson() {
        return String.format(
            "{\"type\":\"wildcard\",\"field\":\"%s\",\"pattern\":\"%s\",\"boost\":%.2f}",
            field, escapeJson(pattern), boost
        );
    }

    @Override
    public String getQueryType() {
        return "wildcard";
    }

    // Getters

    public String getField() {
        return field;
    }

    public String getPattern() {
        return pattern;
    }

    private String escapeJson(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }
}

