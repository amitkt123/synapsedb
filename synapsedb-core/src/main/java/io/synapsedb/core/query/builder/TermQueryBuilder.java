package io.synapsedb.core.query.builder;

import io.synapsedb.core.query.QueryBuilder;
import io.synapsedb.core.query.validation.ValidationResult;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * Builder for exact term matching queries.
 * Used for keyword fields where you want exact matching (not analyzed).
 *
 * Example: Find products in "Electronics" category
 *   new TermQueryBuilder("category", "Electronics")
 *
 * Use this for:
 * - Categories, tags, status codes
 * - IDs, exact values
 * - Non-text fields that shouldn't be analyzed
 *
 * @author Amit Tiwari
 */
public class TermQueryBuilder extends QueryBuilder {

    private final String field;
    private final String value;

    /**
     * Create a term query for exact matching.
     *
     * @param field The field name to search in
     * @param value The exact value to match
     */
    public TermQueryBuilder(String field, String value) {
        this.field = field;
        this.value = value;
    }

    @Override
    public Query toLuceneQuery() {
        Query query = new TermQuery(new Term(field, value));

        // Apply boost if set
        if (boost != 1.0f) {
            return new org.apache.lucene.search.BoostQuery(query, boost);
        }

        return query;
    }

    @Override
    public ValidationResult validate() {
        ValidationResult.Builder builder = new ValidationResult.Builder();

        // Check if field is provided
        if (field == null || field.trim().isEmpty()) {
            builder.addError("field", "Field name is required for term query");
        }

        // Check if value is provided
        if (value == null) {
            builder.addError("value", "Value is required for term query");
        }

        // Warning for empty string value
        if (value != null && value.trim().isEmpty()) {
            builder.addWarning("value", "Term value is empty. This may not match any documents.");
        }

        // Warning for text-like fields
        if (field != null && (field.equals("description") || field.equals("content") || field.equals("text"))) {
            builder.addWarning(field,
                "Field '" + field + "' looks like a text field. Consider using MatchQueryBuilder for full-text search.");
        }

        // Warning for values with spaces (should be analyzed)
        if (value != null && value.contains(" ")) {
            builder.addWarning("value",
                "Term value contains spaces. TermQuery does exact matching - consider using MatchQuery for analyzed text.");
        }

        return builder.build();
    }

    @Override
    public String toString() {
        return String.format("TermQuery{field='%s', value='%s', boost=%.2f}",
            field, value, boost);
    }

    @Override
    public String toJson() {
        return String.format(
            "{\"type\":\"term\",\"field\":\"%s\",\"value\":\"%s\",\"boost\":%.2f}",
            field, escapeJson(value), boost
        );
    }

    @Override
    public String getQueryType() {
        return "term";
    }

    // Getters

    public String getField() {
        return field;
    }

    public String getValue() {
        return value;
    }

    /**
     * Escape special characters in JSON strings
     */
    private String escapeJson(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }
}

