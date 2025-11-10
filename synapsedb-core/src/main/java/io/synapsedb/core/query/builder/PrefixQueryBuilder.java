package io.synapsedb.core.query.builder;

import io.synapsedb.core.query.QueryBuilder;
import io.synapsedb.core.query.validation.ValidationResult;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;

/**
 * Builder for prefix queries - matches terms that start with a given prefix.
 * More efficient than wildcard queries for "starts with" operations.
 *
 * Examples:
 * - Find names starting with "John": prefix("name", "John")
 * - Find emails starting with "admin": prefix("email", "admin")
 *
 * Use cases:
 * - Auto-complete functionality
 * - Prefix-based filtering
 * - Username/email search
 *
 * Note: For "contains" or "ends with", use WildcardQueryBuilder instead
 *
 * @author Amit Tiwari
 */
public class PrefixQueryBuilder extends QueryBuilder {

    private final String field;
    private final String prefix;

    public PrefixQueryBuilder(String field, String prefix) {
        this.field = field;
        this.prefix = prefix;
    }

    @Override
    public Query toLuceneQuery() {
        Query query = new PrefixQuery(new Term(field, prefix));

        if (boost != 1.0f) {
            return new org.apache.lucene.search.BoostQuery(query, boost);
        }

        return query;
    }

    @Override
    public ValidationResult validate() {
        ValidationResult.Builder builder = new ValidationResult.Builder();

        if (field == null || field.trim().isEmpty()) {
            builder.addError("field", "Field name is required for prefix query");
        }

        if (prefix == null || prefix.isEmpty()) {
            builder.addError("prefix", "Prefix value is required and cannot be empty");
        }

        if (prefix != null && prefix.length() == 1) {
            builder.addWarning("prefix",
                "Single-character prefix may match too many terms and be slow. Consider longer prefix.");
        }

        if (prefix != null && prefix.length() > 100) {
            builder.addWarning("prefix",
                "Very long prefix (" + prefix.length() + " chars). Consider using TermQuery for exact matching.");
        }

        return builder.build();
    }

    @Override
    public String toString() {
        return String.format("PrefixQuery{field='%s', prefix='%s', boost=%.2f}",
            field, prefix, boost);
    }

    @Override
    public String toJson() {
        return String.format(
            "{\"type\":\"prefix\",\"field\":\"%s\",\"prefix\":\"%s\",\"boost\":%.2f}",
            field, escapeJson(prefix), boost
        );
    }

    @Override
    public String getQueryType() {
        return "prefix";
    }

    // Getters

    public String getField() {
        return field;
    }

    public String getPrefix() {
        return prefix;
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

