package io.synapsedb.query.builder;

import io.synapsedb.query.QueryBuilder;
import io.synapsedb.query.validation.ValidationResult;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;

/**
 * Builder for fuzzy queries - matches terms that are similar to the given term.
 * Uses Levenshtein (edit) distance to find similar terms.
 *
 * Great for:
 * - Handling typos: "wireles" matches "wireless"
 * - Spelling variations: "color" matches "colour"
 * - User input errors: "iphone" matches "iPhone"
 *
 * Examples:
 * - fuzzy("name", "Jon") // matches "John", "Joan", "Jones"
 * - fuzzy("product", "iphone").maxEdits(1) // stricter matching
 *
 * Edit Distance:
 * - 0 edits: exact match (use TermQuery instead)
 * - 1 edit: close match (recommended for most cases)
 * - 2 edits: loose match (default, more results but some false positives)
 *
 * @author Amit Tiwari
 */
public class FuzzyQueryBuilder extends QueryBuilder {

    private final String field;
    private final String term;
    private int maxEdits = FuzzyQuery.defaultMaxEdits; // Default: 2
    private int prefixLength = FuzzyQuery.defaultPrefixLength; // Default: 0

    public FuzzyQueryBuilder(String field, String term) {
        this.field = field;
        this.term = term;
    }

    /**
     * Set maximum edit distance (0, 1, or 2).
     * Lower value = stricter matching, fewer results.
     *
     * @param maxEdits edit distance (0, 1, or 2)
     */
    public FuzzyQueryBuilder maxEdits(int maxEdits) {
        if (maxEdits < 0 || maxEdits > 2) {
            throw new IllegalArgumentException("maxEdits must be 0, 1, or 2");
        }
        this.maxEdits = maxEdits;
        return this;
    }

    /**
     * Set prefix length - number of initial characters that must match exactly.
     * Higher value = faster but less fuzzy.
     *
     * Example: prefixLength(3) for "wireless" means "wir" must match exactly,
     * then fuzzy matching applies to "eless"
     *
     * @param prefixLength number of characters that must match exactly (0+)
     */
    public FuzzyQueryBuilder prefixLength(int prefixLength) {
        if (prefixLength < 0) {
            throw new IllegalArgumentException("prefixLength must be >= 0");
        }
        this.prefixLength = prefixLength;
        return this;
    }

    @Override
    public Query toLuceneQuery() {
        Query query = new FuzzyQuery(new Term(field, term), maxEdits, prefixLength);

        if (boost != 1.0f) {
            return new org.apache.lucene.search.BoostQuery(query, boost);
        }

        return query;
    }

    @Override
    public ValidationResult validate() {
        ValidationResult.Builder builder = new ValidationResult.Builder();

        if (field == null || field.trim().isEmpty()) {
            builder.addError("field", "Field name is required for fuzzy query");
        }

        if (term == null || term.isEmpty()) {
            builder.addError("term", "Search term is required and cannot be empty");
        }

        if (term != null && term.length() < 3) {
            builder.addWarning("term",
                "Very short term ('" + term + "'). Fuzzy matching on short terms may produce too many results.");
        }

        if (term != null && term.length() > 50) {
            builder.addWarning("term",
                "Very long term (" + term.length() + " chars). Fuzzy matching may be slow.");
        }

        if (maxEdits == 0) {
            builder.addWarning("maxEdits",
                "maxEdits=0 means exact matching. Use TermQueryBuilder instead for better performance.");
        }

        if (prefixLength > 0 && term != null && prefixLength >= term.length()) {
            builder.addWarning("prefixLength",
                "prefixLength >= term length - this is effectively exact matching.");
        }

        return builder.build();
    }

    @Override
    public String toString() {
        return String.format("FuzzyQuery{field='%s', term='%s', maxEdits=%d, prefixLength=%d, boost=%.2f}",
            field, term, maxEdits, prefixLength, boost);
    }

    @Override
    public String toJson() {
        return String.format(
            "{\"type\":\"fuzzy\",\"field\":\"%s\",\"term\":\"%s\",\"maxEdits\":%d,\"prefixLength\":%d,\"boost\":%.2f}",
            field, escapeJson(term), maxEdits, prefixLength, boost
        );
    }

    @Override
    public String getQueryType() {
        return "fuzzy";
    }

    // Getters

    public String getField() {
        return field;
    }

    public String getTerm() {
        return term;
    }

    public int getMaxEdits() {
        return maxEdits;
    }

    public int getPrefixLength() {
        return prefixLength;
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

