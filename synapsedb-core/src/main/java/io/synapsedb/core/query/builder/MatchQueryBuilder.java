package io.synapsedb.core.query.builder;

import io.synapsedb.core.query.QueryBuilder;
import io.synapsedb.core.query.validation.ValidationResult;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;

/**
 * Builder for full-text match queries.
 * Used for text fields where you want analyzed/tokenized matching.
 *
 * Example: Find products with "wireless headphones" in description
 * This will match documents containing "wireless" OR "headphones"
 *
 * @author Amit Tiwari
 */
public class MatchQueryBuilder extends QueryBuilder {

    private final String field;
    private final String text;
    private Operator operator = Operator.OR;
    private Analyzer analyzer;

    /**
     * Operator for combining terms in a match query
     */
    public enum Operator {
        OR,     // Any term matches (more results)
        AND     // All terms must match (fewer, more precise results)
    }

    public MatchQueryBuilder(String field, String text) {
        this.field = field;
        this.text = text;
        this.analyzer = new StandardAnalyzer(); // Default analyzer
    }

    public MatchQueryBuilder operator(Operator operator) {
        this.operator = operator;
        return this;
    }

    public MatchQueryBuilder analyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    @Override
    public Query toLuceneQuery() {
        try {
            // Analyze the text into terms
            TokenStream tokenStream = analyzer.tokenStream(field, text);
            CharTermAttribute termAttr = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();

            BooleanQuery.Builder boolBuilder = new BooleanQuery.Builder();
            BooleanClause.Occur occur = operator == Operator.AND ?
                BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD;

            // Add each analyzed term
            while (tokenStream.incrementToken()) {
                String term = termAttr.toString();
                boolBuilder.add(new TermQuery(new Term(field, term)), occur);
            }

            tokenStream.close();

            Query query = boolBuilder.build();

            // Apply boost if set
            if (boost != 1.0f) {
                return new org.apache.lucene.search.BoostQuery(query, boost);
            }

            return query;

        } catch (IOException e) {
            throw new RuntimeException("Failed to analyze text: " + text, e);
        }
    }

    @Override
    public ValidationResult validate() {
        ValidationResult.Builder builder = new ValidationResult.Builder();

        // Check if field is provided
        if (field == null || field.trim().isEmpty()) {
            builder.addError("field", "Field name is required for match query");
        }

        // Check if text is provided
        if (text == null || text.trim().isEmpty()) {
            builder.addError("text", "Search text is required for match query");
        }

        // Warning for very long queries
        if (text != null && text.length() > 1000) {
            builder.addWarning("text",
                "Search text is very long (" + text.length() + " chars). Consider breaking it down for better performance.");
        }

        // Warning for keyword-like fields
        if (field != null && (field.equals("id") || field.equals("category") || field.equals("status"))) {
            builder.addWarning(field,
                "Field '" + field + "' looks like a keyword field. Consider using TermQueryBuilder for exact matching.");
        }

        return builder.build();
    }

    @Override
    public String toString() {
        return String.format("MatchQuery{field='%s', text='%s', operator=%s, boost=%.2f}",
            field, text, operator, boost);
    }

    @Override
    public String toJson() {
        return String.format(
            "{\"type\":\"match\",\"field\":\"%s\",\"text\":\"%s\",\"operator\":\"%s\",\"boost\":%.2f}",
            field, text, operator, boost
        );
    }

    @Override
    public String getQueryType() {
        return "match";
    }

    // Getters

    public String getField() {
        return field;
    }

    public String getText() {
        return text;
    }

    public Operator getOperator() {
        return operator;
    }
}

