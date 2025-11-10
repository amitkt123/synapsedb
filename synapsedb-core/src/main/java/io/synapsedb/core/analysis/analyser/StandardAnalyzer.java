package io.synapsedb.core.analysis.analyser;

import io.synapsedb.core.analysis.Analyzer;
import io.synapsedb.core.analysis.filter.LowercaseFilter;
import io.synapsedb.core.analysis.filter.TokenFilter;
import io.synapsedb.core.analysis.tokeniser.StandardTokenizer;
import io.synapsedb.core.analysis.tokeniser.Tokenizer;

import java.util.ArrayList;
import java.util.List;

/**
 * Standard analyzer - basic text analysis
 * Pipeline: Tokenize -> Lowercase
 *
 * Example: "Running FAST!" -> ["running", "fast"]
 *
 * @author Amit Tiwari
 */
public class StandardAnalyzer implements Analyzer {

    private final Tokenizer tokenizer;
    private final List<TokenFilter> filters;

    public StandardAnalyzer() {
        this.tokenizer = new StandardTokenizer();
        this.filters = new ArrayList<>();
        this.filters.add(new LowercaseFilter());
    }

    @Override
    public List<String> analyze(String text) {
        if (text == null || text.isEmpty()) {
            return new ArrayList<>();
        }

        // Step 1: Tokenize
        List<String> tokens = tokenizer.tokenize(text);

        // Step 2: Apply filters
        for (TokenFilter filter : filters) {
            tokens = filter.filter(tokens);
        }

        return tokens;
    }

    @Override
    public String getName() {
        return "standard";
    }
}

