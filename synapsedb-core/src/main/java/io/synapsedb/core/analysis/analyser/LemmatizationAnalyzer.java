package io.synapsedb.core.analysis.analyser;

import io.synapsedb.core.analysis.Analyzer;
import io.synapsedb.core.analysis.filter.LowercaseFilter;
import io.synapsedb.core.analysis.filter.StemmingFilter;
import io.synapsedb.core.analysis.filter.StopWordsFilter;
import io.synapsedb.core.analysis.filter.TokenFilter;
import io.synapsedb.core.analysis.stemmer.Lemmatizer;
import io.synapsedb.core.analysis.tokeniser.StandardTokenizer;
import io.synapsedb.core.analysis.tokeniser.Tokenizer;

import java.util.ArrayList;
import java.util.List;

/**
 * Lemmatization analyzer - applies dictionary-based lemmatization
 * Pipeline: Tokenize -> Lowercase -> Remove Stop Words -> Lemmatize
 *
 * Example: "The children were running better" -> ["child", "run", "good"]
 *
 * @author Amit Tiwari
 */
public class LemmatizationAnalyzer implements Analyzer {

    private final Tokenizer tokenizer;
    private final List<TokenFilter> filters;

    public LemmatizationAnalyzer() {
        this.tokenizer = new StandardTokenizer();
        this.filters = new ArrayList<>();
        this.filters.add(new LowercaseFilter());
        this.filters.add(new StopWordsFilter());
        this.filters.add(new StemmingFilter(new Lemmatizer()));
    }

    @Override
    public List<String> analyze(String text) {
        if (text == null || text.isEmpty()) {
            return new ArrayList<>();
        }

        // Step 1: Tokenize
        List<String> tokens = tokenizer.tokenize(text);

        // Step 2: Apply filters (lowercase, stop words, lemmatization)
        for (TokenFilter filter : filters) {
            tokens = filter.filter(tokens);
        }

        return tokens;
    }

    @Override
    public String getName() {
        return "lemmatization";
    }
}

