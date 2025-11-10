package io.synapsedb.core.analysis.filter;

import io.synapsedb.core.analysis.stemmer.Stemmer;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Token filter that applies stemming to all tokens
 * Example: ["running", "cats", "fishing"] -> ["run", "cat", "fish"]
 *
 * @author Amit Tiwari
 */
public class StemmingFilter implements TokenFilter {

    private final Stemmer stemmer;

    public StemmingFilter(Stemmer stemmer) {
        this.stemmer = stemmer;
    }

    @Override
    public List<String> filter(List<String> tokens) {
        return tokens.stream()
                .map(stemmer::stem)
                .collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "stemming_" + stemmer.getName();
    }
}

