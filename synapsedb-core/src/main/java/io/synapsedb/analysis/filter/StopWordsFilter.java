package io.synapsedb.analysis.filter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Removes common stop words that don't add meaning
 * Example: ["the", "cat", "is", "running"] -> ["cat", "running"]
 *
 * @author Amit Tiwari
 */
public class StopWordsFilter implements TokenFilter {

    private final Set<String> stopWords;

    // Common English stop words
    private static final Set<String> DEFAULT_STOP_WORDS = new HashSet<>(Arrays.asList(
        "a", "an", "and", "are", "as", "at", "be", "but", "by",
        "for", "if", "in", "into", "is", "it",
        "no", "not", "of", "on", "or", "such",
        "that", "the", "their", "then", "there", "these",
        "they", "this", "to", "was", "will", "with"
    ));

    public StopWordsFilter() {
        this.stopWords = DEFAULT_STOP_WORDS;
    }

    public StopWordsFilter(Set<String> customStopWords) {
        this.stopWords = customStopWords;
    }

    @Override
    public List<String> filter(List<String> tokens) {
        return tokens.stream()
                .filter(token -> !stopWords.contains(token.toLowerCase()))
                .collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "stop_words";
    }
}

