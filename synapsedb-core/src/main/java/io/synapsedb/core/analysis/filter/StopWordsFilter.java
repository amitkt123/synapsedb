package io.synapsedb.core.analysis.filter;

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
            "for", "if", "in", "into", "is", "it", "its", "of", "on", "or",
            "such", "that", "the", "their", "then", "there", "these", "they",
            "this", "to", "was", "will", "with", "from", "were", "been",
            "have", "has", "had", "do", "does", "did", "can", "could",
            "shall", "should", "would", "may", "might", "must", "about",
            "above", "after", "before", "between", "through", "during",
            "over", "under", "again", "further", "than", "once", "no",
            "not", "own", "same", "so", "too", "very", "just", "also",
            "because", "while", "where", "when", "why", "how", "all",
            "any", "both", "each", "few", "more", "most", "other",
            "some", "such", "only", "too", "very", "just", "own", "same",
            "i", "me", "my", "myself", "we", "our", "ours", "ourselves",
            "you", "your", "yours", "yourself", "yourselves", "he", "him",
            "his", "himself", "she", "her", "hers", "herself", "itself",
            "they", "them", "their", "theirs", "themselves", "what",
            "which", "who", "whom", "whose"
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

