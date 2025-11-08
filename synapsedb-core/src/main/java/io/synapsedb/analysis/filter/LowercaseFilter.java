package io.synapsedb.analysis.filter;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Converts all tokens to lowercase
 * Example: ["Hello", "WORLD"] -> ["hello", "world"]
 *
 * @author Amit Tiwari
 */
public class LowercaseFilter implements TokenFilter {

    @Override
    public List<String> filter(List<String> tokens) {
        return tokens.stream()
                .map(token -> token.toLowerCase(Locale.ROOT))
                .collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "lowercase";
    }
}

