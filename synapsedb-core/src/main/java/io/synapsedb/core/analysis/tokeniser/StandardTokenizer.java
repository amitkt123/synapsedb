package io.synapsedb.core.analysis.tokeniser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Standard tokenizer that splits on whitespace and punctuation
 * Example: "Hello, World!" -> ["Hello", "World"]
 *
 * @author Amit Tiwari
 */
public class StandardTokenizer implements Tokenizer {

    private static final Pattern WORD_PATTERN = Pattern.compile("\\w+");

    @Override
    public List<String> tokenize(String text) {
        if (text == null || text.isEmpty()) {
            return new ArrayList<>();
        }

        // Split on non-word characters and filter empty strings
        return Arrays.stream(text.split("\\W+"))
                .filter(token -> !token.isEmpty())
                .collect(Collectors.toList());
    }
}

