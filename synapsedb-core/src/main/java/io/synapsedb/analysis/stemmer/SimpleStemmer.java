package io.synapsedb.analysis.stemmer;

/**
 * Simple suffix-based stemmer
 * Removes common English suffixes in a simple, predictable way
 *
 * Examples:
 * - running -> run
 * - cats -> cat
 * - fishing -> fish
 * - quickly -> quick
 *
 * Simpler and faster than Porter, but less sophisticated
 *
 * @author Amit Tiwari
 */
public class SimpleStemmer implements Stemmer {

    private static final String[] SUFFIXES = {
        "ing", "ed", "es", "s", "ly", "er", "est", "ment", "ness", "tion", "sion"
    };

    @Override
    public String stem(String word) {
        if (word == null || word.length() <= 3) {
            return word;
        }

        word = word.toLowerCase();

        // Try to remove suffixes
        for (String suffix : SUFFIXES) {
            if (word.endsWith(suffix)) {
                String stem = word.substring(0, word.length() - suffix.length());
                // Keep result if it's at least 2 characters
                if (stem.length() >= 2) {
                    return stem;
                }
            }
        }

        return word;
    }

    @Override
    public String getName() {
        return "simple";
    }
}

