package io.synapsedb.analysis;

import java.util.List;

/**
 * Base interface for text analysis
 * Converts raw text into analyzed tokens
 *
 * @author Amit Tiwari
 */
public interface Analyzer {

    /**
     * Analyze text and return tokens
     *
     * @param text input text to analyze
     * @return list of analyzed tokens
     */
    List<String> analyze(String text);

    /**
     * Get the name of this analyzer
     *
     * @return analyzer name
     */
    String getName();
}

