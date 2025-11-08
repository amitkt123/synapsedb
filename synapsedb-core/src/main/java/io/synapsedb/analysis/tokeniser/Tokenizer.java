package io.synapsedb.analysis.tokeniser;

import java.util.List;

/**
 * Interface for tokenizing text into words/tokens
 *
 * @author Amit Tiwari
 */
public interface Tokenizer {

    /**
     * Tokenize text into individual tokens
     *
     * @param text input text
     * @return list of tokens
     */
    List<String> tokenize(String text);
}

