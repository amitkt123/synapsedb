package io.synapsedb.analysis.stemmer;

/**
 * Interface for stemming algorithms
 * Reduces words to their root/stem form
 *
 * @author Amit Tiwari
 */
public interface Stemmer {

    /**
     * Stem a word to its root form
     * Example: "running" -> "run"
     *
     * @param word input word
     * @return stemmed word
     */
    String stem(String word);

    /**
     * Get the name of this stemmer
     *
     * @return stemmer name
     */
    String getName();
}

