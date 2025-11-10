package io.synapsedb.core.analysis.filter;

import java.util.List;

/**
 * Interface for filtering/transforming tokens
 * Filters can lowercase, remove stopwords, stem, etc.
 *
 * @author Amit Tiwari
 */
public interface TokenFilter {

    /**
     * Filter and transform tokens
     *
     * @param tokens input tokens
     * @return filtered/transformed tokens
     */
    List<String> filter(List<String> tokens);

    /**
     * Get the name of this filter
     *
     * @return filter name
     */
    String getName();
}

