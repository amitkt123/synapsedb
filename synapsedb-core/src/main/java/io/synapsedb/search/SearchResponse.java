package io.synapsedb.search;

import io.synapsedb.document.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the response from a search request.
 * Contains documents, total hits, and execution metadata.
 *
 * @author Amit Tiwari
 */
public class SearchResponse {

    private final List<Document> documents;
    private final long totalHits;
    private final long tookMillis;
    private final boolean timedOut;
    private final List<String> errors;
    private final List<String> warnings;

    private SearchResponse(Builder builder) {
        this.documents = builder.documents;
        this.totalHits = builder.totalHits;
        this.tookMillis = builder.tookMillis;
        this.timedOut = builder.timedOut;
        this.errors = builder.errors;
        this.warnings = builder.warnings;
    }

    // Getters

    public List<Document> getDocuments() {
        return Collections.unmodifiableList(documents);
    }

    public long getTotalHits() {
        return totalHits;
    }

    public long getTookMillis() {
        return tookMillis;
    }

    public boolean isTimedOut() {
        return timedOut;
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public List<String> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    public List<String> getWarnings() {
        return Collections.unmodifiableList(warnings);
    }

    public int getReturnedHits() {
        return documents.size();
    }

    @Override
    public String toString() {
        return "SearchResponse{" +
                "totalHits=" + totalHits +
                ", returnedHits=" + documents.size() +
                ", tookMillis=" + tookMillis +
                ", timedOut=" + timedOut +
                ", errors=" + errors.size() +
                '}';
    }

    /**
     * Builder for SearchResponse
     */
    public static class Builder {
        private List<Document> documents = new ArrayList<>();
        private long totalHits = 0;
        private long tookMillis = 0;
        private boolean timedOut = false;
        private List<String> errors = new ArrayList<>();
        private List<String> warnings = new ArrayList<>();

        public Builder documents(List<Document> documents) {
            this.documents = documents;
            return this;
        }

        public Builder addDocument(Document doc) {
            this.documents.add(doc);
            return this;
        }

        public Builder totalHits(long totalHits) {
            this.totalHits = totalHits;
            return this;
        }

        public Builder tookMillis(long tookMillis) {
            this.tookMillis = tookMillis;
            return this;
        }

        public Builder timedOut(boolean timedOut) {
            this.timedOut = timedOut;
            return this;
        }

        public Builder addError(String error) {
            this.errors.add(error);
            return this;
        }

        public Builder addWarning(String warning) {
            this.warnings.add(warning);
            return this;
        }

        public SearchResponse build() {
            return new SearchResponse(this);
        }
    }
}

