package io.synapsedb.core.search;

import io.synapsedb.core.analysis.Analyzer;
import io.synapsedb.core.analysis.analyser.StandardAnalyzer;
import io.synapsedb.core.document.Document;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Inverted index for full-text search capabilities
 *
 * @author Amit Tiwari
 */
public class FullTextIndex {

    private final Map<String, Map<String, Set<String>>> fieldIndexes;
    private final Map<String, Document> documents;
    private final Pattern tokenPattern;
    private final Set<String> stopWords;
    private Analyzer analyzer;

    public FullTextIndex() {
        this(new StandardAnalyzer());
    }

    public FullTextIndex(Analyzer analyzer) {
        this.fieldIndexes = new ConcurrentHashMap<>();
        this.documents = new ConcurrentHashMap<>();
        this.tokenPattern = Pattern.compile("\\W+");
        this.stopWords = initializeStopWords();
        this.analyzer = analyzer;
    }

    /**
     * Set the analyzer for text processing
     */
    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    /**
     * Get current analyzer
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * Index a document for full-text search
     */
    public void indexDocument(Document document, List<String> searchableFields) {
        documents.put(document.getId(), document);

        for (String field : searchableFields) {
            Object value = document.getField(field);
            if (value != null) {
                String text = value.toString();
                Set<String> tokens = tokenize(text);

                Map<String, Set<String>> fieldIndex =
                        fieldIndexes.computeIfAbsent(field, k -> new ConcurrentHashMap<>());

                for (String token : tokens) {
                    fieldIndex.computeIfAbsent(token, k -> ConcurrentHashMap.newKeySet())
                            .add(document.getId());
                }
            }
        }
    }

    /**
     * Remove document from index
     */
    public void removeDocument(String docId) {
        documents.remove(docId);

        for (Map<String, Set<String>> fieldIndex : fieldIndexes.values()) {
            for (Set<String> docIds : fieldIndex.values()) {
                docIds.remove(docId);
            }
        }
    }

    /**
     * Search for documents containing the query text in specified field
     */
    public SearchResult search(String field, String queryText, SearchOptions options) {
        Set<String> queryTokens = tokenize(queryText);
        Map<String, Double> scores = new HashMap<>();

        Map<String, Set<String>> fieldIndex = fieldIndexes.get(field);
        if (fieldIndex == null) {
            return new SearchResult(Collections.emptyList(), 0);
        }

        // Calculate TF-IDF scores
        int totalDocs = documents.size();

        for (String token : queryTokens) {
            Set<String> matchingDocs = fieldIndex.get(token);
            if (matchingDocs == null || matchingDocs.isEmpty()) {
                continue;
            }

            double idf = Math.log((double) totalDocs / matchingDocs.size());

            for (String docId : matchingDocs) {
                Document doc = documents.get(docId);
                if (doc != null) {
                    double tf = calculateTermFrequency(doc, field, token);
                    double score = tf * idf;
                    scores.merge(docId, score, Double::sum);
                }
            }
        }

        // Sort by score and apply pagination
        List<ScoredDocument> results = scores.entrySet().stream()
                .map(e -> new ScoredDocument(documents.get(e.getKey()), e.getValue()))
                .sorted(Comparator.comparingDouble(ScoredDocument::getScore).reversed())
                .skip(options.offset)
                .limit(options.limit)
                .collect(Collectors.toList());

        return new SearchResult(results, scores.size());
    }

    /**
     * Search across multiple fields
     */
    public SearchResult searchMultipleFields(List<String> fields, String queryText, SearchOptions options) {
        Map<String, Double> aggregatedScores = new HashMap<>();

        for (String field : fields) {
            SearchResult fieldResult = search(field, queryText,
                    new SearchOptions(0, Integer.MAX_VALUE));

            for (ScoredDocument scoredDoc : fieldResult.getResults()) {
                aggregatedScores.merge(scoredDoc.getDocument().getId(),
                        scoredDoc.getScore(), Double::sum);
            }
        }

        List<ScoredDocument> results = aggregatedScores.entrySet().stream()
                .map(e -> new ScoredDocument(documents.get(e.getKey()), e.getValue()))
                .sorted(Comparator.comparingDouble(ScoredDocument::getScore).reversed())
                .skip(options.offset)
                .limit(options.limit)
                .collect(Collectors.toList());

        return new SearchResult(results, aggregatedScores.size());
    }

    /**
     * Phrase search - documents must contain exact phrase
     */
    public SearchResult phraseSearch(String field, String phrase, SearchOptions options) {
        String[] words = tokenPattern.split(phrase.toLowerCase());
        Set<String> candidates = null;

        Map<String, Set<String>> fieldIndex = fieldIndexes.get(field);
        if (fieldIndex == null) {
            return new SearchResult(Collections.emptyList(), 0);
        }

        // Find documents containing all words
        for (String word : words) {
            if (stopWords.contains(word)) continue;

            Set<String> wordDocs = fieldIndex.get(word);
            if (wordDocs == null || wordDocs.isEmpty()) {
                return new SearchResult(Collections.emptyList(), 0);
            }

            if (candidates == null) {
                candidates = new HashSet<>(wordDocs);
            } else {
                candidates.retainAll(wordDocs);
            }
        }

        if (candidates == null || candidates.isEmpty()) {
            return new SearchResult(Collections.emptyList(), 0);
        }

        // Verify exact phrase in candidates
        List<ScoredDocument> results = candidates.stream()
                .map(documents::get)
                .filter(Objects::nonNull)
                .filter(doc -> containsPhrase(doc, field, phrase))
                .map(doc -> new ScoredDocument(doc, 1.0))
                .skip(options.offset)
                .limit(options.limit)
                .collect(Collectors.toList());

        return new SearchResult(results, results.size());
    }

    private boolean containsPhrase(Document doc, String field, String phrase) {
        Object value = doc.getField(field);
        if (value == null) return false;
        return value.toString().toLowerCase().contains(phrase.toLowerCase());
    }

    private double calculateTermFrequency(Document doc, String field, String term) {
        Object value = doc.getField(field);
        if (value == null) return 0.0;

        String text = value.toString().toLowerCase();

        // Tokenize the text and count occurrences of the term
        String[] tokens = tokenPattern.split(text);
        int count = 0;

        for (String token : tokens) {
            if (token.equals(term)) {
                count++;
            }
        }

        return count;
    }

    private Set<String> tokenize(String text) {
        // Use analyzer for advanced text processing
        List<String> tokens = analyzer.analyze(text);
        return new HashSet<>(tokens);
    }

    private Set<String> initializeStopWords() {
        return Set.of(
                "a", "an", "and", "are", "as", "at", "be", "by", "for",
                "from", "has", "he", "in", "is", "it", "its", "of", "on",
                "that", "the", "to", "was", "will", "with"
        );
    }

    public static class SearchOptions {
        public final int offset;
        public final int limit;

        public SearchOptions(int offset, int limit) {
            this.offset = offset;
            this.limit = limit;
        }
    }

    public static class SearchResult {
        private final List<ScoredDocument> results;
        private final int totalMatches;

        public SearchResult(List<ScoredDocument> results, int totalMatches) {
            this.results = results;
            this.totalMatches = totalMatches;
        }

        public List<ScoredDocument> getResults() { return results; }
        public int getTotalMatches() { return totalMatches; }
    }

    public static class ScoredDocument {
        private final Document document;
        private final double score;

        public ScoredDocument(Document document, double score) {
            this.document = document;
            this.score = score;
        }

        public Document getDocument() { return document; }
        public double getScore() { return score; }
    }
}
