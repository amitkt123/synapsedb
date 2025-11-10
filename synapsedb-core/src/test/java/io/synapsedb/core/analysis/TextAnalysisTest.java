package io.synapsedb.core.analysis;

import io.synapsedb.core.analysis.analyser.LemmatizationAnalyzer;
import io.synapsedb.core.analysis.analyser.StandardAnalyzer;
import io.synapsedb.core.analysis.analyser.StemmingAnalyzer;
import io.synapsedb.core.analysis.stemmer.Lemmatizer;
import io.synapsedb.core.analysis.stemmer.PorterStemmer;
import io.synapsedb.core.analysis.stemmer.SimpleStemmer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for text analysis
 *
 * @author Amit Tiwari
 */
class TextAnalysisTest {

    @Test
    @DisplayName("Porter Stemmer should reduce words to root form")
    void testPorterStemmer() {
        PorterStemmer stemmer = new PorterStemmer();

        assertEquals("run", stemmer.stem("running"));
        assertEquals("cat", stemmer.stem("cats"));
        assertEquals("fish", stemmer.stem("fishing"));
        assertEquals("happi", stemmer.stem("happiness")); // Porter produces "happi"
        assertEquals("nation", stemmer.stem("national"));
    }

    @Test
    @DisplayName("Simple Stemmer should remove common suffixes")
    void testSimpleStemmer() {
        SimpleStemmer stemmer = new SimpleStemmer();

        assertEquals("runn", stemmer.stem("running")); // Simple stemmer just removes "ing"
        assertEquals("cat", stemmer.stem("cats"));
        assertEquals("fish", stemmer.stem("fishing"));
        assertEquals("quick", stemmer.stem("quickly"));
    }

    @Test
    @DisplayName("Lemmatizer should use dictionary for irregular words")
    void testLemmatizer() {
        Lemmatizer lemmatizer = new Lemmatizer();

        // Regular words (fall back to stemming)
        assertEquals("run", lemmatizer.stem("running"));

        // Irregular verbs
        assertEquals("be", lemmatizer.stem("was"));
        assertEquals("be", lemmatizer.stem("were"));
        assertEquals("go", lemmatizer.stem("went"));
        assertEquals("have", lemmatizer.stem("had"));

        // Irregular nouns
        assertEquals("child", lemmatizer.stem("children"));
        assertEquals("foot", lemmatizer.stem("feet"));
        assertEquals("goose", lemmatizer.stem("geese"));

        // Irregular adjectives
        assertEquals("good", lemmatizer.stem("better"));
        assertEquals("good", lemmatizer.stem("best"));
    }

    @Test
    @DisplayName("Standard Analyzer should tokenize and lowercase")
    void testStandardAnalyzer() {
        Analyzer analyzer = new StandardAnalyzer();

        List<String> tokens = analyzer.analyze("The CATS are Running!");

        assertEquals(4, tokens.size());
        assertTrue(tokens.contains("the"));
        assertTrue(tokens.contains("cats"));
        assertTrue(tokens.contains("are"));
        assertTrue(tokens.contains("running"));
    }

    @Test
    @DisplayName("Stemming Analyzer should apply stemming")
    void testStemmingAnalyzer() {
        Analyzer analyzer = new StemmingAnalyzer();

        List<String> tokens = analyzer.analyze("The cats are running");

        // Stop words removed: "the", "are"
        // Stemmed: "cats" -> "cat", "running" -> "run"
        assertTrue(tokens.contains("cat"));
        assertTrue(tokens.contains("run"));
        assertFalse(tokens.contains("the"));
        assertFalse(tokens.contains("are"));
    }

    @Test
    @DisplayName("Lemmatization Analyzer should apply lemmatization")
    void testLemmatizationAnalyzer() {
        Analyzer analyzer = new LemmatizationAnalyzer();

        List<String> tokens = analyzer.analyze("The children were running");

        // Stop words removed: "the", "were"
        // Lemmatized: "children" -> "child", "running" -> "run"
        assertTrue(tokens.contains("child"));
        assertTrue(tokens.contains("run"));
        assertFalse(tokens.contains("the"));
    }

    @Test
    @DisplayName("Should handle empty and null input")
    void testEdgeCases() {
        Analyzer analyzer = new StandardAnalyzer();

        assertTrue(analyzer.analyze("").isEmpty());
        assertTrue(analyzer.analyze(null).isEmpty());
        assertTrue(analyzer.analyze("   ").isEmpty());
    }

    @Test
    @DisplayName("Stemming should improve search recall")
    void testSearchRecall() {
        Analyzer stemming = new StemmingAnalyzer();

        // Document contains "running"
        List<String> docTokens = stemming.analyze("I am running fast");

        // Query is "run"
        List<String> queryTokens = stemming.analyze("run");

        // Both should contain "run" after stemming
        assertTrue(docTokens.contains("run"));
        assertTrue(queryTokens.contains("run"));
    }

    @Test
    @DisplayName("Lemmatization should handle irregular forms")
    void testIrregularForms() {
        Analyzer lemmatization = new LemmatizationAnalyzer();

        // These irregular forms should map to same lemma
        List<String> tokens1 = lemmatization.analyze("was");
        List<String> tokens2 = lemmatization.analyze("were");
        List<String> tokens3 = lemmatization.analyze("is");

        // "was", "were", "is" are stop words, so they get removed
        // Test with actual words that aren't stop words
        List<String> tokens4 = lemmatization.analyze("children");
        List<String> tokens5 = lemmatization.analyze("better");

        assertTrue(tokens4.contains("child"));
        assertTrue(tokens5.contains("good"));
    }
}

