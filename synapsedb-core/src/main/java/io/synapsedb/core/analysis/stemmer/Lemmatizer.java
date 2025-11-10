package io.synapsedb.core.analysis.stemmer;

import java.util.HashMap;
import java.util.Map;

/**
 * Lemmatization - reduces words to their dictionary base form
 * More accurate than stemming but requires a dictionary
 *
 * Examples:
 * - running -> run
 * - better -> good
 * - was -> be
 * - feet -> foot
 * - geese -> goose
 *
 * This implementation uses a built-in dictionary of common irregular words
 *
 * @author Amit Tiwari
 */
public class Lemmatizer implements Stemmer {

    private final Map<String, String> lemmaDict;
    private final Stemmer fallbackStemmer;

    public Lemmatizer() {
        this.lemmaDict = buildLemmaDictionary();
        this.fallbackStemmer = new PorterStemmer();
    }

    @Override
    public String stem(String word) {
        if (word == null || word.isEmpty()) {
            return word;
        }

        String lowercase = word.toLowerCase();

        // Check dictionary first
        if (lemmaDict.containsKey(lowercase)) {
            return lemmaDict.get(lowercase);
        }

        // Fall back to stemming for regular words
        return fallbackStemmer.stem(lowercase);
    }

    @Override
    public String getName() {
        return "lemmatizer";
    }

    /**
     * Build dictionary of irregular word forms
     */
    private Map<String, String> buildLemmaDictionary() {
        Map<String, String> dict = new HashMap<>();

        // Irregular verbs
        dict.put("was", "be");
        dict.put("were", "be");
        dict.put("been", "be");
        dict.put("am", "be");
        dict.put("is", "be");
        dict.put("are", "be");
        dict.put("had", "have");
        dict.put("has", "have");
        dict.put("did", "do");
        dict.put("does", "do");
        dict.put("done", "do");
        dict.put("went", "go");
        dict.put("gone", "go");
        dict.put("got", "get");
        dict.put("gotten", "get");
        dict.put("saw", "see");
        dict.put("seen", "see");
        dict.put("came", "come");
        dict.put("took", "take");
        dict.put("taken", "take");
        dict.put("made", "make");
        dict.put("said", "say");
        dict.put("told", "tell");
        dict.put("thought", "think");
        dict.put("found", "find");
        dict.put("gave", "give");
        dict.put("given", "give");
        dict.put("left", "leave");
        dict.put("felt", "feel");
        dict.put("kept", "keep");
        dict.put("held", "hold");
        dict.put("brought", "bring");
        dict.put("began", "begin");
        dict.put("begun", "begin");
        dict.put("ran", "run");
        dict.put("wrote", "write");
        dict.put("written", "write");
        dict.put("stood", "stand");
        dict.put("heard", "hear");
        dict.put("let", "let");
        dict.put("meant", "mean");
        dict.put("set", "set");
        dict.put("met", "meet");
        dict.put("sat", "sit");
        dict.put("spoke", "speak");
        dict.put("spoken", "speak");
        dict.put("lay", "lie");
        dict.put("lain", "lie");
        dict.put("led", "lead");
        dict.put("read", "read");
        dict.put("grew", "grow");
        dict.put("grown", "grow");
        dict.put("drew", "draw");
        dict.put("drawn", "draw");
        dict.put("fell", "fall");
        dict.put("fallen", "fall");
        dict.put("chose", "choose");
        dict.put("chosen", "choose");
        dict.put("flew", "fly");
        dict.put("flown", "fly");
        dict.put("forgot", "forget");
        dict.put("forgotten", "forget");
        dict.put("wore", "wear");
        dict.put("worn", "wear");
        dict.put("broke", "break");
        dict.put("broken", "break");
        dict.put("drove", "drive");
        dict.put("driven", "drive");

        // Irregular nouns
        dict.put("children", "child");
        dict.put("feet", "foot");
        dict.put("teeth", "tooth");
        dict.put("geese", "goose");
        dict.put("mice", "mouse");
        dict.put("men", "man");
        dict.put("women", "woman");
        dict.put("people", "person");
        dict.put("oxen", "ox");
        dict.put("sheep", "sheep");
        dict.put("deer", "deer");
        dict.put("fish", "fish");

        // Adjectives/Adverbs
        dict.put("better", "good");
        dict.put("best", "good");
        dict.put("worse", "bad");
        dict.put("worst", "bad");
        dict.put("more", "much");
        dict.put("most", "much");
        dict.put("less", "little");
        dict.put("least", "little");
        dict.put("further", "far");
        dict.put("furthest", "far");
        dict.put("farther", "far");
        dict.put("farthest", "far");

        return dict;
    }
}

