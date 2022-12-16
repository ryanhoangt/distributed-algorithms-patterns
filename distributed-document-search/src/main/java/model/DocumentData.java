package model;

import java.util.HashMap;
import java.util.Map;

public class DocumentData {

    // map term to its frequency in the document
    private Map<String, Double> termToFrequency = new HashMap<>();

    public void putTermFrequency(String term, double freq) {
        termToFrequency.put(term, freq);
    }

    public double getFrequency(String term) {
        return termToFrequency.get(term);
    }

}
