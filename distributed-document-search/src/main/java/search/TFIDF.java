package search;

import model.DocumentData;

import java.util.*;

public class TFIDF {

    private static double calculateTFFromDocText(String term, List<String> words) {
        long count = 0;
        for (String word: words) {
            if (word.equalsIgnoreCase(term)) count++;
        }
        double tf = (double) count / words.size();
        return tf;
    }

    public static DocumentData createDocumentDataForTermsFromDocText(List<String> terms,
                                                                     List<String> words) {
        DocumentData documentData = new DocumentData();

        for (String term: terms) {
            double termFreq = calculateTFFromDocText(term, words);
            documentData.putTermFrequency(term, termFreq);
        }
        return documentData;
    }

    private static double calculateIDF(String term,
                                       Map<String, DocumentData> docNameToDocData) {
        double docCount = 0;
        for (String docName: docNameToDocData.keySet()) {
            DocumentData docData = docNameToDocData.get(docName);
            double termFreq = docData.getFrequency(term);
            if (termFreq > 0.0) {
                docCount++;
            }
        }
        return docCount == 0 ? 0 : Math.log10(docNameToDocData.size() / docCount);
    }

    private static Map<String, Double> getIDFsForTerms(List<String> terms,
                                                       Map<String, DocumentData> docNameToDocData) {
        Map<String, Double> termToIDF = new HashMap<>();
        for (String term: terms) {
            double idf = calculateIDF(term, docNameToDocData);
            termToIDF.put(term, idf);
        }
        return termToIDF;
    }

    private static double calculateDocScoreForTerms(List<String> terms,
                                                    DocumentData docData,
                                                    Map<String, Double> termToIDF) {
        double score = 0;
        for (String term: terms) {
            double tf = docData.getFrequency(term);
            double idf = termToIDF.get(term);
            score += tf * idf;
        }
        return score;
    }

    public static Map<Double, List<String>> getDocumentsSortedByScore(List<String> terms,
                                                                      Map<String, DocumentData> docNameToDocData) {
        TreeMap<Double, List<String>> scoreToDocuments = new TreeMap<>();
        Map<String, Double> termToIDF = getIDFsForTerms(terms, docNameToDocData);

        for (String docName: docNameToDocData.keySet()) {
            DocumentData docData = docNameToDocData.get(docName);
            double docScore = calculateDocScoreForTerms(terms, docData, termToIDF);
            addDocScoreToTreeMap(scoreToDocuments, docName, docScore);
        }
        return scoreToDocuments.descendingMap();
    }

    private static void addDocScoreToTreeMap(TreeMap<Double, List<String>> scoreToDocuments,
                                             String docName, double docScore) {
        List<String> docWithCurScore = scoreToDocuments.get(docScore);
        if (docWithCurScore == null) {
            docWithCurScore = new ArrayList<>();
        }
        docWithCurScore.add(docName);
        scoreToDocuments.put(docScore, docWithCurScore);
    }

    public static List<String> getWordsFromLine(String line) {
        return Arrays.asList(line.split("(\\.)+|(,)+|( )+|(-)+|(\\?)+|(!)+|(;)+|(:)+|(/d)+|(/n)+"));
    }

    public static List<String> getWordsFromLines(List<String> lines) {
        List<String> words = new ArrayList<>();
        for (String line: lines) {
            words.addAll(getWordsFromLine(line));
        }
        return words;
    }

}
