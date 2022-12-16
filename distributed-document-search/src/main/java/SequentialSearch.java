import model.DocumentData;
import search.TFIDF;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SequentialSearch {

    public static final String BOOKS_DIRECTORY = "./resources/books";
    public static final String SEARCH_QUERY_1 = "The best detective that catches many criminals using his deductive methods";
    public static final String SEARCH_QUERY_2 = "The girl that falls through a rabbit hole into a fantasy wonderland";
    public static final String SEARCH_QUERY_3 = "A war between Russian and France in the cold winter";

    public static void main(String[] args) throws FileNotFoundException {
        var startTime = System.nanoTime();
        File documentsDir = new File(BOOKS_DIRECTORY);

        List<String> documentPaths = Arrays.asList(documentsDir.list())
                .stream()
                .map(docName -> BOOKS_DIRECTORY + "/" + docName)
                .collect(Collectors.toList());
        List<String> terms = TFIDF.getWordsFromLine(SEARCH_QUERY_1);
        findMostRelevantDocuments(terms, documentPaths);
        var timeTaken = System.nanoTime() - startTime;
        System.out.println("Time taken (ns): " + timeTaken);
    }

    private static void findMostRelevantDocuments(List<String> terms, List<String> documentPaths) throws FileNotFoundException {
        Map<String, DocumentData> docPathToDocData = new HashMap<>();

        // create a DocumentData obj for each document
        for (String docPath: documentPaths) {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(docPath));
            List<String> lines = bufferedReader.lines().collect(Collectors.toList());
            List<String> words = TFIDF.getWordsFromLines(lines);
            DocumentData docData = TFIDF.createDocumentDataForTermsFromDocText(terms, words);
            docPathToDocData.put(docPath, docData);
        }

        // perform TF-IDF
        Map<Double, List<String>> documentsSortedByScore =
                TFIDF.getDocumentsSortedByScore(terms, docPathToDocData);
        printResult(documentsSortedByScore);
    }

    private static void printResult(Map<Double, List<String>> documentsSortedByScore) {
        for (Map.Entry<Double, List<String>> entry: documentsSortedByScore.entrySet()) {
            double score = entry.getKey();
            for (String docPath: entry.getValue()) {
                System.out.println(String.format("Book: %s - Score: %f", docPath.split("/")[3], score));
            }
        }
    }


}
