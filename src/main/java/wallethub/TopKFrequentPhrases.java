package wallethub;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.time.StopWatch;

/**
 * Read file and store word count in a map. Then prepare minHeap of size K 
 * from map. Finally extract top k from heap 
 * @author nikhil
 */
public class TopKFrequentPhrases {
    private final int topK;
    private final PriorityQueue<WordFreq> topKMinHeap;
    private Map<String, Integer> frequencyMap;

    public TopKFrequentPhrases(int k) {
        this.topK = k;
        this.frequencyMap = new HashMap<String, Integer>();
        this.topKMinHeap = new PriorityQueue<WordFreq>(k, Comparator.comparingInt(wc -> wc.getFreq()));
    }

    public WordFreq[] topKPhrasesApproach(Path filePath) throws IOException {
        StopWatch totalStopWatch = new StopWatch();
        totalStopWatch.start();
        wordCountFromFile(filePath);
        heapFromWordCountApproach();
        WordFreq[] topK = extractFromHeap();
        totalStopWatch.stop();
        System.out.println("Total Time taken : " + totalStopWatch.getTime());
        return topK;
    }

    /**
     * 
     * Iterate lines of file. While processing each line,
     * Split line into 50 phrases and update the wordcount.
     * 
     * O(numLines * lengthOfLine)
     * @param filePath
     * @throws IOException
     */
    public void wordCountFromFile(Path filePath) throws IOException {
        try (Stream<String> lines = Files.lines(filePath)) {
            Function<String, Stream<String>> lineSplitFunc = new Function<String, Stream<String>>() {
                @Override
                public Stream<String> apply(String line) {
                    return Arrays.asList(line.split("\\|")).stream();
                }
            };
            Stream<String> words = lines.flatMap(lineSplitFunc);
            frequencyMap = words.collect(Collectors.toConcurrentMap(elem -> elem, elem -> 1, Integer::sum));
        }
    }

    /**
     * Iterate wordCountMap
     * For each entry, either (add to heap) or (remove top and then add)
     * O(u(no. of unique entries in map) * logk)
     */
    private void heapFromWordCountApproach() {
        for (Map.Entry<String, Integer> entry : frequencyMap.entrySet()) {
            WordFreq wordFreq = new WordFreq(entry.getKey(), entry.getValue().intValue());
            if (topKMinHeap.size() < topK) {
                topKMinHeap.add(wordFreq);
            } else if (entry.getValue().intValue() > topKMinHeap.peek().freq) {
                topKMinHeap.poll();
                topKMinHeap.add(wordFreq);
            }
        }
    }

    // O(u(no. of unique entries in map)
    public WordFreq[] extractFromHeap() {
        final WordFreq[] topKWords = new WordFreq[topK];
        int i = 0;
        while (topKMinHeap.size() > 0) {
            topKWords[i++] = topKMinHeap.remove();
        }
        return topKWords;
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Path filePath = FileSystems.getDefault().getPath("/Users/nikhil/Desktop", "test1");
        TopKFrequentPhrases frequentPhrases = new TopKFrequentPhrases(4);
        final WordFreq[] topK1 = frequentPhrases.topKPhrasesApproach(filePath);
        for (WordFreq word : topK1) {
            System.out.println(word.getWord() + ":" + word.getFreq());
        }
    }
}
