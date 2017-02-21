package wallethub;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

/**
 * Approach:
 * 
 * 1) Iterate lines of file
 * 
 * 2) Split lines and flatten using flatmap
 * 
 * 3) Build phrase frequency map using collect
 * 
 * 4) Store Top K Phrases in minHeap/priority queue
 * 
 * @author nikhil
 */
public class TopKFrequentPhrases {
  final int k;
  Map<String, Integer> frequencyMap;
  final PriorityQueue<WordFreq> topKMinHeap;
  final Path inputFile;

  public TopKFrequentPhrases(int k, Path inputFile) {
    this.k = k;
    this.frequencyMap = new HashMap<String, Integer>();
    this.topKMinHeap = new PriorityQueue<WordFreq>();
    this.inputFile = inputFile;
  }

  public WordFreq[] topKPhrases(Path filePath) throws IOException, InterruptedException {
    LineIterator it = FileUtils.lineIterator(filePath.toFile(), "UTF-8");
    int i = 0;
    try {

      String line;
      while (it.hasNext()) {
        line = it.nextLine();
        i++;
        // if (i == 10000) {
        // i = 0;
        // Thread.sleep(100);
        // }
        // do something with line
        // System.out.println(i++);
      }
    } finally {
      LineIterator.closeQuietly(it);
      System.out.println("linecount: " + i);
    }
    return null;
  }

  private void topKHeap() {
    for (Map.Entry<String, Integer> entry : frequencyMap.entrySet()) {
      WordFreq wordFreq = new WordFreq(entry.getKey(), entry.getValue().intValue());
      if (topKMinHeap.size() < k) {
        topKMinHeap.add(wordFreq);
      } else if (entry.getValue().intValue() > topKMinHeap.peek().freq) {
        topKMinHeap.poll();
        topKMinHeap.add(wordFreq);
      }
    }
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    Path filePath =
        FileSystems.getDefault().getPath("/Users/nikhil/Desktop/wallethub_test_files", "fileCopy");
    TopKFrequentPhrases frequentPhrases = new TopKFrequentPhrases(3, filePath);
    final WordFreq[] topK = frequentPhrases.topKPhrases(filePath);


    // for (WordFreq word : topK) {
    // System.out.println(word.getWord() + ":" + word.getFreq());
    // }
  }
}
