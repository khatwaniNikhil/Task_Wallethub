package wallethub;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.time.StopWatch;

/**
 * Approach:
 * 
 * 1) Producer to Memory Map regions of file into ConcurrentLinkedDeque<MappedByteBuffer>
 * 
 * 2) Consumer threads processes from ConcurrentLinkedDeque<MappedByteBuffer>
 * 
 * 2a) Read line by line MappedByteBuffer
 * 
 * 2b) Split line
 * 
 * 2c) compute word count in frequencyMap
 * 
 * 3) Store Top K Phrases in minHeap/priority queue
 * 
 * @author nikhil
 *
 */
public class TopKFrequentPhrasesMultiThreaded {
  final int k;
  Map<String, AtomicInteger> frequencyMap;
  final PriorityQueue<WordFreq> topKMinHeap;
  final Path inputFile;
  int numConsumerThreads;
  ConcurrentLinkedDeque<MappedByteBuffer> maps;
  long len;
  long mapChunk = 100 * 1024 * 1000; // 100MB
  AtomicInteger linesCount = new AtomicInteger(0);

  public TopKFrequentPhrasesMultiThreaded(int k, Path inputFile, int numConsumerThreads) {
    this.k = k;
    this.frequencyMap = new HashMap<String, AtomicInteger>();
    this.topKMinHeap = new PriorityQueue<WordFreq>(k, Comparator.comparingInt(wc -> wc.getFreq()));
    this.inputFile = inputFile;
    this.numConsumerThreads = numConsumerThreads;
  }

  class ProducerThread extends Thread {
    public void memorymap() throws IOException {
      RandomAccessFile raf = new RandomAccessFile(inputFile.toFile(), "rw");
      FileChannel chan = raf.getChannel();
      long t0 = System.currentTimeMillis();
      maps = new ConcurrentLinkedDeque<MappedByteBuffer>();
      len = raf.length();
      long pos = 0;
      System.out.println("length file bytes: " + len);
      while (pos < len) {
        long mappedRegionSize = Math.min(mapChunk, len - pos);
        MappedByteBuffer map;
        System.out.println("pos: " + pos + " mappedRegionSize:" + mappedRegionSize);
        map = chan.map(MapMode.READ_WRITE, pos, mappedRegionSize);
        pos += mappedRegionSize;
        System.out.println("next pos: " + pos);
        maps.add(map);
      }
      raf.close();

      long t1 = System.currentTimeMillis();
      System.out.println("memory maps count: " + maps.size());
      System.out.println("Time taken memory mapping: " + (t1 - t0) + "ms");
    }

    @Override
    public void run() {
      try {
        memorymap();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  class ConsumerThread extends Thread {
    CountDownLatch latch;

    public ConsumerThread(CountDownLatch latch) {
      super();
      this.latch = latch;
    }

    // keep on consuming buffers from Queue and work on them
    public void run() {
      BufferedReader in = null;
      while (!maps.isEmpty()) {
        try {
          MappedByteBuffer mapBuffer = maps.pollFirst();
          if (mapBuffer != null) {
            while (mapBuffer.hasRemaining()) {
              System.out.println("limit: " + mapBuffer.limit());
              System.out.println("capacity: " + mapBuffer.capacity());
              byte[] buffer = new byte[(int) mapBuffer.limit()];
              mapBuffer.get(buffer);
              in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer)));
              for (String nextLine = in.readLine(); nextLine != null; nextLine = in.readLine()) {
                linesCount.incrementAndGet();
                String[] words = nextLine.split("\\|");
                for (final String word : words) {
                  AtomicInteger value = frequencyMap.putIfAbsent(word, new AtomicInteger(1));
                  if (value != null) {
                    value.incrementAndGet();
                  }
                }

              }
            }
          }
        } catch (IOException io) {

        }
      }
      latch.countDown();
      if (in != null)
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
    }
  }

  public Thread startProducerThread() {
    Thread producer = new ProducerThread();
    producer.start();
    return producer;
  }

  public void startConsumers() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(numConsumerThreads);
    ConsumerThread[] workers = new ConsumerThread[numConsumerThreads];
    for (int i = 0; i < workers.length; i++) {
      workers[i] = new ConsumerThread(latch);
      workers[i].start();
    }
    latch.await();
  }

  public WordFreq[] topKPhrases() throws IOException, InterruptedException {

    StopWatch stopwatchHeap = new StopWatch();
    stopwatchHeap.start();
    topKHeap();
    stopwatchHeap.stop();
    System.out.println("Time taken adding topk to Heap : " + stopwatchHeap.getTime() + " ms");

    // extract the top K
    StopWatch stopwatchExtractTopK = new StopWatch();
    stopwatchExtractTopK.start();
    final WordFreq[] topK = new WordFreq[k];
    int i = 0;
    while (topKMinHeap.size() > 0) {
      topK[i++] = topKMinHeap.remove();
    }
    stopwatchExtractTopK.stop();
    System.out.println("Time taken ExtractTopK from heap: " + stopwatchExtractTopK.getTime()
        + " ms");


    return topK;
  }

  private void topKHeap() {
    for (Map.Entry<String, AtomicInteger> entry : frequencyMap.entrySet()) {
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
    StopWatch totalStopWatch = new StopWatch();
    totalStopWatch.start();
    Path filePath =
        FileSystems.getDefault().getPath("/Users/nikhil/Desktop/wallethub_test_files", "fileCopy");
    int k = 3;
    int numConsumerThreads = 3;
    TopKFrequentPhrasesMultiThreaded object =
        new TopKFrequentPhrasesMultiThreaded(k, filePath, numConsumerThreads);
    Thread producer = object.startProducerThread();
    Thread.sleep(200);
    object.startConsumers();
    System.out.println("lines count: " + object.linesCount);
    final WordFreq[] topK = object.topKPhrases();
    producer.join();
    totalStopWatch.stop();
    System.out.println("Total Time taken : " + totalStopWatch.getTime() + " ms");
    System.out.println("RESULTS:");
    for (WordFreq word : topK) {
      System.out.println(word.getWord() + ":" + word.getFreq());
    }
  }
}
