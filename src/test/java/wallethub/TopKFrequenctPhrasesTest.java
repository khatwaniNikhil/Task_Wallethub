package wallethub;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import wallethub.TopKFrequentPhrases;
import wallethub.WordFreq;


public class TopKFrequenctPhrasesTest {

  @Test
  public void test() throws IOException, URISyntaxException {
    Path filePath = Paths.get(ClassLoader.getSystemResource("smallfile.txt").toURI());
    TopKFrequentPhrases object = new TopKFrequentPhrases(3, filePath);
    final WordFreq[] topK = object.topKPhrases(filePath);
    assertTrue(topK.length == 3);
    assertTrue(topK[2].getWord().equals("Foobar Candy "));
    assertTrue(topK[2].getFreq() == 7);

    assertTrue(topK[1].getWord().equals(" Olympics 2012 "));
    assertTrue(topK[1].getFreq() == 5);

    assertTrue(topK[0].getWord().equals(" PGA "));
    assertTrue(topK[0].getFreq() == 4);
  }

}
