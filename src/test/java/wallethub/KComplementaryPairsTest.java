package wallethub;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import wallethub.KComplementaryPairs;
import wallethub.KComplementaryPairs.Pair;

public class KComplementaryPairsTest {

  @Test(expected = IllegalArgumentException.class)
  public void testNull() {
    KComplementaryPairs.findKComplimentaryPairs(1, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testArraySizeZero() {
    KComplementaryPairs.findKComplimentaryPairs(1, new int[0]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testArraySizeOne() {
    KComplementaryPairs.findKComplimentaryPairs(1, new int[1]);
  }

  @Test()
  public void testArraySizeTwoMatch() {
    int[] array = {1, 0};
    List<Pair> pairs = KComplementaryPairs.findKComplimentaryPairs(1, array);
    assertTrue(pairs.size() == 2);
  }


  @Test()
  public void testArraySizeTwoUnmatch() {
    int[] array = {1, 2};
    List<Pair> pairs = KComplementaryPairs.findKComplimentaryPairs(1, array);
    assertTrue(pairs.size() == 0);
  }


  @Test()
  public void testArraySizeThreeMatch() {
    int[] array = {1, 2, 2};
    List<Pair> pairs = KComplementaryPairs.findKComplimentaryPairs(3, array);
    System.out.println(pairs.size());
    assertTrue(pairs.size() == 4);
    assertTrue(Collections.frequency(pairs, new Pair(1, 2)) == 2);
    assertTrue(Collections.frequency(pairs, new Pair(2, 1)) == 2);
  }
}
