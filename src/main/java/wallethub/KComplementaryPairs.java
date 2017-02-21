package wallethub;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.primitives.Ints;

/**
 * Approach
 * 
 * 1) Iterate array and prepare Map of word and its occurence freq
 * 
 * 2) Iterate word count map,
 * 
 * 2a) If for current elem, k-elem exists based on correponding freq of both in array, add pair
 * 
 * @author nikhil
 *
 */
public class KComplementaryPairs {

  public static class Pair {
    int i;
    int j;

    public Pair(int i, int j) {
      this.i = i;
      this.j = j;
    }


    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + i;
      result = prime * result + j;
      return result;
    }


    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Pair other = (Pair) obj;
      if (i != other.i)
        return false;
      if (j != other.j)
        return false;
      return true;
    }


    @Override
    public String toString() {
      return "Pair [i=" + i + ", j=" + j + "]";
    }

  }

  public static List<Pair> findKComplimentaryPairs(int k, int[] array) {
    if (array == null || array.length < 2) {
      throw new IllegalArgumentException();
    }

    List<Pair> pairs = new ArrayList<KComplementaryPairs.Pair>();
    ConcurrentMap<Integer, Integer> elemCountMap =
        Ints.asList(array).stream()
            .collect(Collectors.toConcurrentMap(elem -> elem, elem -> 1, Integer::sum));

    for (Map.Entry<Integer, Integer> map : elemCountMap.entrySet()) {
      Integer elem = map.getKey();
      Integer count = map.getValue();
      if (elemCountMap.containsKey(k - elem)) {
        for (int i = 1; i <= count; i++) {
          for (int j = 1; j <= elemCountMap.get(k - elem); j++) {
            Pair pair = new Pair(elem, k - elem);
            pairs.add(pair);
            // System.out.println("Pair: " + pair);
          }
        }
      }
    }
    return pairs;
  }
}
