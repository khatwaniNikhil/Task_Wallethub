package wallethub;

public class WordFreq {
  String word;
  int freq;

  public WordFreq(final String w, final int c) {
    word = w;
    freq = c;
  }

  public String getWord() {
    return word;
  }

  public int getFreq() {
    return freq;
  }

  @Override
  public String toString() {
    return "WordFreq [word=" + word + ", freq=" + freq + "]";
  }

}
