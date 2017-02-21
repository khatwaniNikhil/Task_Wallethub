package wallethub;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wallethub.Palindrome;

public class PalindromeTest {

  @Test(expected = IllegalArgumentException.class)
  public void testEmpty() {
    Palindrome.isPalindrome("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNull() {
    Palindrome.isPalindrome(null);
  }

  @Test()
  public void testIsPalindrome() {
    assertTrue(Palindrome.isPalindrome("1234321"));
  }

  @Test()
  public void testIsNotPalindrome() {
    assertTrue(!Palindrome.isPalindrome("1234321111"));
  }
}
