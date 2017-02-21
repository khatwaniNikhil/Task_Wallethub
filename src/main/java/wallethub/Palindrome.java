package wallethub;

public class Palindrome {

    public static boolean isPalindrome(String str) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException();
        } else if (str.length() == 1) {
            return true;
        } else {
            char[] array = str.toCharArray();
            for (int start = 0, end = str.length() - 1; start <= end; start++, end--) {
                if (array[start] != array[end]) {
                    return false;
                }
            }
            return true;
        }
    }

}
