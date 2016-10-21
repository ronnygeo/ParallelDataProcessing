package assignment1.CoarseLock;

/**
 * Created by ronnygeo on 9/25/16.
 */
public class Fibonacci {
    public static void fibo(int number) {
        int a = 0;
        int b = 1;
        int c;
        for (int i = 2; i < number; i++) {
            c = a + b;
            a = b;
            b = c;
        }
    }
}
