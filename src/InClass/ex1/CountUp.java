package InClass.ex1;

import java.util.ArrayList;

public class CountUp {

    static Object object = new Object();
    static int count = 0;

    public static void main(String[] args) throws Exception {
        ArrayList<CountThread> ts = new ArrayList<CountThread>();

        for (int ii = 0; ii < 4; ++ii) {
            ts.add(new CountThread());
        }
        
        for (int ii = 0; ii < 4; ++ii) {
            ts.get(ii).start();
        }
        
        for (int ii = 0; ii < 4; ++ii) {
            ts.get(ii).join();
        }
    }

    public static void barrier() throws InterruptedException{
        // tools: synchronized, wait, notifyAll
        //Number of threads is known, constant

            synchronized (object) {
                count++;
                if (count == 4) {
                    object.notifyAll();
                    count = 0;
                }
                else
                    object.wait();

            }
        }
    }

class CountThread extends Thread {
    @Override
    public void run() {
        for (int ii = 0; ii < 5; ++ii) {
            System.out.println("" + ii);
            try {
                CountUp.barrier();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
