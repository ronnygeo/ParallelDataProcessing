package assignment1.NoSharing;

import java.io.FileNotFoundException;
import java.util.*;

/**
 * Created by ronnygeo on 9/24/16.
 */
public class NoSharingExec {

    public static void main(String[] args) throws Exception {
        boolean delay = false;
        if (args.length > 1 && args[1].equals("--delay")) {
            delay = true;
        }
        // A list of Station Data object to store the partial data from each thread
        List<StationData> partialData = new ArrayList<>();
        //Variable to store the final Station Data Object
        StationData finalData = new StationData();
        ArrayList<NoSharingThread> threads = new ArrayList<>();
        List lines;
        long startTime;
        try {
            //Getting list of lines using FileLoader class
            lines = FileLoader.load(args[0]);
            int noOfLines = lines.size();
            //Getting the number of processors to determine the thread count
            int threadCount = Runtime.getRuntime().availableProcessors();

            //Storing the initial count of active threads, before starting new one.
            int startThreadCount = Thread.activeCount();
            //Creating the indices array for splitting input
            int[] indices = new int[threadCount*2];
            indices[0] = 0;
            //Creating the index using the number of threads
            for (int i=1, j = 0; i < threadCount*2; i++) {
                if (i % 2 != 0) {
                    j++;
                }
                indices[i] = noOfLines/4 * j + 1;
            }
            indices[threadCount*2 - 1] = noOfLines;

            //Variable to store the program start time
            startTime = System.currentTimeMillis();

            //Creating threads for input split
            for (int tc = 0; tc < threadCount * 2; tc+= 2) {
                StationData partialThreadData = new StationData();
                partialData.add(partialThreadData);
                threads.add(new NoSharingThread("No Lock Thread", partialThreadData, lines.subList(indices[tc], indices[tc + 1]), delay));
            }

            //Starting each thread
            for (Thread t: threads) {
                t.start();
            }

            //Waiting for all the threads to complete
            while (Thread.activeCount() > startThreadCount) {
                for (Thread t: threads) {
                    t.join();
                }
            }

            //For each partial data in the thread, added to final data after all the threads are completed.
            for (StationData sData : partialData) {
                    finalData.updateFromStationData(sData);
            }

            System.out.println("Run time with no sharing threads: " + (System.currentTimeMillis() - startTime) + "ms");
            finalData.printData();

        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }
}
