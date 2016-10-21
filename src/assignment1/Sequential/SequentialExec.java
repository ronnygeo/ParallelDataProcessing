package assignment1.Sequential;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by ronnygeo on 9/24/16.
 */
/*
The Sequential Execution class runs the program in sequential order
without using any threads. It iterates through each records and updates
the object with the value.
 */
public class SequentialExec {
    public static void main (String[] args) {
        List<String> lines;
        boolean delay = false;
        StationData sd = new StationData();
        //Checking if the delay argument is provided
        if (args.length > 1 && args[1].equals("--delay")) {
            delay = true;
        }
        long startTime;
        try {
            startTime = System.currentTimeMillis();
            lines = new ArrayList<>();
            Scanner reader = new Scanner(new File(args[0]));

            while (reader.hasNext()) {
                String line = reader.nextLine();
                String[] words = line.split(",");
                //System.out.println(Arrays.toString(words));
                String stationId = words[0];
                String type = words[2];

                if (type != null && words[3] != null && type.equals("TMAX")) {
                    float value = Float.parseFloat(words[3]);
                    if (delay) {
                        Fibonacci.fibo(17);
                    }
                    sd.addTMAXForStation(stationId, value);
                }
            }
            System.out.println("Run Time for sequential: " + (System.currentTimeMillis() - startTime) + "ms");
            sd.printData();
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }

    }
}
