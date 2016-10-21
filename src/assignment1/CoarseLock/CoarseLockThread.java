package assignment1.CoarseLock;

import java.util.List;

/**
 * Created by ronnygeo on 9/24/16.
 */
/*
* The Coarse Lock Thread class synchonizes the call the update the
* temperature in the provided StationData. This blocks the execution
* for all the thread while StationData is being updated. */
public class CoarseLockThread extends Thread {

    private String threadName;
    private Thread t;
    private final StationData sd;
    private List<String> inputData;
    private boolean delay;

    public CoarseLockThread(String name, StationData data, List<String> input, Boolean delay) {
        threadName = name;
        sd = data;
        inputData = input;
        this.delay = delay;
    }

    @Override
    public void run() {
        //For each line in input data
        for (Object l : inputData) {
            String line = (String) l;
            // Split the line into words
            String[] words = line.split(",");
            String stationId = words[0];
            String type = words[2];

            if (type != null && words[3] != null && type.equals("TMAX")) {
                float value = Float.parseFloat(words[3]);
                //Add data to the Station Data object synchronously
                synchronized (sd) {
                    if (delay) {
                        sd.addTMAXForStationDelayed(stationId, value);
                    } else {
                        sd.addTMAXForStation(stationId, value);
                    }
                }
            }
        }
    }

    public void start() {
        // If the thread is not initialized, start a new thread
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }
}
