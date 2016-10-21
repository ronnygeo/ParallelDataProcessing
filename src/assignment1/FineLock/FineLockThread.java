package assignment1.FineLock;

import java.util.List;

/**
 * Created by ronnygeo on 9/24/16.
 */

/*
* The Fine Lock Thread class synchonizes the call the update each value
* of a key if exists. It only locks the Data Structure at the value
* of the object. Other threads can access other Objects at other keys. */
public class FineLockThread extends Thread {

    private String threadName;
    private Thread t;
    private FineLockStationData sd;
    private List<String> inputData;

    private boolean delay;

    public FineLockThread(String name, FineLockStationData data, List<String> input, Boolean d) {
        threadName = name;
        sd = data;
        inputData = input;
        delay = d;
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
                //Add data to the Station Data object
                if (delay) {
                    sd.addTMAXForStationDelayed(stationId, value);
                } else {
                    sd.addTMAXForStation(stationId, value);
                }
            }
        }
    }

    public void start() {
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }
}
