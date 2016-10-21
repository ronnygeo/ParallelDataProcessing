package assignment1.NoLock;

import java.util.ConcurrentModificationException;
import java.util.List;

/**
 * Created by ronnygeo on 9/24/16.
 */
/*
The NoLockThread class do not use any kind of locks or synchronization
while updating the data.
 */
public class NoLockThread extends Thread {

    private String threadName;
    private Thread t;
    private StationData sd;
    private List<String> inputData;
    private boolean delay;

    // Threadname name, Reference StationData, Input List
    public NoLockThread(String name, StationData data, List<String> input, Boolean d) {
        threadName = name;
        sd = data;
        inputData = input;
        delay = d;
    }

    @Override
    public void run() {        //For each line in input data
        for (Object l : inputData) {
            String line = (String) l;
            // Split the line into words
            String[] words = line.split(",");
            String stationId = words[0];
            String type = words[2];

            if (type != null && words[3] != null && type.equals("TMAX")) {
                float value = Float.parseFloat(words[3]);
                //Add data to the Station Data object while checking for concurrent exception
                try {
                    if (delay) {
                        sd.addTMAXForStationDelayed(stationId, value);
                    } else {
                    sd.addTMAXForStation(stationId, value);
                }
                }catch (ConcurrentModificationException e) {
                    System.out.println(e);
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
