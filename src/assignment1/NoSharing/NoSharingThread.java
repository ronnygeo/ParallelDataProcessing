package assignment1.NoSharing;

import java.util.List;

/**
 * Created by ronnygeo on 9/24/16.
 */

/*
* The No Sharing Thread class uses a thread specific DataStructure to update the values.
* No other thread has access to this particular object.
* Each thread completes the work independently without affecting other threads. */
public class NoSharingThread extends Thread {

    private String threadName;
    private Thread t;
    private StationData sd;
    private List<String> inputData;
    private boolean delay;

    public NoSharingThread(String name, StationData data, List<String> input, Boolean d) {
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
                //Add data to the Station Data object synchronously
                if (delay) {
                    sd.addTMAXForStationDelayed(stationId, value);
                } else {
                sd.addTMAXForStation(stationId, value);
            }
            }
        }
//        try {
//            sd.wait();
//        } catch (Exception e) {
//            System.out.println(e);
//        }
    }

    public void start() {
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }
}
