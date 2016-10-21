package assignment1.FineLock;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ronnygeo on 9/24/16.
 */
/*
* The Fine Lock Station Data object stores the values for each station and implements a fine lock on this object
* and the value as required. Key is the station id and the value is a Temperature Data object that stores
 * the temperature info at the station. */
public class FineLockStationData {
    private HashMap<String, FineLockTemperatureData> sd = new HashMap<>();

    private static final Object lock = new Object();
    public FineLockStationData() {}

    //Adds the given TMAX to the object
    public void addTMAXForStation(String stationId, float TMAX) {
        FineLockTemperatureData td = sd.get(stationId);
        if (td == null) {
            td = new FineLockTemperatureData(0, 0);
            synchronized (lock) {
                sd.put(stationId, td);
            }
        }
            td.setTotalTMAX(td.getTotalTMAX() + TMAX);
            td.setCountTMAX(td.getCountTMAX() + 1);
            td.updateAverage();
    }

    // Add the given TMAX to the object using a fibo delay
    public void addTMAXForStationDelayed(String stationId, float TMAX) {
        FineLockTemperatureData td = sd.get(stationId);
        if (td == null) {
            td = new FineLockTemperatureData(0, 0);
            synchronized (lock) {
                Fibonacci.fibo(17);
                sd.put(stationId, td);
            }
        }
        td.setTotalTMAX(td.getTotalTMAX() + TMAX);
        td.setCountTMAX(td.getCountTMAX() + 1);
        td.updateAverage();
    }


    public void printData() {
        for (Map.Entry<String, FineLockTemperatureData> entry: sd.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().getAverage());
        }
    }

    public int getSize() {
        return sd.keySet().size();
    }
}
