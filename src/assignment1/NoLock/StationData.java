package assignment1.NoLock;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by ronnygeo on 9/24/16.
 */
/*
* The Station Data object stores the values for each station.
* A Key is the station id and the value is a Temperature Data object that stores the temperature info
 */
public class StationData {
    private HashMap<String, TemperatureData> sd = new HashMap<>();

    public StationData () {}

    public double getAverageForStation(String stationId) {
        return sd.get(stationId).getAverage();

    }

    //Adds the given TMAX to the object and updates the values
    public void addTMAXForStation(String stationId, float TMAX) {
        TemperatureData td = sd.get(stationId);
        if (td == null) {
            td = new TemperatureData(0, 0);
            sd.put(stationId, td);
        }
            td.setTotalTMAX(td.getTotalTMAX() + TMAX);
            td.setCountTMAX(td.getCountTMAX() + 1);
            td.updateAverage();
    }

    //Method that adds a Fibonacci delay to the update
    public void addTMAXForStationDelayed(String stationId, float TMAX) {
        Fibonacci.fibo(17);
        addTMAXForStation(stationId, TMAX);
    }


    // Method to update the Temperature Data with another Temperature data
    public void updateTemperatureDataForStation(String stationId, TemperatureData t) {
        TemperatureData td = sd.get(stationId);
        if (td == null) {
            td = t;
            sd.put(stationId, td);
        }   else {
            td.updateDataFromTemperatureData(t);
        }
    }

    // Gets the Temperature Data for a particular station ID.
    public TemperatureData getTemperatureData(String stationId) {
        return sd.get(stationId);
    }

    // Get all the keys of the Object.
    public Set getKeys() {
        return sd.keySet();
    }

    // Update the Object with another StationData
    public synchronized void updateFromStationData(StationData s) {
        for (Object k: s.getKeys()) {
            TemperatureData td = s.getTemperatureData((String) k);
            updateTemperatureDataForStation((String) k, td);
        }
    }

    //Print the StationId and the Average TMAX at that station.
    public synchronized void printData() {
        System.out.println("Size of data: " + sd.keySet().size() + "\n");
        for (Map.Entry<String, TemperatureData> entry: sd.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().getAverage());
        }

    }

}
