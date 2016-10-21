package assignment1.NoLock;

/**
 * Created by ronnygeo on 9/24/16.
 */
/*
* Temperature Data Class stores the Total, count and the average TMAX
* for a particular station.
 */
public class TemperatureData {
    private float TotalTMAX;
    private int countTMAX;
    private double averageTMAX;
    public TemperatureData(float total, int count) {
        TotalTMAX = total;
        countTMAX = count;
        updateAverage();
    }

    public float getTotalTMAX() {
        return TotalTMAX;
    }

    public void setTotalTMAX(float totalTMAX) {
        TotalTMAX = totalTMAX;
    }

    public int getCountTMAX() {
        return countTMAX;
    }

    public void setCountTMAX(int countTMAX) {
        this.countTMAX = countTMAX;
    }

    public double getAverage() {
        return averageTMAX;
    }

    public void updateAverage() {
        this.averageTMAX = this.TotalTMAX / this.countTMAX;
    }

    //Update the Object with another Temperature Data Object
    public void updateDataFromTemperatureData(TemperatureData td) {
        setTotalTMAX(td.getTotalTMAX() + getTotalTMAX());
        setCountTMAX(td.getCountTMAX() + getCountTMAX());
        updateAverage();
    }
}
