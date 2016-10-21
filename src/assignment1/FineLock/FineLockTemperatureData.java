package assignment1.FineLock;

/**
 * Created by ronnygeo on 9/24/16.
 */
/*
* Fine Lock Temperature Data Class stores the Total, count and the average TMAX
* for a particular station and uses a Fine Lock on the object.
 */
public class FineLockTemperatureData {
    private float TotalTMAX;
    private int countTMAX;
    private double averageTMAX;
    private final Object lock = new Object();
    public FineLockTemperatureData(float total, int count) {
        synchronized(lock) {
            TotalTMAX = total;
            countTMAX = count;
            updateAverage();
        }
    }

    public float getTotalTMAX() {
        return TotalTMAX;
    }

    public void setTotalTMAX(float totalTMAX) {
        synchronized (lock) {
            TotalTMAX = totalTMAX;
        }
    }

    public int getCountTMAX() {
        return countTMAX;
    }

    public void setCountTMAX(int countTMAX) {
        synchronized (this) {
        this.countTMAX = countTMAX;
    }
    }

    public double getAverage() {
        return averageTMAX;
    }

    public void updateAverage() {
        synchronized(lock) {
        this.averageTMAX = this.TotalTMAX / this.countTMAX;
    }
    }
}
