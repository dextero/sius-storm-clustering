package pl.edu.agh.sius.clustering;

import java.util.Arrays;

public class CharacteristicVector implements Cloneable {
    public enum Status {
        Sporadic,
        Normal
    }

    public enum Density {
        Sparse,
        Transitional,
        Dense
    }

    public Status status;
    public long timeLastUpdated = 0;
    public double timeLastRemovedSporadic = 0.0;
    public double density = 0.0;
    public int[] position;
    public DoubleRange[] boundingBox;
    public int cluster = NO_CLASS;

    public static final int NO_CLASS = -1;

    public CharacteristicVector(int[] position) {
        this.position = position;
    }

    public void update(long timestamp) {
        assert position != null;

//        double oldDensity = density;
//        long oldTime = timeLastUpdated;
        density = densityAtTime(timestamp);
        timeLastUpdated = timestamp;

//        System.out.println("update " + position[0] + "," + position[1] + ": density " + oldDensity + " -> " + density + ", time = " + timeLastUpdated + ", was " + oldTime);
    }

    public double densityAtTime(long timestamp) {
        return Math.pow(Constants.DECAY_FACTOR, timestamp - timeLastUpdated) * density + 1.0;
    }

    public Density getDensityLevel() {
        if (density < Constants.SPARSE_DENSITY_LIMIT) {
            return Density.Sparse;
        } else if (density < Constants.DENSE_DENSITY_LIMIT) {
            return Density.Transitional;
        } else {
            return Density.Dense;
        }
    }

    public void update(CharacteristicVector cv) {
        status = cv.status;
        timeLastUpdated = cv.timeLastUpdated;
        timeLastRemovedSporadic = cv.timeLastRemovedSporadic;
        density = cv.density;
        position = cv.position;
        boundingBox = cv.boundingBox;
    }

    @Override
    public String toString() {
        return Arrays.toString(position) +
                ", density=" + density +
                ", cluster=" + cluster;
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            return null;
        }
    }
}
