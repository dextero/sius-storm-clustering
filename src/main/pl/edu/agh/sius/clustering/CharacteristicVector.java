package pl.edu.agh.sius.clustering;

import java.time.Instant;

public class CharacteristicVector {
    public static final double DECAY_FACTOR = 0.9;

    enum Status {
        Sporadic,
        Normal
    }

    public Status status;
    public long timeLastUpdated = 0;
    public double timeLastRemovedSporadic = 0.0;
    public double density = 0.0;
    public int[] position;
    public DoubleRange[] boundingBox;

    public CharacteristicVector(int[] position) {
        this.position = position;
    }

    public void update(long timestamp) {
        assert position != null;

//        double oldDensity = density;
//        long oldTime = timeLastUpdated;
        density = Math.pow(DECAY_FACTOR, timestamp - timeLastUpdated) * density + 1.0;
        timeLastUpdated = timestamp;

//        System.out.println("update " + position[0] + "," + position[1] + ": density " + oldDensity + " -> " + density + ", time = " + timeLastUpdated + ", was " + oldTime);
    }
}
