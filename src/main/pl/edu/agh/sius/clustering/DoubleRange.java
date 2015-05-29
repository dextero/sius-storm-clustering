package pl.edu.agh.sius.clustering;

import java.io.Serializable;

public class DoubleRange implements Serializable {
    public final double min;
    public final double max;

    public DoubleRange(double min,
                       double max) {
        assert min < max;

        this.min = min;
        this.max = max;
    }

    @Override
    public String toString() {
        return "[" +  min + ", " + max + ")";
    }
}
