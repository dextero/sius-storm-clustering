package pl.edu.agh.sius.clustering;

public class DoubleRange {
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
