package pl.edu.agh.sius.clustering;

public class Constants {
    public static final int DIM_SIZE = 5;
    public static final int DIM_CUBES_PER_BOLT = 25;

    public static final int TOTAL_CUBES_PER_SPACE = DIM_SIZE * DIM_CUBES_PER_BOLT;

    public static final double DENSITY_MIN = 50.0 / (double)(DIM_SIZE * DIM_CUBES_PER_BOLT);
    public static final double DECAY_FACTOR = 0.99;
}
