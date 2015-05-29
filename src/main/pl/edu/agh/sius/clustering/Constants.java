package pl.edu.agh.sius.clustering;

public class Constants {
    public static final double DECAY_FACTOR = 0.97;
    public static final int DIM_SIZE = 5;
    public static final int DIM_CUBES_PER_BOLT = 10;

    public static final double DIMENSION_SIZE = 1.0;
    public static final double MIN_VARIANCE = 0.001;
    public static final double MAX_VARIANCE = 0.05;

    public static final int TOTAL_CUBES_PER_SPACE = DIM_SIZE * DIM_CUBES_PER_BOLT;
    public static final double DENSITY_MIN = 3000.0 / (double)(TOTAL_CUBES_PER_SPACE * TOTAL_CUBES_PER_SPACE);
    public static final double CUBE_SIZE = Constants.DIMENSION_SIZE / ((double) (Constants.TOTAL_CUBES_PER_SPACE));
}
