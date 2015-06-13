package pl.edu.agh.sius.clustering;

public class Constants {
    public static final double DECAY_FACTOR = 0.97;
    public static final int DIM_SIZE = 5;
    public static final int DIM_CUBES_PER_BOLT = 10;

    public static final double DIMENSION_SIZE = 1.0;
    public static final double MIN_VARIANCE = 0.001;
    public static final double MAX_VARIANCE = 0.05;

    public static final int TOTAL_CUBES_PER_DIM = DIM_SIZE * DIM_CUBES_PER_BOLT;
    public static final int TOTAL_CUBES_PER_SPACE = TOTAL_CUBES_PER_DIM * TOTAL_CUBES_PER_DIM;
    public static final double DENSITY_MIN = 1.0 + 0.01;
    public static final double CUBE_SIZE = Constants.DIMENSION_SIZE / ((double) (Constants.TOTAL_CUBES_PER_DIM));

    public static final double LOW_THRESHOLD_PARAMETER = 0.8;
    public static final double HIGH_THRESHOLD_PARAMETER = 3.0;
    public static final double SPARSE_DENSITY_LIMIT = LOW_THRESHOLD_PARAMETER / (TOTAL_CUBES_PER_SPACE * (1.0 - DECAY_FACTOR));
    public static final double DENSE_DENSITY_LIMIT = HIGH_THRESHOLD_PARAMETER / (TOTAL_CUBES_PER_SPACE * (1.0 - DECAY_FACTOR));
    public static final double SPORADIC_GRID_DELETE_CONSTANT = 0.1;

    public static final int MESSAGES_PER_UPDATE = 100;
}
