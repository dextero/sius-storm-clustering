package pl.edu.agh.sius.clustering.visualizer;

import clojure.lang.ArraySeq;
import pl.edu.agh.sius.clustering.ColoredPoint;
import pl.edu.agh.sius.clustering.Main;
import pl.edu.agh.sius.clustering.PositionWrapper;

import javax.swing.*;
import java.awt.*;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.util.ArrayList;
import java.util.List;

public class Visualizer extends JFrame implements KeyListener {
    public volatile static List<List<PositionWrapper>> clusters = new ArrayList<>();
    public volatile static List<ColoredPoint> points = new ArrayList<>();
    public volatile static Visualizer self;

    public static final Color[] POINT_COLORS = new Color[] {
            Color.RED,
            Color.GREEN,
            Color.BLUE
    };
    public static final Color[] CLUSTER_COLORS = new Color[] {
            Color.PINK,
            Color.GRAY,
            Color.MAGENTA
    };

    public static int WIDTH = 1440;
    public static int HEIGHT = 900;

    private Visualizer() throws HeadlessException {
        super("Visualizer");

        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setSize(WIDTH, HEIGHT);
        addKeyListener(this);
    }

    @Override public void keyTyped(KeyEvent e) {}
    @Override public void keyReleased(KeyEvent e) {}

    @Override
    public void keyPressed(KeyEvent e) {
        if (e.getKeyCode() == KeyEvent.VK_ESCAPE) {
            dispose();
            Main.finished = true;
        }
    }

    public static void show(int foo) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                try {
                    self = new Visualizer();
                    self.setVisible(true);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        });
    }

    public int translateX(double x) {
        return (int)(x * WIDTH);
    }

    public int translateY(double y) {
        return (int)(y * HEIGHT);
    }

    @Override
    public void paint(Graphics g) {
        System.err.println("dupa = " + points.size());
        try {
            g.clearRect(0, 0, getWidth(), getHeight());

            double factor = Main.DIM_SIZE * Main.DIM_CUBES_PER_BOLT;
            int width = ((int) (WIDTH / factor));
            int height = ((int) (HEIGHT / factor));

            for (int i = 0; i < clusters.size(); ++i) {
                List<PositionWrapper> cluster = clusters.get(i);
                g.setColor(CLUSTER_COLORS[i]);
                for (PositionWrapper pos : cluster) {
                    g.fillRect(translateX(pos.pos[0] / factor),
                               translateY(pos.pos[1] / factor),
                               width, height);
                }
            }

            for (ColoredPoint point : points) {
                g.setColor(POINT_COLORS[point.cluster]);
                g.fillOval(translateX(point.pos[0]), translateY(point.pos[1]), 5, 5);
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
