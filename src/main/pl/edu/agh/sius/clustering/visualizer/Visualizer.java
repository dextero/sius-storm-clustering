package pl.edu.agh.sius.clustering.visualizer;

import clojure.lang.ArraySeq;
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
    public volatile static List<double[]> points = new ArrayList<>();
    public volatile static Visualizer self;

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
            g.setColor(Color.BLACK);

            for (double[] point : points) {
                g.drawOval(translateX(point[0]), translateY(point[1]), 5, 5);
            }

            double factor = Main.DIM_SIZE * Main.DIM_CUBES_PER_BOLT;
            int width = ((int) (WIDTH / factor));
            int height = ((int) (HEIGHT / factor));

            for (List<PositionWrapper> cluster : clusters) {
                for (PositionWrapper pos : cluster) {
                    g.drawRect(translateX(pos.pos[0] / factor),
                               translateY(pos.pos[1] / factor),
                               width, height);
                }
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
