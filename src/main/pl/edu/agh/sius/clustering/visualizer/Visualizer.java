package pl.edu.agh.sius.clustering.visualizer;

import com.badlogic.gdx.ApplicationListener;
import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.graphics.*;
import com.badlogic.gdx.graphics.Color;
import com.badlogic.gdx.graphics.g2d.Sprite;
import com.badlogic.gdx.graphics.g2d.SpriteBatch;
import pl.edu.agh.sius.clustering.ColoredPoint;
import pl.edu.agh.sius.clustering.Main;
import pl.edu.agh.sius.clustering.PositionWrapper;

import java.util.ArrayList;
import java.util.List;

public class Visualizer implements ApplicationListener {
    public volatile static List<List<PositionWrapper>> clusters = new ArrayList<>();
    public volatile static List<ColoredPoint> points = new ArrayList<>();

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

    private SpriteBatch batch;
    private Sprite pointSprite;
    private Sprite rectSprite;

    private int width = 900;
    private int height = 900;

    @Override
    public void create() {
        batch = new SpriteBatch();

        Texture pointTex = new Texture("image/point.png");
        Texture rectTex = new Texture("image/rect.png");

        pointSprite = new Sprite(pointTex);
        rectSprite = new Sprite(rectTex);

        double factor = (double)Main.DIM_SIZE * (double)Main.DIM_CUBES_PER_BOLT;
        float sizeX = translateX(1.0 / factor);
        float sizeY = translateY(1.0 / factor);

        float POINT_SIZE = 5.0f;
        pointSprite.setScale(POINT_SIZE / pointTex.getWidth(), POINT_SIZE / pointTex.getHeight());
        rectSprite.setSize(sizeX, sizeY);

        pointSprite.setOrigin(0.5f, 0.5f);

        Gdx.gl.glViewport(0, 0, width, height);
    }

    @Override
    public void dispose() {
        batch.dispose();
    }

    private float translateX(double x) {
        return (float)((double)width * x);
    }

    private float translateY(double y) {
        return (float)((double)height * y);
    }

    @Override
    public void render() {
        Gdx.gl.glClearColor(0.2f, 0.2f, 0.2f, 1);
        Gdx.gl.glClear(GL30.GL_COLOR_BUFFER_BIT);

        final float factor = (float)Main.DIM_SIZE * (float)Main.DIM_CUBES_PER_BOLT;

        batch.begin();
        for (int i = 0; i < clusters.size(); i++) {
            rectSprite.setColor(CLUSTER_COLORS[i]);

            for (PositionWrapper pos : clusters.get(i)) {
                rectSprite.setPosition(translateX((float)pos.pos[0] / factor),
                                       translateY((float)pos.pos[1] / factor));
                rectSprite.draw(batch);
            }
        }

        for (ColoredPoint point : points) {
            pointSprite.setPosition(translateX(point.pos[0]), translateY(point.pos[1]));
            pointSprite.setColor(POINT_COLORS[point.cluster]);
            pointSprite.draw(batch);
        }
        batch.end();
    }

    @Override
    public void resize(int width,
                       int height) {
//        pointSprite.setSize((float)Main.DIM_SIZE * (float)Main.DIM_CUBES_PER_BOLT / (float)width,
//                            (float)Main.DIM_SIZE * (float)Main.DIM_CUBES_PER_BOLT / (float)height);
//        rectSprite.setSize((float)Main.DIM_SIZE * (float)Main.DIM_CUBES_PER_BOLT / (float)width,
//                           (float)Main.DIM_SIZE * (float)Main.DIM_CUBES_PER_BOLT / (float)height);

        Gdx.gl.glViewport(0, 0, width, height);

        this.width = width;
        this.height = height;
    }

    @Override
    public void pause() {
        Main.finished = true;
    }

    @Override
    public void resume() {
    }
}
