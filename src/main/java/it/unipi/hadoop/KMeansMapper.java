package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.EnumSet;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
    private Point[] centroids;
    private Point[] cumulativePoints; // to accumulate the points associated with each centroid (each Point object counts internally the number of points "accumulated" in it)
    private int k;
    private int d;
    private long time = 0; // Used for testing (to measure map function exection time)
    private int counter;

    @Override
    public void setup(Context context) throws IOException {
        // Mapper initialization (reading list of centroids from an external file)
        Configuration conf = context.getConfiguration();
        k = conf.getInt("k", 2);
        d = conf.getInt("d", 2);
        centroids = new Point[k];
        initializeCentroids(context);
        cumulativePoints = new Point[k];
        for (int i = 0; i < k; i++) {
            cumulativePoints[i] = new Point(d);
        }
        counter = 0;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        long start = System.currentTimeMillis();

        Point point = new Point(value.toString(), d); // Create Point object
        cumulativePoints[computeNearestIndex(point)].sumPoint(point); // Accumulate the point in "cumulative point" relative to the nearest centroid

        time += (System.currentTimeMillis() - start); // To accumulate the execution time of (only) the map functions
        counter++;
    }

    @Override
    public void cleanup(Context context) throws InterruptedException, IOException { // (In-Mapper Combining)
        for (int i = 0; i < k; i++) { // Emit all the k cumulative points
            context.write(new IntWritable(i), cumulativePoints[i]);
        }

        // used in test phase to write to file the execution time of the mapper (sum of execution times of map function)
        /*FileContext fc = FileContext.getFileContext(context.getConfiguration());
        Path filePath = new Path("counter_mapper_inmapcomb" + context.getTaskAttemptID().getTaskID().toString().split("_")[4] + ".txt");
        try (FSDataOutputStream fin = fc.create(filePath, EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND))) {
            fin.writeBytes(counter + "\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/
    }

    // The following method extracts line by line all the points, corresponding to the coordinates of the centroids, from a file.
    // Each line contains the coordinates of a centroid.
    private void initializeCentroids(Context context) throws IOException {
        String hdfsFile = "centroids.txt";
        FileContext fc = FileContext.getFileContext(context.getConfiguration());
        Path filePath = new Path(hdfsFile);
        // Read the file of centroids line by line
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fc.open(filePath)))) {
            String line;
            int centroidCount = 0;
            while ((line = reader.readLine()) != null && centroidCount < k)
                centroids[centroidCount++] = new Point(line, d);
        } catch (FileNotFoundException e) {
            // Handle the error if the file of centroids is not found
            throw new IOException("Centroid file not found", e);
        }
    }

    // Method that returns the index of the centroid closest to the point passed as argument
    public int computeNearestIndex(Point point) {
        double minDistance = point.calculateDistanceSquared(centroids[0]);
        int minIndex = 0;
        for (int i = 1; i < k; i++) {
            double actualDistance = point.calculateDistanceSquared(centroids[i]);
            if (actualDistance < minDistance) {
                minDistance = actualDistance;
                minIndex = i;
            }
        }
        return minIndex;
    }
}

