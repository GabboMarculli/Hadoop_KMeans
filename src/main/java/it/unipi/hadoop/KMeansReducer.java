package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Point> {
    public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
        Point centroid = Point.sumPoints(values); // Sum the points in the list "values" together
        centroid.divideByScalar(); // Divide the previously cumulated point (its coordinates) by the number of points "contained" in it
        context.write(key, centroid); // Emit the centroid (the key is the "index" of the centroid)
    }
}
