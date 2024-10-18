package it.unipi.hadoop;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class Point implements Writable {
    private ArrayWritable coordinates = new ArrayWritable(DoubleWritable.class); // Using ArrayWritable class simplifies the serialization and deserialization of arrays
    private IntWritable numberOfPoints = new IntWritable(1); // this field corresponds to the number of points "accumulated" in the current object (because a point can be the sum of more points)

    public Point() {
    } // Necessary for Hadoop

    /*
     Point(int d) takes an integer parameter d, which represents the dimensionality of the point in a multidimensional space.
     It initializes the coordinates of the point with all zeros and sets the number of points to 0.
    */
    public Point(int d) {
        DoubleWritable[] coords = new DoubleWritable[d];
        for (int i = 0; i < d; i++) {
            coords[i] = new DoubleWritable(0);
        }
        coordinates.set(coords);
        numberOfPoints = new IntWritable(0);
    }

    /*
     Point(String line, int d) is used to create a Point object by parsing a string line that contains comma-separated coordinate values.
     It also takes an integer d representing the dimensionality of the point. This constructor splits the string,
     converts the splits to double, and initializes the Point object with the parsed coordinates.
    */
    public Point(String line, int d) {
        if (!line.isEmpty()) {
            String[] coordinatesAux = line.split(",");
            // We assume that the line contains the exact number of coordinates d
            DoubleWritable[] _coordinates = new DoubleWritable[d];
            for (int i = 0; i < d; i++) {
                double temp = Double.parseDouble(coordinatesAux[i]);
                if (!Double.isNaN(temp)) {
                    _coordinates[i] = new DoubleWritable(temp);
                } else {
                    throw new IllegalArgumentException("NaN_ERROR");
                }
            }
            coordinates = new ArrayWritable(DoubleWritable.class, _coordinates);
        } else
            throw new IllegalArgumentException("Error during parsing of string! Empty line!");
    }

    public DoubleWritable[] getCoordinates() {
        Writable[] coordsW = coordinates.get();
        DoubleWritable[] coordsDW = new DoubleWritable[coordsW.length];
        for (int i = 0; i < coordsW.length; i++) {
            coordsDW[i] = (DoubleWritable) coordsW[i];
        }
        return coordsDW;
    }

    public void setCoordinates(DoubleWritable[] coordinates) {
        this.coordinates.set(coordinates);
    }

    private int getNumberOfPoints() {
        return numberOfPoints.get();
    }

    private void setNumberOfPoints(int numberOfPoints) {
        this.numberOfPoints = new IntWritable(numberOfPoints);
    }

    // The following method calculates the squared Euclidean distance between the current point and another point.
    public double calculateDistanceSquared(Point otherPoint) {
        double distanceSquared = 0;
        double diff;
        DoubleWritable[] coordFirstPoint = getCoordinates();
        DoubleWritable[] coordOtherPoint = otherPoint.getCoordinates();
        for (int i = 0; i < coordFirstPoint.length; i++) {
            diff = coordFirstPoint[i].get() - coordOtherPoint[i].get();
            distanceSquared += diff * diff;
        }
        return distanceSquared;
    }

    public void sumPoint(Point p) { // This method sums to the current point the point p (increasing properly the numberOfPoints field)
        int totalNumberOfPoints = this.getNumberOfPoints();
        DoubleWritable[] coordCurrentPoint = getCoordinates();
        DoubleWritable[] coordOtherPoint = p.getCoordinates();
        for (int i = 0; i < coordCurrentPoint.length; i++)
            coordCurrentPoint[i].set(coordCurrentPoint[i].get() + coordOtherPoint[i].get());
        totalNumberOfPoints += p.getNumberOfPoints();
        this.setNumberOfPoints(totalNumberOfPoints);
    }

    /*
     The following method, similarly to the previous one, is used to obtain a point by summing together the points in a list.
     This method allows to maintain the cumulative sum of points during the execution of the K-means algorithm.
     The number of points (numberOfPoints field) associated with a cluster is also appropriately updated.
     This method is necessary for computing the new centroids of the clusters during the iterations of the K-means algorithm.
     */
    public static Point sumPoints(Iterable<Point> list) {
        Iterator<Point> iter = list.iterator();
        Point temp;
        temp = iter.next(); // We can safely assume that there is at least one element in the list because otherwise the reduce function would not have been executed with the associated key
        DoubleWritable[] coordCurrentPoint = temp.getCoordinates();
        DoubleWritable[] coordNextPoint;
        int countNumberOfPoints = temp.getNumberOfPoints();
        while (iter.hasNext()) {
            temp = iter.next();
            coordNextPoint = temp.getCoordinates();
            for (int i = 0; i < coordCurrentPoint.length; i++)
                coordCurrentPoint[i].set(coordCurrentPoint[i].get() + coordNextPoint[i].get());
            countNumberOfPoints += temp.getNumberOfPoints();
        }
        temp.setNumberOfPoints(countNumberOfPoints);
        temp.setCoordinates(coordCurrentPoint);
        return temp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException { // Point serialization
        coordinates.write(dataOutput);
        numberOfPoints.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException { // Point deserialization
        coordinates.readFields(dataInput);
        numberOfPoints.readFields(dataInput);
    }

    /*
     The following method is used to divide the point coordinates by the number of associated points.
     This method is necessary to compute the centroids of the clusters in the k-means algorithm.
     */
    public void divideByScalar() {
        DoubleWritable[] coordPoint = getCoordinates();
        for (DoubleWritable doubleWritable : coordPoint) {
            // If a centroid has no near point, it will be a division by 0, that will give infinity (this case is admitted
            // and then managed: the application will be stopped when the driver code will read NaN as coordinates for a centroid)
            doubleWritable.set(doubleWritable.get() / getNumberOfPoints());
        }
    }

    public String toString() { // Format of toString: "coord[0],coord[1],coord[2]"
        DoubleWritable[] coordPoint = getCoordinates();
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < coordPoint.length - 1; i++)
            result.append(coordPoint[i].get()).append(",");
        result.append(coordPoint[coordPoint.length - 1]);
        return result.toString();
    }
}
