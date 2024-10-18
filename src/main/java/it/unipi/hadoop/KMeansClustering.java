package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class KMeansClustering {
    private static Point[] centroids;
    private static int k;
    private static int d;
    private static int n;
    private static double threshold;
    private static int maxIterations;
    private static int reducers;
    private static final String CENTROIDS_FILE = "centroids.txt";

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        parseInput(args);
        conf.setInt("k", k); // We add k to the configuration because it is needed by the mapper task
        conf.setInt("d", d); // We add d to the configuration because it is needed by the mapper task
        Point[] oldCentroids = new Point[k];
        try {
            initializeRandomCentroids(k, d, conf, args[6]);
            for (int i = 0; i < k; i++)
                System.out.println(centroids[i]);
            System.out.println("#########################################################################################################################################################################");

            long startTime = System.currentTimeMillis(); // Used to measure the execution time of the entire program
            int iteration = 0; // Used to count the iterations
            Job job;
            while (iteration++ < maxIterations) {
                writeCentroidsToFile(conf);
                job = createJob(conf);
                // Define I/O
                FileInputFormat.addInputPath(job, new Path(args[6]));
                String outputPath = args[7] + "_" + iteration;
                FileOutputFormat.setOutputPath(job, new Path(outputPath));
                if (job.waitForCompletion(true)) // It starts the job and waits for its completion
                    System.out.println("########################################################################## ITERATION " + iteration + " COMPLETED! ##########################################################################");
                else {
                    System.out.println("########################################################################## ITERATION " + iteration + " FAILED! ##########################################################################");
                    System.exit(1);
                }
                System.arraycopy(centroids, 0, oldCentroids, 0, k);
                readComputedCentroids(reducers, outputPath, conf); // Read the centroids computed by the current MapReduce job execution
                for (int i = 0; i < k; i++)
                    System.out.println(centroids[i]);
                System.out.println("#########################################################################################################################################################################");
                if (checkThreshold(oldCentroids, centroids)) // error check
                    break;
            }
            long executionTime = System.currentTimeMillis() - startTime;
            System.out.println("Execution time in ms: " + executionTime);
            for (int i = 0; i < k; i++)
                System.out.println(centroids[i]);
            System.out.println("#########################################################################################################################################################################");

            // The following piece of code has been used during testing to write the execution time to an external file
            /*fc = FileContext.getFileContext(conf);
            filePath = new Path("test_output_kmeansclustering.txt");
            try (FSDataOutputStream fin = fc.create(filePath, EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND))) {
                fin.writeBytes("" + executionTime + ',' + n + ',' + k + ',' + d + ',' + iteration + ',' + reducers + '\n');
            } catch (IOException e) {
                throw new RuntimeException(e);
            }*/

        } catch (IllegalArgumentException e) {
            if (e.getMessage().equals(("NaN_ERROR"))) // If this error occurs, it means that a centroid has no points near to it --> The program exits and the application must be  restarted
                System.err.println("Bad values for centroids! Restart the application!");
            System.exit(1);
        }
    }

    public static int[] generateIndex() { // This method is used to randomly extract k indexes that correspond to the positions of the points in the dataset that will be used as initial centroids
        int[] random_indexes = new int[k];
        ArrayList<Integer> stream = ThreadLocalRandom.current().ints(0, n).distinct().limit(k).boxed().sorted().collect(Collectors.toCollection(ArrayList::new));
        for (int i = 0; i < k; i++) {
            random_indexes[i] = stream.get(i);
        }
        return random_indexes;
    }

    private static void parseInput(String[] args) { // This method parses the strings passed by command line and performs some initializations
        if (args.length != 8) {
            System.err.println("Usage: KMeansClustering <k> <d> <n> <threshold> <max_iterations> <reducers> <input> <output>");
            System.exit(1);
        }
        // print arguments
        System.out.println("args[0]: <k>=" + args[0]);
        System.out.println("args[1]: <d>=" + args[1]);
        System.out.println("args[2]: <n>=" + args[2]);
        System.out.println("args[3]: <threshold>=" + args[3]);
        System.out.println("args[4]: <max_iterations>=" + args[4]);
        System.out.println("args[5]: <reducers>=" + args[5]);
        System.out.println("args[6]: <input>=" + args[6]);
        System.out.println("args[7]: <output>=" + args[7]);
        // initialize variables
        k = new Integer(args[0]);
        centroids = new Point[k];
        d = new Integer(args[1]);
        n = new Integer(args[2]);
        threshold = new Double(args[3]);
        maxIterations = new Integer(args[4]);
        reducers = new Integer(args[5]);
    }

    private static Job createJob(Configuration conf) throws IOException { // This method creates a MapReduce job setting all the needed types
        Job job = Job.getInstance(conf, "kmeansclustering");
        job.setJarByClass(KMeansClustering.class);
        // Set mapper and reducer
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setNumReduceTasks(reducers);
        // job.setMapOutputKeyClass(IntWritable.class); // This is not necessary because K2 and K3 are the same
        // job.setMapOutputValueClass(Point.class);  // This is not necessary because V2 and V3 are the same
        // Define reducer's output key-value
        job.setOutputKeyClass(IntWritable.class); // K3
        job.setOutputValueClass(Point.class); // V3
        // Set input format
        job.setInputFormatClass(TextInputFormat.class); // Input key Type: LongWritable (K1); Input value type: Text (V1)
        return job;
    }

    // The following method reads the centroid computed by a MapReduce job form the proper output directory
    private static void readComputedCentroids(int nreducers, String outputPath, Configuration conf) throws UnsupportedFileSystemException {
        for (int i = 0; i < nreducers; i++) {
            // Build output path of output file
            String path = outputPath + "/part-r-" + String.format("%05d", i);
            // Read centroids from the output file: format of each line: "index   point"
            FileContext fc = FileContext.getFileContext(conf);
            Path filePath = new Path(path);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fc.open(filePath)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] splits = line.split("\t");
                    centroids[new Integer(splits[0])] = new Point(splits[1], d);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // This method saves the centroids on file in HDFS (used by the map task)
    private static void writeCentroidsToFile(Configuration configuration) throws UnsupportedFileSystemException {
        FileContext fc = FileContext.getFileContext(configuration);
        Path filePath = new Path(CENTROIDS_FILE);
        try (FSDataOutputStream fin = fc.create(filePath, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE))) {
            for (Point centroid : centroids) {
                fin.writeBytes(centroid.toString() + '\n');
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // The following method is used to randomly initialize the centroids from the points in the dataset
    private static void initializeRandomCentroids(int k, int d, Configuration conf, String input) throws UnsupportedFileSystemException {
        FileContext fc = FileContext.getFileContext(conf);
        Path filePath = new Path(input);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fc.open(filePath)))) {
            String line;
            int centroidIndex = 0, point = 0;

            int[] randomIndexes = generateIndex();

            while ((line = reader.readLine()) != null && centroidIndex < k) {
                if (randomIndexes[centroidIndex] == point++) {
                    centroids[centroidIndex++] = new Point(line, d);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // This method checks if the change in the centroids between two subsequent iterations is under the threshold or not
    public static boolean checkThreshold(Point[] oldCentroids, Point[] newCentroids) {
        double error;
        for (int i = 0; i < oldCentroids.length; i++) {
            error = oldCentroids[i].calculateDistanceSquared(newCentroids[i]);
            if (error > threshold) {
                // saveErrorToFile(error, conf);
                System.out.println("Error : " + error);
                return false;
            }
        }
        //saveErrorToFile(error, conf);
        return true;
    }

    /*
    // This method has been used to write the error to a file during the test phase
    public static void saveErrorToFile(double error, Configuration conf) throws UnsupportedFileSystemException {
        FileContext fc = FileContext.getFileContext(conf);
        Path filePath = new Path("ErrorLog.txt");
        try (FSDataOutputStream fin = fc.create(filePath, EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND))) {
            fin.writeBytes(n + " ");
            fin.writeBytes(d + " ");
            fin.writeBytes(k + " ");
            fin.writeBytes(error + "\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }*/
}
