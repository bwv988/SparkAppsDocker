package com.warpbreaks.io;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class ALSMovieRecommenderApp {
    private static final String APP_NAME;
    private static final String MOVIES_FILE;
    private static final String RATINGS_FILE;
    private static final String MY_RATINGS_FILE;

    private static final long SEED;
    // Declare data schema for reading CSV files efficiently.
    private final StructType moviesSchema = new StructType()
            .add("movieId", "integer")
            .add("title", "string")
            .add("genres", "string");
    private final StructType ratingsSchema = new StructType()
            .add("userId", "integer")
            .add("movieId", "integer")
            .add("rating", "double")
            .add("timestamp", "long");
    // In order to later evaluate & compare the models, we will consider the Root Mean Squared Error (RMSE).
    private RegressionEvaluator evaluator = new RegressionEvaluator()
            .setMetricName("rmse")
            .setLabelCol("rating")
            .setPredictionCol("prediction");
    private final String[] seqCol = {"prediction"};
    private String dataPath;
    private SparkSession spark;
    private Dataset<Row> moviesDF;
    private Dataset<Row> ratingsDF;
    private Dataset<Row> trainingSet;
    private Dataset<Row> validationSet;
    private Dataset<Row> testSet;

    static {
        APP_NAME = "ALSMovieRecommenderApp";
        MOVIES_FILE = "movies.csv";
        RATINGS_FILE = "ratings.csv";
        MY_RATINGS_FILE = "myratings.csv";
        SEED = 1222121193L;
    }

    public static void main(String[] args) {
        ALSMovieRecommenderApp app = new ALSMovieRecommenderApp(args);

        app.pipeline();
    }

    public ALSMovieRecommenderApp(String[] args) {
        initializeApp(args);
    }

    /* The Movie Recommender data processing pipeline. */
    public void pipeline() {
        loadData();

        printSummaries();

        splitData();

        ALSModel bestModel = trainALSModel(trainingSet, validationSet);

        assessModel(bestModel);

        predictRatings();

        cleanUp();
    }

    private void initializeApp(String[] args) {
        if (args.length != 2) {
            System.err.format("Usage: %s <spark master> <data dir path>", APP_NAME);
            System.exit(0);
        }

        String sparkMaster = args[0];
        dataPath = args[1];

        spark = SparkSession
                .builder()
                .appName(APP_NAME)
                .master(sparkMaster)
                .getOrCreate();
    }

    private void loadData() {
        moviesDF = spark.read()
                .option("header", "true")
                .schema(moviesSchema)
                .csv(dataPath + "/" + MOVIES_FILE);

        moviesDF.cache();

        ratingsDF = spark.read()
                .option("header", "true")
                .schema(ratingsSchema)
                .csv(dataPath + "/" + RATINGS_FILE);

        ratingsDF.cache();
    }

    private void printSummaries() {
        // Show the first 3 rows of the data sets.
        moviesDF.show(3);
        ratingsDF.show(3);

        // Print some count summaries.
        System.out.format("\n---> Number of observations in the movies data set: %d\n", moviesDF.count());
        System.out.format("\n---> Number of observations in the ratings data set: %d\n", ratingsDF.count());
    }

    private void splitData() {
        // First, let's split the data into training, test and validation sets.
        // For reproducibility, we'll use the same seed.
        Dataset<Row>[] splits = ratingsDF.randomSplit(new double[]{0.6, 0.2, 0.2}, SEED);
        trainingSet = splits[0];
        validationSet = splits[1];
        testSet = splits[2];
    }

    private ALSModel trainALSModel(Dataset<Row> trainingData, Dataset<Row> validationData) {
        // Build the recommendation model using ALS on the training data.
        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.1)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating")
                .setSeed(SEED);

        // In order to evaluate & compare the models, we will consider the Root Mean Squared Error (RMSE).
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");

        // Initialize the model selection variables.
        int modelIdx = 0;
        int bestRankIdx = -1;
        double minError = Double.POSITIVE_INFINITY;
        int[] ranks = new int[]{4, 8, 12};
        double[] errors = new double[ranks.length];
        ALSModel[] models = new ALSModel[ranks.length];

        // Iterate process for several ranks.
        for (int rank : ranks) {
            als.setRank(rank);

            // Build model on training data.
            ALSModel model = als.fit(trainingData);

            // Predict on validation set.
            Dataset<Row> predictions = cleanPredictions(model.transform(validationData));

            // Get the RMSE for the current model.
            double rmse = evaluator.evaluate(predictions);

            System.out.format("\n---> For rank %d, the RMSE is: %f\n", rank, rmse);

            errors[modelIdx] = rmse;
            models[modelIdx] = model;

            // Determine best model.
            if (rmse < minError) {
                minError = rmse;
                bestRankIdx = modelIdx;
            }

            modelIdx++;
        }

        System.out.format("\n\n---> RESULTS:\nRank of best model: %d. RMSE for best model: %f\n", ranks[bestRankIdx], errors[bestRankIdx]);

        // Return the best model.
        return(models[bestRankIdx]);
    }


    private  void assessModel(ALSModel bestModel) {
        // Now using the held-out data.
        Dataset<Row> testPredictions = cleanPredictions(bestModel.transform(testSet));

        System.out.format("\n---> RMSE for predictions on test set: %f\n", evaluator.evaluate(testPredictions));
    }

    private void predictRatings() {
        // Read own ratings from file.
        Dataset<Row> myRatingsDF = spark.read()
                .option("header", "true")
                .schema(ratingsSchema)
                .csv(dataPath + "/" + MY_RATINGS_FILE);

        myRatingsDF.cache();

        // Add own ratings to training set.
        Dataset<Row> newTrainingSet = trainingSet.union(myRatingsDF);

        // Re-train the model and select best model.
        ALSModel userModel = trainALSModel(newTrainingSet, validationSet);

        // Assess the model.
        assessModel(userModel);

        // ...
    }

    private  void cleanUp() {
        spark.stop();
    }

    // Drop rows with NaN in prediction column.
    // This is because of:
    // https://issues.apache.org/jira/browse/SPARK-14489
    private Dataset<Row> cleanPredictions(Dataset<Row> predictions) {
        return predictions.na().drop(seqCol);
    }
}