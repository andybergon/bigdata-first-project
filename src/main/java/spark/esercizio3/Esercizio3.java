package spark.esercizio3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import util.DurationPrinter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function2;

public class Esercizio3 implements Serializable {
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.err.println("Usage:  spark-submit ... jar <input_file> <output_folder>");
			System.exit(1);
		}

		long startTime = new Date().getTime();

		SparkConf sparkConf = new SparkConf().setAppName("Esercizio3");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		String inputFileReceipt = args[0];
		String outputFolderPath = args[1];

		JavaPairRDD<String, String> result = calculateResult(sparkContext, inputFileReceipt);
		DurationPrinter.printElapsedTimeWithMessage(startTime, "Time to create RDD");

		FileUtils.deleteDirectory(new File(outputFolderPath));
		result.saveAsTextFile(outputFolderPath);
		DurationPrinter.printElapsedTimeWithMessage(startTime, "Time to complete Job");

		sparkContext.close();
	}

	private static JavaPairRDD<String, String> calculateResult(JavaSparkContext sparkContext, String inputFileReceipt) {

		// vedere come fare il parsing
		// VEDERE SE SU CLUSTER FUNZIONA COUNT ROWS
		final int rowsNumber = (int) sparkContext.textFile(inputFileReceipt).count();
		System.out.println("Total Rows: " + rowsNumber);

		JavaPairRDD<Tuple2<String, String>, Integer> ones = sparkContext.textFile(inputFileReceipt)
				.flatMapToPair(new PairFlatMapFunction<String, Tuple2<String, String>, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Tuple2<String, String>, Integer>> call(String rec) {
						List<Tuple2<Tuple2<String, String>, Integer>> results = new ArrayList<Tuple2<Tuple2<String, String>, Integer>>();
						String[] tokens = StringUtils.split(rec, ",");
						for (int i = 1; i < tokens.length; i++) {
							for (int j = 1; j < tokens.length; j++) {
								if (i != j) {
									results.add(new Tuple2<>(new Tuple2<>(tokens[i], tokens[j]), 1));
								}
							}
						}
						return results;
					}
				});

		JavaPairRDD<Tuple2<String, String>, Integer> counts = ones
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		JavaPairRDD<String, Tuple2<String, Integer>> firstRDD = counts.mapToPair(
				new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> s) {
						return new Tuple2<>(s._1._1, new Tuple2<>(s._1._2, s._2));
					}
				});

		//

		JavaPairRDD<String, Integer> ones1 = sparkContext.textFile(inputFileReceipt)
				.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, Integer>> call(String rec) {
						List<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>();
						String[] tokens = StringUtils.split(rec, ",");
						for (int i = 1; i < tokens.length; i++) {
							results.add(new Tuple2<String, Integer>(tokens[i], 1)); //genero solo (pane,1) senza data
						}
						return results;
					}
				});
		JavaPairRDD<String, Integer> secondRDD = ones1.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> join = firstRDD.join(secondRDD);
		// genera (pane, (latte,1), contPane) 

		JavaPairRDD<String, String> result = join.mapToPair(
				new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>>, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>> s) {
						double pairsCount = (double) s._2._1._2;
						double itemCount = (double) s._2._2;

						String support = Double.toString((pairsCount / rowsNumber) * 100);
						String confidence = Double.toString((pairsCount / itemCount) * 100);

						return new Tuple2<>(s._1 + "," + s._2._1._1, support + "," + confidence);

					}
				});
		return result;
	}

}
