package spark.topproducts;

import org.apache.spark.api.java.*;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TopProducts {
	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}

		String logFile = args[0];
		SparkConf sparkConf = new SparkConf().setAppName(TopProducts.class.getSimpleName());
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sparkContext.textFile(logFile, 1);
		//		lines = lines.cache(); // persist RDD with default storage level

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				return Arrays.asList(Pattern.compile(" ").split(s));
			}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		List<Tuple2<String, Integer>> output = counts.collect();
		
		System.out.println("##### RESULTS #####");
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		sparkContext.stop();

		//		long numAs = lines.filter(new Function<String, Boolean>() {
		//			public Boolean call(String s) {
		//				return s.contains("a");
		//			}
		//		}).count();
		//
		//		long numBs = lines.filter(new Function<String, Boolean>() {
		//			public Boolean call(String s) {
		//				return s.contains("b");
		//			}
		//		}).count();

		//		String output = "Lines with a: " + numAs + ", lines with b: " + numBs;
		System.out.println(output);
	}
}
