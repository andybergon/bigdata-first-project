package spark.topproducts;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class TopProducts {
	public static void main(String[] args) {

		// va a cercare dentro /home/<user>/
		String logFile = "Desktop/spesa.txt"; // Should be some file on your LOCAL(?) system

		SparkConf conf = new SparkConf().setAppName(TopProducts.class.getSimpleName());

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> logData = sc.textFile(logFile).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("a");
			}
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("b");
			}
		}).count();

		System.out.println("##### RESULTS #####");
		String output = "Lines with a: " + numAs + ", lines with b: " + numBs;
		System.out.println(output);
	}
}
