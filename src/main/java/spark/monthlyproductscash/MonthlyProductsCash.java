package spark.monthlyproductscash;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class MonthlyProductsCash {
	public static void main(String[] args) {

		String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
		SparkConf conf = new SparkConf().setAppName("Simple Application");
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

		String output = "Lines with a: " + numAs + ", lines with b: " + numBs;
		System.out.println(output);
	}
}