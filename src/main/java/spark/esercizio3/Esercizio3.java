package spark.esercizio3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.OneToCount;
import spark.utils.RDDprinter;
import util.DurationPrinter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

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

		JavaRDD<String> input = sparkContext.textFile(inputFileReceipt);

		// TODO: VEDERE SE SU CLUSTER FUNZIONA
		final int rowsNumber = (int) input.count();

		JavaPairRDD<Tuple2<String, String>, Integer> onesCouples = input.flatMapToPair(new LineToOnesWithCouples());
		//		RDDprinter.printSampleRDD(onesCouples); // [((pane,latte),1), ((latte,pane),1)]

		JavaPairRDD<Tuple2<String, String>, Integer> countCouples = onesCouples.reduceByKey(new OneToCount());
		//		RDDprinter.printSampleRDD(countCouples); // [((pane,vino),51678), ((insalata,formaggio),51601)]

		JavaPairRDD<String, Tuple2<String, Integer>> firstElementPair = countCouples.mapToPair(new CountToPairs());
		//		RDDprinter.printSampleRDD(firstElementPair); // [(pane,(vino,51678)), (insalata,(formaggio,51601))]

		JavaPairRDD<String, Integer> onesSingles = sparkContext.textFile(inputFileReceipt)
				.flatMapToPair(new LineToOne1());
		//		RDDprinter.printSampleRDD(onesSingles); // [(pane,1), (latte,1)]

		JavaPairRDD<String, Integer> countSingles = onesSingles.reduceByKey(new OneToCount());
		//		RDDprinter.printSampleRDD(countSingles); // [(latte,246110), (vino,245011)]

		JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> join = firstElementPair.join(countSingles);
		//		RDDprinter.printSampleRDD(join); // [(latte,((dolce,51628),246110)), (latte,((formaggio,51961),246110))]

		JavaPairRDD<String, String> result = join.sortByKey().mapToPair(new JoinToResult(rowsNumber));
		//		RDDprinter.printSampleRDD(result); // [(latte,dolce,5.16,20.98), (latte,formaggio,5.20,21.11)]

		return result;
	}

	private static final class JoinToResult
			implements PairFunction<Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>>, String, String> {
		private final int rowsNumber;
		private static final long serialVersionUID = 1L;

		private JoinToResult(int rowsNumber) {
			this.rowsNumber = rowsNumber;
		}

		@Override
		public Tuple2<String, String> call(Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>> s) {
			double pairsCount = (double) s._2._1._2;
			double itemCount = (double) s._2._2;

			DecimalFormat df = new DecimalFormat("####0.00");

			String support = df.format((pairsCount / rowsNumber) * 100);
			String confidence = df.format((pairsCount / itemCount) * 100);

			return new Tuple2<>(s._1 + "," + s._2._1._1, support + "," + confidence);
		}
	}

	private static final class CountToPairs
			implements PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> s) {
			return new Tuple2<>(s._1._1, new Tuple2<>(s._1._2, s._2));
		}
	}

	private static final class LineToOne1 implements PairFlatMapFunction<String, String, Integer> {
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
	}

	private static final class LineToOnesWithCouples
			implements PairFlatMapFunction<String, Tuple2<String, String>, Integer> {
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
	}

}
