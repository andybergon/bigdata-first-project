package spark.esercizio2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.LineToPair;
import spark.OneToCount;
import spark.utils.RDDprinter;
import util.DurationPrinter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function2;

public class Esercizio2 implements Serializable {
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws IOException {
		if (args.length < 3) {
			System.err.println(
					"Usage:  spark-submit ... jar <input_file_spesa.txt> <input_file_prices.txt> <output_folder>");
			System.exit(1);
		}

		long startTime = System.currentTimeMillis();

		SparkConf sparkConf = new SparkConf().setAppName("Esercizio2");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		String inputFileReceipt = args[0];
		String inputFilePrices = args[1];
		String outputFolderPath = args[2];

		JavaPairRDD<String, List<Tuple2<String, Integer>>> result = calculateResult(sparkContext, inputFileReceipt,
				inputFilePrices);
		DurationPrinter.printElapsedTimeWithMessage(startTime, "Time to create RDD");

		FileUtils.deleteDirectory(new File(outputFolderPath));
		result.saveAsTextFile(outputFolderPath);
		DurationPrinter.printElapsedTimeWithMessage(startTime, "Time to complete Job");

		sparkContext.close();
	}

	private static JavaPairRDD<String, List<Tuple2<String, Integer>>> calculateResult(JavaSparkContext sparkContext,
			String inputFileReceipt, String inputFilePrices) {

		JavaPairRDD<String, Integer> ones = sparkContext.textFile(inputFileReceipt).flatMapToPair(new LineToPair());
		//		RDDprinter.printSampleRDD(ones); // [(2015-08,pane,1), (2015-08,latte,1)]

		JavaPairRDD<String, Integer> prices = sparkContext.textFile(inputFilePrices).mapToPair(new LinesToPrices());
		//		RDDprinter.printSampleRDD(prices); // [(pesce,6), (formaggio,4)]

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new OneToCount());
		//		RDDprinter.printSampleRDD(counts); // [(2015-03,latte,21139), (2015-02,formaggio,18834)]

		JavaPairRDD<String, String> aggregate = counts.sortByKey().mapToPair(new CountsToAggregate());
		//		RDDprinter.printSampleRDD(aggregate); // [(latte,2015-03:21139), (formaggio,2015-02:18834)]

		JavaPairRDD<String, String> reduced = aggregate.reduceByKey(new AggregateToReduced());
		//		RDDprinter.printSampleRDD(reduced); // [(latte,2015-03:21139 2015-10:20784,...),(...)]

		JavaPairRDD<String, List<Tuple2<String, Integer>>> date2list = reduced.mapToPair(new ReducedToResult());
		//		RDDprinter.printSampleRDD(date2list); // [(latte,([(2015-01,20951), (2015-02,20251), ...])), (vino,([(2015-01,20433), (2015-01,19171), …])]

		JavaPairRDD<String, Tuple2<List<Tuple2<String, Integer>>, Integer>> join = date2list.join(prices);
		//		RDDprinter.printSampleRDD(join); // [(latte,([(2015-03,21139), (2015-10,20784)]), 2)]

		JavaPairRDD<String, List<Tuple2<String, Integer>>> result = join.sortByKey().mapToPair(new JoinToResult());
		//		RDDprinter.printSampleRDD(result); // [(latte,[(2015-03,42278), (2015-10,41568))]), (vino,...)])

		return result;
	}

	private static final class JoinToResult implements
			PairFunction<Tuple2<String, Tuple2<List<Tuple2<String, Integer>>, Integer>>, String, List<Tuple2<String, Integer>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, List<Tuple2<String, Integer>>> call(
				Tuple2<String, Tuple2<List<Tuple2<String, Integer>>, Integer>> s) {
			List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
			int prezzo = s._2._2;
			for (Tuple2<String, Integer> tupla : s._2._1) {
				list.add(new Tuple2<>(tupla._1, tupla._2 * prezzo));
			}
			//riempire la lista con la moltiplicazione
			return new Tuple2<>(s._1, list);
		}
	}

	private static final class ReducedToResult
			implements PairFunction<Tuple2<String, String>, String, List<Tuple2<String, Integer>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, List<Tuple2<String, Integer>>> call(Tuple2<String, String> s) {
			List<Tuple2<String, Integer>> lista = new ArrayList<Tuple2<String, Integer>>();
			for (String item_count : s._2.split(" ")) {
				String date = item_count.split(":")[0]; //data
				int count = Integer.parseInt(item_count.split(":")[1]); //contatore
				lista.add(new Tuple2<>(date, count));
			}
			return new Tuple2<>(s._1, lista);
		}
	}

	private static final class AggregateToReduced implements Function2<String, String, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String call(String s1, String s2) {
			return s1 + " " + s2;
		}
	}

	private static final class CountsToAggregate implements PairFunction<Tuple2<String, Integer>, String, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, String> call(Tuple2<String, Integer> s) {
			String date = s._1.split(",")[0]; //splitto sulla virgola per ottenere la data
			String date_count = date + ":" + s._2; // creo la string "item contatore"
			return new Tuple2<>(s._1.split(",")[1], date_count); // ("item", "data:cont")              
		}
	}

	private static final class LinesToPrices implements PairFunction<String, String, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Integer> call(String s) {
			String item = s.split(",")[0];
			int price = Integer.parseInt(s.split(",")[1]);
			return new Tuple2<>(item, price);
		}
	}

}
