package spark.esercizio1;

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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function2;
import org.apache.commons.io.FileUtils;

public class Esercizio1 implements Serializable {
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.err.println("Usage:  spark-submit ... jar <input_file> <output_folder>");
			System.exit(1);
		}

		long startTime = System.currentTimeMillis();

		String inputFileReceipt = args[0];
		String outputFolderPath = args[1];

		SparkConf sparkConf = new SparkConf().setAppName("Esercizio1");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		JavaPairRDD<String, List<Tuple2<String, Integer>>> result = calculateResult(sparkContext, inputFileReceipt);
		DurationPrinter.printElapsedTimeWithMessage(startTime, "Time to create RDD");

		FileUtils.deleteDirectory(new File(outputFolderPath));
		result.saveAsTextFile(outputFolderPath); // saveAsTextFile creates a NEW directory!
		DurationPrinter.printElapsedTimeWithMessage(startTime, "Time to complete Job");

		sparkContext.close();
	}

	private static JavaPairRDD<String, List<Tuple2<String, Integer>>> calculateResult(JavaSparkContext sparkContext,
			String inputFile) {

		/* Per stampare no "rdd.foreach(println)" or "rdd.map(println)"
		 * Se uso "rdd.collect().foreach(println)" per stampare il driver(locale) va out of memory.
		 * Uso: "rdd.take(100).foreach(println)", in Java8: "list.take(100).forEach(System.out::println)"
		 * */

		JavaPairRDD<String, Integer> ones = sparkContext.textFile(inputFile).flatMapToPair(new LineToPair());
		RDDprinter.printSampleRDD(ones); // [(2015-08,pane,1), (2015-08,latte,1)]

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new OneToCount());
		RDDprinter.printSampleRDD(counts); // [(2015-03,latte,21139), (2015-02,formaggio,18834)]

		JavaPairRDD<String, String> aggregate = counts.mapToPair(new CountsToAggregate());
		RDDprinter.printSampleRDD(aggregate); // [(2015-03,latte 21139), (2015-02,formaggio 18834)]

		JavaPairRDD<String, String> reduced = aggregate.reduceByKey(new AggregateToReduced());
		RDDprinter.printSampleRDD(reduced); // [(2015-05,pesce 20870, ...),(2015-07,vino 20908, ...)]

		JavaPairRDD<String, List<Tuple2<String, Integer>>> date2list = reduced.sortByKey().mapToPair(new ReducedToResult());
		RDDprinter.printSampleRDD(date2list.sample(false, 1 / 100)); // (2015-05,[(vino,21180), (insalata,21180), (pane,21040), (latte,20963), (dolce,20955)])

		return date2list;
	}

	private static final class ReducedToResult
			implements PairFunction<Tuple2<String, String>, String, List<Tuple2<String, Integer>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, List<Tuple2<String, Integer>>> call(Tuple2<String, String> s) {
			List<Tuple2<String, Integer>> productCountList = new ArrayList<Tuple2<String, Integer>>();
			for (String item_count : s._2.split(", ")) {
				String item = item_count.split(" ")[0]; //prodotto
				int count = Integer.parseInt(item_count.split(" ")[1]); //contatore
				productCountList.add(new Tuple2<>(item, count));
			}
			Collections.sort(productCountList, new Comparator<Tuple2<String, Integer>>() {
				@Override
				public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
					return t2._2.compareTo(t1._2);
				}
			});

			//tronco se è maggiore di 5
			if (productCountList.size() > 5) {
				List<Tuple2<String, Integer>> subList = productCountList.subList(0, 5); //da 0 a 4
				return new Tuple2<>(s._1, subList); //ritornato (data, lista(prod,cont))
			}
			return new Tuple2<>(s._1, productCountList);
		}
	}

	private static final class AggregateToReduced implements Function2<String, String, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String call(String s1, String s2) {
			return s1 + ", " + s2;
		}
	}

	private static final class CountsToAggregate implements PairFunction<Tuple2<String, Integer>, String, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, String> call(Tuple2<String, Integer> s) {
			String date = s._1.split(",")[0]; //splitto sulla virgola per ottenere la data
			String item_count = s._1.split(",")[1] + " " + s._2; // creo la string "item contatore"
			return new Tuple2<>(date, item_count);
		}
	}

	/* old implementation */
	@SuppressWarnings("unused")
	private static JavaPairRDD<String, List<Tuple2<String, Integer>>> calculateResult2(JavaSparkContext sparkContext,
			String inputFile) {
		JavaPairRDD<String, Integer> ones = sparkContext.textFile(inputFile)
				.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, Integer>> call(String receipt) {
						List<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>();
						String[] tokens = StringUtils.split(receipt, ",");
						for (int i = 1; i < tokens.length; i++) {
							results.add(new Tuple2<String, Integer>(tokens[0].substring(0, 7) + "," + tokens[i], 1));
						}
						return results;
					}
				});

		//"data prod", contatore --> data, lista <prod, i 
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		JavaPairRDD<String, String> aggregate = counts
				.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Tuple2<String, Integer> s) {
						String date = s._1.split(",")[0]; //splitto sulla virgola per ottenere la data
						String item_count = s._1.split(",")[1] + " " + s._2; // creo la string "item contatore"
						return new Tuple2<>(date, item_count);
					}
				});

		JavaPairRDD<String, String> reduced = aggregate.reduceByKey(new Function2<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String s1, String s2) {
				return s1 + ", " + s2;
			}
		});

		// lo applico ad una <Tuple2<String,String> del JavaPairRDD<String,String> e returno un JavaPairRDD<String,List<String,Integer>>>
		JavaPairRDD<String, List<Tuple2<String, Integer>>> date2list = reduced
				.mapToPair(new PairFunction<Tuple2<String, String>, String, List<Tuple2<String, Integer>>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, List<Tuple2<String, Integer>>> call(Tuple2<String, String> s) {
						List<Tuple2<String, Integer>> productCountList = new ArrayList<Tuple2<String, Integer>>();
						for (String item_count : s._2.split(", ")) {
							String item = item_count.split(" ")[0]; //prodotto
							int count = Integer.parseInt(item_count.split(" ")[1]); //contatore
							productCountList.add(new Tuple2<>(item, count));
						}
						Collections.sort(productCountList, new Comparator<Tuple2<String, Integer>>() {
							@Override
							public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
								return t2._2.compareTo(t1._2);
							}
						});

						//tronco se è maggiore di 5
						if (productCountList.size() > 5) {
							List<Tuple2<String, Integer>> subList = productCountList.subList(0, 5); //da 0 a 4
							return new Tuple2<>(s._1, subList); //ritornato (data, lista(prod,cont))
						}
						return new Tuple2<>(s._1, productCountList);
					}
				});

		return date2list;
	}

}
