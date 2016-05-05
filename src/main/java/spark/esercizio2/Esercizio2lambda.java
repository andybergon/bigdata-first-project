//package spark.esercizio2;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//
//import scala.Tuple2;
//import util.DurationFormatter;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.commons.lang.StringUtils;
//
//public class Esercizio2lambda implements Serializable {
//	private static final long serialVersionUID = 1L;
//
//	private String pathToFileScontrini;
//	private String pathToFilePrice;
//
//	public Esercizio2lambda(String fileScontrini, String filePrezzi) {
//		this.pathToFileScontrini = fileScontrini;
//		this.pathToFilePrice = filePrezzi;
//	}
//
//	public static void main(String[] args) throws IOException {
//		if (args.length < 3) {
//			System.err.println(
//					"Usage:  spark-submit ... jar <input_file_spesa.txt> <input_file_prices.txt> <output_folder>");
//			System.exit(1);
//		}
//
//		long startTime = System.currentTimeMillis();
//
//		SparkConf conf = new SparkConf().setAppName("Esercizio2");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//
//		Esercizio2lambda wc = new Esercizio2lambda(args[0], args[1]);
//		String outputFolderPath = args[2];
//
//		//        JavaPairRDD<String,Integer> resultPrice = wc.loadPrices(sc);
//		//        resultPrice.saveAsTextFile("/Users/Simone/Desktop/sparkPrices");
//		//        JavaPairRDD<String, List<Tuple2<String,Integer>>> result =wc.aggregate(sc);
//		//        File sparkoutput = new File("/Users/Simone/Desktop/sparkoutput");
//		//        deleteFile(sparkoutput);
//		//        result.saveAsTextFile("/Users/Simone/Desktop/sparkoutput"); 
//
//		JavaPairRDD<String, List<Tuple2<String, Integer>>> join = wc.computeJoin(sc);
//
//		File sparkoutput = new File(outputFolderPath);
//		deleteFile(sparkoutput);
//		FileUtils.deleteDirectory(sparkoutput);
//
//		long endTime = System.currentTimeMillis();
//		long elapsedTime = endTime - startTime;
//
//		String formattedElapsedTime = DurationFormatter.formatDuration(elapsedTime);
//		System.out.println("##########################################################");
//		System.out.println("Job COMPLETED in " + formattedElapsedTime);
//		System.out.println("##########################################################");
//
//		join.saveAsTextFile(outputFolderPath);
//		sc.close();
//	}
//
//	public static void deleteFile(File element) {
//		if (element.isDirectory()) {
//			for (File sub : element.listFiles()) {
//				deleteFile(sub);
//			}
//		}
//	}
//
//	public JavaPairRDD<String, Integer> loadData(JavaSparkContext sc) {
//		JavaPairRDD<String, Integer> ones = sc.textFile(pathToFileScontrini).flatMapToPair(rec -> {
//			List<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>();
//			String[] tokens = StringUtils.split(rec, ",");
//			for (int i = 1; i < tokens.length; i++) {
//				results.add(new Tuple2<String, Integer>(tokens[0].substring(0, 7) + "," + tokens[i], 1));
//			}
//			return results;
//		});
//		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
//		return counts;
//	}
//
//	public JavaPairRDD<String, List<Tuple2<String, Integer>>> aggregate(JavaSparkContext sc) {
//		JavaPairRDD<String, Integer> counts = loadData(sc); //"data prod", contatore --> data, lista <prod, i 
//		JavaPairRDD<String, String> aggregate = counts.mapToPair(s -> {
//			String date = s._1.split(",")[0]; //splitto sulla virgola per ottenere la data
//			String date_count = date + ":" + s._2; // creo la string "item contatore"
//			return new Tuple2<>(s._1.split(",")[1], date_count); // ("item", "data:cont")              
//		});
//
//		JavaPairRDD<String, String> reduced = aggregate.reduceByKey((s1, s2) -> s1 + " " + s2); //arrivo ad avere ("pane", "02-2015:852")
//		// lo applico ad una <Tuple2<String,String> del JavaPairRDD<String,String> e returno un JavaPairRDD<String,List<String,Integer>>>
//		JavaPairRDD<String, List<Tuple2<String, Integer>>> date2list = reduced.mapToPair(s -> {
//			List<Tuple2<String, Integer>> lista = new ArrayList<Tuple2<String, Integer>>();
//			for (String item_count : s._2.split(" ")) {
//				String date = item_count.split(":")[0]; //data
//				int count = Integer.parseInt(item_count.split(":")[1]); //contatore
//				lista.add(new Tuple2<>(date, count));
//			}
//			return new Tuple2<>(s._1, lista);
//		});
//		return date2list;
//	}
//
//	/* carica i prezzi dal file prezzi e li inserisce in un JavaPairRDD<String, Integer> */
//	public JavaPairRDD<String, Integer> loadPrices(JavaSparkContext sc) {
//
//		JavaPairRDD<String, Integer> mappa = sc.textFile(pathToFilePrice).mapToPair(s -> {
//			String item = s.split(",")[0];
//			int price = Integer.parseInt(s.split(",")[1]);
//			return new Tuple2<>(item, price);
//		});
//
//		return mappa;
//	}
//
//	/* esegue il join tra firstRDD (JavaPairRDD<String, List<Tuple2<String,Integer>>>) e secondRDD (JavaPairRDD<String, Integer>) */
//	public JavaPairRDD<String, List<Tuple2<String, Integer>>> computeJoin(JavaSparkContext sc) {
//
//		JavaPairRDD<String, List<Tuple2<String, Integer>>> firstRDD = aggregate(sc);
//		JavaPairRDD<String, Integer> secondRDD = loadPrices(sc);
//		JavaPairRDD<String, Tuple2<List<Tuple2<String, Integer>>, Integer>> join = firstRDD.join(secondRDD);
//
//		JavaPairRDD<String, List<Tuple2<String, Integer>>> result = join.mapToPair(s -> {
//			List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
//			int prezzo = s._2._2;
//			for (Tuple2<String, Integer> tupla : s._2._1) {
//				list.add(new Tuple2<>(tupla._1, tupla._2 * prezzo));
//			}
//			//riempire la lista con la moltiplicazione
//			return new Tuple2<>(s._1, list);
//		});
//
//		return result;
//
//	}
//}