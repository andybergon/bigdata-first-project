//package spark.esercizio1;
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
//import org.apache.commons.lang.StringUtils;
//import org.apache.commons.io.FileUtils;
//
//public class Esercizio1lambda implements Serializable {
//	private static final long serialVersionUID = 1L;
//
//	private static String pathToFile;
//
//	public Esercizio1lambda(String file) {
//		this.pathToFile = file;
//	}
//
//	public static void main(String[] args) throws IOException {
//		if (args.length < 2) {
//			System.err.println("Usage:  spark-submit ... jar <input_file> <output_folder>");
//			System.exit(1);
//		}
//
//		long startTime = System.currentTimeMillis();
//
//		Esercizio1lambda wc = new Esercizio1lambda(args[0]);
//		String outputFolderPath = args[1];
//
//		JavaPairRDD<String, List<Tuple2<String, Integer>>> result = wc.aggregate();
//
//		long endTime = System.currentTimeMillis();
//		long elapsedTime = endTime - startTime;
//
//		String formattedElapsedTime = DurationFormatter.formatDuration(elapsedTime);
//		System.out.println("##########################################################");
//		System.out.println("Job COMPLETED in " + formattedElapsedTime);
//		System.out.println("##########################################################");
//
//		File sparkoutput = new File(outputFolderPath);
//		deleteFile(sparkoutput);
//		FileUtils.deleteDirectory(sparkoutput);
//
//		result.saveAsTextFile(outputFolderPath); //saveAsTextFile crea una NUOVA directory!!!
//	}
//
//	public static void deleteFile(File element) {
//		if (element.isDirectory()) {
//			for (File sub : element.listFiles()) {
//				deleteFile(sub);
//			}
//		}
//		element.delete();
//	}
//
//	public JavaPairRDD<String, List<Tuple2<String, Integer>>> aggregate() {
//		JavaPairRDD<String, Integer> counts = loadData(); //"data prod", contatore --> data, lista <prod, i 
//
//		JavaPairRDD<String, String> aggregate = counts.mapToPair(s -> {
//			String date = s._1.split(",")[0]; //splitto sulla virgola per ottenere la data
//			String item_count = s._1.split(",")[1] + " " + s._2; // creo la string "item contatore"
//			return new Tuple2<>(date, item_count);
//		});
//
//		JavaPairRDD<String, String> reduced = aggregate.reduceByKey((s1, s2) -> s1 + ", " + s2);
//
//		// lo applico ad una <Tuple2<String,String> del JavaPairRDD<String,String> e returno un JavaPairRDD<String,List<String,Integer>>>
//		JavaPairRDD<String, List<Tuple2<String, Integer>>> date2list = reduced.mapToPair(s -> {
//			List<Tuple2<String, Integer>> lista = new ArrayList<Tuple2<String, Integer>>();
//			for (String item_count : s._2.split(", ")) {
//				String item = item_count.split(" ")[0]; //prodotto
//				int count = Integer.parseInt(item_count.split(" ")[1]); //contatore
//				lista.add(new Tuple2<>(item, count));
//			}
//			lista.sort((t1, t2) -> t2._2.compareTo(t1._2));
//
//			//tronco se è maggiore di 5
//			if (lista.size() > 5) {
//				List<Tuple2<String, Integer>> subList = lista.subList(0, 5); //da 0 a 4
//				return new Tuple2<>(s._1, subList); //ritornato (data, lista(prod,cont))
//			}
//			return new Tuple2<>(s._1, lista);
//		});
//
//		return date2list;
//	}
//
//	public JavaPairRDD<String, Integer> loadData() {
//		SparkConf conf = new SparkConf().setAppName("Esercizio1");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//
//		JavaPairRDD<String, Integer> ones = sc.textFile(this.pathToFile).flatMapToPair(rec -> {
//
//			List<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>();
//			String[] tokens = StringUtils.split(rec, ",");
//
//			for (int i = 1; i < tokens.length; i++) {
//				results.add(new Tuple2<String, Integer>(tokens[0].substring(0, 7) + "," + tokens[i], 1));
//			}
//
//			return results;
//		});
//
//		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
//
//		return counts;
//	}
//
//}