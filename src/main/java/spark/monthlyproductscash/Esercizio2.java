package spark.monthlyproductscash;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function2;

public class Esercizio2 implements Serializable{
	private static String pathToFileScontrini;
	private static String pathToFilePrice;
	private static final long serialVersionUID = 1L;
	public Esercizio2(String fileScontrini, String filePrezzi){
		this.pathToFileScontrini = fileScontrini;
		this.pathToFilePrice = filePrezzi;
	}
	public static void main(String[] args) throws IOException{
		if (args.length < 1) {
			System.exit(1);
		}
		
		long startDate = new Date().getTime();
		
		SparkConf conf = new SparkConf()
				.setAppName("Esercizio2");
		JavaSparkContext sc = new JavaSparkContext(conf);

		Esercizio2 wc = new Esercizio2(args[0], args[1]); 
		//        JavaPairRDD<String,Integer> resultPrice = wc.loadPrices(sc);
		//        resultPrice.saveAsTextFile("/Users/Simone/Desktop/sparkPrices");
		//        JavaPairRDD<String, List<Tuple2<String,Integer>>> result =wc.aggregate(sc);
		//        File sparkoutput = new File("/Users/Simone/Desktop/sparkoutput");
		//        deleteFile(sparkoutput);
		//        result.saveAsTextFile("/Users/Simone/Desktop/sparkoutput"); 

		JavaPairRDD<String, List<Tuple2<String,Integer>>> join = wc.computeJoin(sc);
		long endDate = new Date().getTime();
		System.out.println("Job took "+(TimeUnit.MILLISECONDS.toMillis(endDate-startDate)) + " milliseconds");
		File sparkoutput = new File("/home/luca/Desktop/sparkoutput");
        deleteFile(sparkoutput);
        FileUtils.deleteDirectory(sparkoutput);
        join.saveAsTextFile("/home/luca/Desktop/sparkoutput");
		sc.close();
	}

	public static void deleteFile(File element) {
		if (element.isDirectory()) {
			for (File sub : element.listFiles()) {
				deleteFile(sub);
			}
		}
	}

	public JavaPairRDD<String, Integer> loadData(JavaSparkContext sc) {
		//        SparkConf conf = new SparkConf()
		//        .setAppName("Esercizio2");
		//        JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<String,Integer> ones = sc.textFile(pathToFileScontrini).flatMapToPair(new PairFlatMapFunction <String,String,Integer>()  {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<Tuple2<String,Integer>> call(String rec) {        
				List<Tuple2<String,Integer>> results = new ArrayList<Tuple2<String,Integer>>();
				String[] tokens = StringUtils.split(rec, ",");
				for (int i=1; i < tokens.length; i++) {
					results.add(new Tuple2<String,Integer>(tokens[0].substring(0, 7)+","+tokens[i], 1));
				}
				return results;
			}
		});
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}); 
		return counts;
	}
	public JavaPairRDD<String, List<Tuple2<String,Integer>>> aggregate(JavaSparkContext sc) {
		JavaPairRDD<String, Integer> counts = loadData(sc); //"data prod", contatore --> data, lista <prod, i 
		JavaPairRDD<String, String> aggregate = counts.mapToPair(new PairFunction<Tuple2<String,Integer>, String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(Tuple2<String,Integer> s) {
				String date = s._1.split(",")[0]; //splitto sulla virgola per ottenere la data
				String date_count = date+":"+s._2; // creo la string "item contatore"
				return new Tuple2<>(s._1.split(",")[1], date_count); // ("item", "data:cont")              
			} });
		JavaPairRDD<String, String> reduced = aggregate.reduceByKey(new Function2<String, String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(String s1, String s2) {
				return s1+" "+s2;
			}
		}); //arrivo ad avere ("pane", "02-2015:852")
		// lo applico ad una <Tuple2<String,String> del JavaPairRDD<String,String> e returno un JavaPairRDD<String,List<String,Integer>>>
		JavaPairRDD<String, List<Tuple2<String,Integer>>> date2list = reduced.mapToPair(new PairFunction<Tuple2<String,String>, String, List<Tuple2<String,Integer>>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, List<Tuple2<String,Integer>>> call(Tuple2<String,String> s) {
				List<Tuple2<String,Integer>> lista = new ArrayList<Tuple2<String,Integer>>();
				for(String item_count : s._2.split(" ")){
					String date = item_count.split(":")[0]; //data
					int count = Integer.parseInt(item_count.split(":")[1]); //contatore
					lista.add(new Tuple2<>(date,count));
				}                
				return new Tuple2<>(s._1,lista); 
			} });
		return date2list;            
	}

	/* carica i prezzi dal file prezzi e li inserisce in un JavaPairRDD<String, Integer> */
	public JavaPairRDD<String, Integer> loadPrices(JavaSparkContext sc) {

		JavaPairRDD<String, Integer> mappa = sc.textFile(pathToFilePrice).mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String s) {
				String item = s.split(",")[0];
				int price = Integer.parseInt(s.split(",")[1]);
				return new Tuple2<>(item,price);             
			} });

		return mappa;
	}

	/* esegue il join tra firstRDD (JavaPairRDD<String, List<Tuple2<String,Integer>>>) e secondRDD (JavaPairRDD<String, Integer>) */
	public JavaPairRDD<String, List<Tuple2<String,Integer>>> computeJoin(JavaSparkContext sc){

		JavaPairRDD<String, List<Tuple2<String,Integer>>> firstRDD = aggregate(sc);
		JavaPairRDD<String, Integer> secondRDD = loadPrices(sc);
		JavaPairRDD<String, Tuple2<List<Tuple2<String,Integer>>, Integer>> join = firstRDD.join(secondRDD); 

		JavaPairRDD<String, List<Tuple2<String,Integer>>> result = join.mapToPair(new PairFunction<Tuple2<String, Tuple2<List<Tuple2<String,Integer>>, Integer>>, 
				String, List<Tuple2<String,Integer>>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, List<Tuple2<String,Integer>>> call(Tuple2<String, Tuple2<List<Tuple2<String,Integer>>, Integer>> s) {
				List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();
				int prezzo = s._2._2;
				for(Tuple2<String,Integer> tupla : s._2._1){
					list.add(new Tuple2<>(tupla._1, tupla._2*prezzo));
				}
				//riempire la lista con la moltiplicazione
				return new Tuple2<>(s._1, list);             
			} });

		return result;

	}
}