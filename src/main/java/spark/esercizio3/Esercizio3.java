package spark.esercizio3;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
public class Esercizio3 implements Serializable{
    private static String pathToFileScontrini;
    private static final long serialVersionUID = 1L;
    private static int rowsNumber;
    public Esercizio3(String fileScontrini){
        this.pathToFileScontrini = fileScontrini;
    }
    public static void main(String[] args) throws IOException{
        if (args.length < 1) {
            System.exit(1);
        }
        long startDate = new Date().getTime();
        SparkConf conf = new SparkConf()
        .setAppName("Esercizio3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Esercizio3 wc = new Esercizio3(args[0]); 
        rowsNumber = (int)countRows(sc); //vedere come fare il parsing
        //System.out.println("countRows: "+rowsNumber);
        JavaPairRDD<String, String> result = wc.computeJoin(sc);
        long endDate = new Date().getTime();
        System.out.println("Job took "+(TimeUnit.MILLISECONDS.toMillis(endDate-startDate)) + " milliseconds");
		File sparkoutput = new File("/home/luca/Desktop/sparkoutput");
        deleteFile(sparkoutput);
        FileUtils.deleteDirectory(sparkoutput);
        result.saveAsTextFile("/home/luca/Desktop/sparkoutput");
        sc.close();
    }
    
	public static void deleteFile(File element) {
		if (element.isDirectory()) {
			for (File sub : element.listFiles()) {
				deleteFile(sub);
			}
		}
	}
    
    
    //UNICO DUBBIO VEDERE SE SU CLUSTER FUNZIONA COUNT ROWS
    public static long countRows(JavaSparkContext sc){
        JavaRDD<String> lines = sc.textFile(pathToFileScontrini);
        return lines.count();
    }
    // genera il primo rdd tuple2(tupl2(s,s),int)
    public JavaPairRDD<String, Tuple2<String,Integer>> generateFirstRDD(JavaSparkContext sc) {
        JavaPairRDD<Tuple2<String,String>,Integer> ones = sc.textFile(pathToFileScontrini).flatMapToPair(new PairFlatMapFunction <String,Tuple2<String,String>,Integer>()  {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Tuple2<Tuple2<String,String>,Integer>> call(String rec) {        
                List<Tuple2<Tuple2<String,String>,Integer>> results = new ArrayList<Tuple2<Tuple2<String,String>,Integer>>();
                String[] tokens = StringUtils.split(rec, ",");
                for (int i=1; i < tokens.length; i++) {
                    for(int j=1; j< tokens.length; j++){
                        if(i!=j){
                            results.add(new Tuple2<>(new Tuple2<>(tokens[i], tokens[j]), 1));
                        }
                    }                    
                }
                return results;
            }
        });   
        JavaPairRDD<Tuple2<String,String>, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        }); 
        JavaPairRDD<String, Tuple2<String,Integer>> result = counts.mapToPair(new PairFunction<Tuple2<Tuple2<String,String>,Integer>, 
                String, Tuple2<String,Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Tuple2<String,Integer>> call(Tuple2<Tuple2<String,String>,Integer> s) {
                return new Tuple2<>(s._1._1, new Tuple2<>(s._1._2, s._2));
            } });
        return result;
    }
    /* genero le coppie (pane,10)*/ 
    public JavaPairRDD<String, Integer> generateSecondRDD(JavaSparkContext sc) {
        JavaPairRDD<String,Integer> ones = sc.textFile(pathToFileScontrini).flatMapToPair(new PairFlatMapFunction <String,String,Integer>()  {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Tuple2<String,Integer>> call(String rec) {        
                List<Tuple2<String,Integer>> results = new ArrayList<Tuple2<String,Integer>>();
                String[] tokens = StringUtils.split(rec, ",");
                for (int i=1; i < tokens.length; i++) {
                    results.add(new Tuple2<String,Integer>(tokens[i], 1)); //genero solo (pane,1) senza data
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
    /* esegue il join tra firstRDD e secondRDD   Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>> */
    public JavaPairRDD<String, String> computeJoin(JavaSparkContext sc){
        JavaPairRDD<String, Tuple2<String,Integer>> firstRDD = generateFirstRDD(sc);
        JavaPairRDD<String, Integer> secondRDD = generateSecondRDD(sc);
        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> join = firstRDD.join(secondRDD); 
        // genera (pane, (latte,1), contPane) 
        
        JavaPairRDD<String, String> result = join.mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>>, 
                String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>> s) {
                double pairsCount = (double)s._2._1._2;
                double itemCount = (double)s._2._2;
                
                String support = Double.toString((pairsCount/rowsNumber)*100);
                String confidence = Double.toString((pairsCount/itemCount)*100);        
                
                return new Tuple2<>(s._1+","+s._2._1._1, support+","+confidence); 
                
            } });
        
        return result;
    }
}