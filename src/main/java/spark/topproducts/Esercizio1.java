package spark.topproducts;

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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function2;
import org.apache.commons.io.FileUtils;

public class Esercizio1 implements Serializable{
    private static String pathToFile;
    private static final long serialVersionUID = 1L;
    public Esercizio1(String file){
        this.pathToFile = file;
    }
    public static void main(String[] args) throws IOException{
        if (args.length < 1) {
            System.exit(1);
        }
        Esercizio1 wc = new Esercizio1(args[0]);        
        JavaPairRDD<String, List<Tuple2<String,Integer>>> result =wc.aggregate();
        
        File sparkoutput = new File("/home/luca/Desktop/sparkoutput");
        deleteFile(sparkoutput);
        result.saveAsTextFile("/home/luca/Desktop/sparkoutput"); //saveAsTextFile crea una NUOVA directory!!!
    }
    
    public static void deleteFile(File element) {
    	   if (element.isDirectory()) {
    	       for (File sub : element.listFiles()) {
    	           deleteFile(sub);
    	       }
    	   }
    	   element.delete();
    	}
    /**
     *  Load the data from the text file and return an RDD of words
     */
    public JavaPairRDD<String, Integer> loadData() {
        SparkConf conf = new SparkConf()
        .setAppName("Esercizio1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String,Integer> ones = sc.textFile(pathToFile).flatMapToPair(new PairFlatMapFunction <String,String,Integer>()  {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Tuple2<String,Integer>> call(String rec) { 
                //        
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
    public JavaPairRDD<String, List<Tuple2<String,Integer>>> aggregate() {
        JavaPairRDD<String, Integer> counts = loadData(); //"data prod", contatore --> data, lista <prod, i 
        JavaPairRDD<String, String> aggregate = counts.mapToPair(new PairFunction<Tuple2<String,Integer>, String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, String> call(Tuple2<String,Integer> s) {
                String date = s._1.split(",")[0]; //splitto sulla virgola per ottenere la data
                String item_count = s._1.split(",")[1]+" "+s._2; // creo la string "item contatore"
                return new Tuple2<>(date, item_count);               
            } });
        JavaPairRDD<String, String> reduced = aggregate.reduceByKey(new Function2<String, String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(String s1, String s2) {
                return s1+", "+s2;
            }
        }); 
        // lo applico ad una <Tuple2<String,String> del JavaPairRDD<String,String> e returno un JavaPairRDD<String,List<String,Integer>>>
        JavaPairRDD<String, List<Tuple2<String,Integer>>> date2list = reduced.mapToPair(new PairFunction<Tuple2<String,String>, String, List<Tuple2<String,Integer>>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, List<Tuple2<String,Integer>>> call(Tuple2<String,String> s) {
                List<Tuple2<String,Integer>> lista = new ArrayList<Tuple2<String,Integer>>();
                for(String item_count : s._2.split(", ")){
                    String item = item_count.split(" ")[0]; //prodotto
                    int count = Integer.parseInt(item_count.split(" ")[1]); //contatore
                    lista.add(new Tuple2<>(item,count));
                }
                lista.sort(new Comparator<Tuple2<String,Integer>>(){
                    @Override
                    public int compare(Tuple2<String,Integer> t1, Tuple2<String,Integer> t2){
                        return t2._2.compareTo(t1._2);
                    }
                });
                //tronco se è maggiore di 5
                if(lista.size()>5){
                List<Tuple2<String,Integer>> subList = lista.subList(0, 5); //da 0 a 4
                return new Tuple2<>(s._1,subList); //ritornato (data, lista(prod,cont))
                }
                return new Tuple2<>(s._1,lista);
            } });
        return date2list;            
    }
}