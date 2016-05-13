package spark.utils;

import java.util.Comparator;

import scala.Tuple2;

public class Tuple2Comparator implements Comparator<Tuple2<String, String>> {
	public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
		return o1._1().compareTo(o2._2());

		/* violates comparable contract from java7 - https://docs.oracle.com/javase/7/docs/api/java/lang/Comparable.html */
		//		int comparable = o1._1().compareTo(o2._2());
		//		
		//		if (comparable < 0) {
		//			return -1;
		//		} else if (comparable > 0) {
		//			return 1;
		//		} else {
		//			return 0;
		//		}
	}
}
