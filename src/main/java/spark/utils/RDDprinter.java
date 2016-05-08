package spark.utils;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;

public class RDDprinter {
	public static void printSampleRDD(JavaPairRDD rdd) {
		System.out.println(sampleRDDtoString(rdd));
		System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
		System.out.println("############################################################");
	}

	public static String sampleRDDtoString(JavaPairRDD rdd) {
		int itemsToPrint = 2;
		List sampleList = rdd.take(itemsToPrint);

		return Arrays.toString(sampleList.toArray());
	}

	public static String sampleRDDwithListsToString(JavaPairRDD rdd) {
		int itemsToPrint = 5;
		List sampleList = rdd.take(itemsToPrint);
		String output = "";

		System.out.println(sampleList.getClass());

		for (Object sample : sampleList) {
			if (sample instanceof List<?>) {
				//				List sampleListInner = (List) sample;
				//				output += Arrays.toString(sampleListInner.toArray()) + "\n";
			} else {
				output += sample.toString() + "\n";
			}
		}

		return output;
	}
}
