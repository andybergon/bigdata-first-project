package spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public final class LineToPair implements PairFlatMapFunction<String, String, Integer> {
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
}
