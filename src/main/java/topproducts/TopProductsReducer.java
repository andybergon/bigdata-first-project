package topproducts;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopProductsReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		StringBuilder valueList = new StringBuilder();
		boolean firstValue = true;

		for (Text val : values) {

			if (!firstValue) {
				valueList.append(", ");
			}

			valueList.append(val.toString());
			firstValue = false;
		}

		context.write(key, new Text(valueList.toString()));

	}

//	public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
//		
//		String[] productList = values.toString().split(",");
//		
//		String valuesString = "";
//		
//		List asList = Arrays.asList(productList);
//		Set<String> mySet = new HashSet<String>(asList);
//		for(String s: mySet){
//			valuesString = valuesString + s + " " + Collections.frequency(asList,s);
//		}
//
//		values.set(valuesString);
//		context.write(key, values);
//	}
}