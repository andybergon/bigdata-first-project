package topproducts;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopProductsMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text date = new Text();
	private Text product = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		// StringTokenizer tokenizer = new StringTokenizer(line);
		String[] rowElements = line.split(",");

		String dateFormatted = rowElements[0].substring(0, 7);
		if (dateFormatted.lastIndexOf("-") == 6) {
			dateFormatted = dateFormatted.substring(0, 6);
		}
		
		date.set(dateFormatted); // cut day of date

		for (int i = 1; i < rowElements.length; i++) {
			product.set(rowElements[i]); // vedere se va creato un nuovo Text
			context.write(date, product);
		}
	}
}
