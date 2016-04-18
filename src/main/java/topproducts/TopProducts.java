package topproducts;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopProducts {

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "TopProducts");

		job.setJarByClass(TopProducts.class);
		
		job.setMapperClass(TopProductsMapper.class);
		// combiner use
		job.setCombinerClass(TopProductsReducer.class);
		job.setReducerClass(TopProductsReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}