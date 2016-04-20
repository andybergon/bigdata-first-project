package topproducts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

public class TopProductsChain extends Configured implements Tool {
	private static final IntWritable ONE = new IntWritable(1);
	/* Abbiamo righe in questo formato 2015-8-20,pesce,formaggio,insalata,pane
	con il primo mapper tagliamo il giorno dalla data e formiamo righe con data,prodotto come
	chiave e 1 come valore 
	
	2015-8-16,pesce,formaggio
	=>
	2015-8,pesce,1
	2015-8,formaggio,1
	
	quindi in output abbiamo Text, IntWritable*/
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text keyDateProduct = new Text();
		protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			String line = value.toString();
			String[] rowElements = line.split(",");
			String dateFormatted = rowElements[0].substring(0, 7);
			if (dateFormatted.lastIndexOf("-") == 6) {
				dateFormatted = dateFormatted.substring(0, 6);
			}
			for (int i = 1; i < rowElements.length; i++) {
				keyDateProduct.set(dateFormatted + ":" + rowElements[i]);
				ctx.write(keyDateProduct, ONE);
			}
		}
	}
	/* Il primo reducer ci fa le somme quindi 
	 * 2015-8:pesce 1  
	 * 2015-8:pesce 1
	 * =>
	 * 2015-8:pesce 2
	 */
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	/* Questo secondo mapper deve splittare la chiave data:prodotto in data: e il valore deve diventare prodotto quantità
	 * 2015-8:pesce 2, 
	 * 2015-8:formaggio 30
	 * => 
	 * 2015-8 pesce 2
	 * 2015-8 formaggio 30
	 */
	public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			Text dataKey = new Text();
			Text prodQuantityValue = new Text();
			String data, prodQuantity;
			
			String[] row = value.toString().split(":");
			data = row[0];
			prodQuantity = row[1];
			
			dataKey.set(data);
			prodQuantityValue.set(prodQuantity);
			
			System.out.println("key:" + dataKey.toString() + "|prodQty:" + prodQuantityValue.toString());
			
			ctx.write(dataKey, prodQuantityValue);
		}
	}
	
	/*
	 * 2015-8:pesce 2
	 * 2015-8:formaggio 30
	 * =>
	 * 2015-8:pesce 2, formaggio 30 
	 * 
	 * */
	
	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		private Map<String, Integer> countMap = new HashMap<String, Integer>();
		
		class ValueComparator implements Comparator<String> {
		    Map<String, Integer> base;

		    public ValueComparator(Map<String, Integer> base) {
		        this.base = base;
		    }

		    // Note: this comparator imposes orderings that are inconsistent with
		    // equals.
		    public int compare(String a, String b) {
		        if (base.get(a) >= base.get(b)) { //TODO: second sort on name
		            return -1;
		        } else {
		            return 1;
		        } // returning 0 would merge keys
		    }
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Text monthlyProducts = new Text();
			
			for (Text value : values) {
				String product = "";
				String quantity = "";
				String line = value.toString();
		        StringTokenizer tokenizer = new StringTokenizer(line);
		        
		        System.out.println("key:" + key);
		        System.out.println("line:" + line);
		        while (tokenizer.hasMoreTokens()) { //TODO
		        	product = tokenizer.nextToken();
		        	System.out.println("prod:" + product);
		        	quantity = tokenizer.nextToken();
		        	if(quantity.equals(","))//questa quantity la vede come "," !!!!!!!!!!!!!!!
		        		quantity = "1";//per fare una prova ho messo 1
		        	System.out.println("qty:" + quantity);
		        }
				
				//String[] rows = value.toString().split("\t"); //TODO: check
				Integer quantityInteger = new Integer(quantity);//questa quantity la vede come ","
				System.out.println("product:" + product);
				System.out.println("Integer:" + quantityInteger);
				countMap.put(product, quantityInteger);
			}
			System.out.println(countMap.values().toString());
			System.out.println("count keys size:" + countMap.keySet().size());
			System.out.println("count values size:" + countMap.values().size());
			
			ValueComparator bvc = new ValueComparator(countMap);
			TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(bvc);
			sortedMap.putAll(countMap);
			
			System.out.println("sorted keys size:" + sortedMap.keySet().size());
			System.out.println("sorted values size:" + sortedMap.values().size());
			
			String fiveProducts = "";
			int counter = 0;
			

			for (String keySorted : sortedMap.keySet()) {
				if (counter == 4) { //TODO: check if 6
					break;
				}
				fiveProducts = fiveProducts + keySorted + " ";// + sortedMap.get(keySorted).toString();
				if (counter != 4) {
					fiveProducts += ", ";
				}
				counter++;
				
			}
			System.out.println("key:" + key.toString());
			System.out.println("fiveProd:" + fiveProducts);
			monthlyProducts.set(fiveProducts);
			context.write(key, monthlyProducts);
			
			/*
						String mpString = "";
						for (Text value : values) {
							mpString = mpString + value.toString() + ", ";
						}
						
						monthlyProducts.set(mpString);
						context.write(key, monthlyProducts);
		*/}
	}

	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Path temp = new Path("tmp/tmp");//questo deve essere un file nella cartella tmp!!!!! NON una cartella!
		
		Configuration conf = getConf();
		boolean succ = false;
		
		/* JOB 1 */
		Job job1 = new Job(conf, "top-prod-pass-1");
		
		FileInputFormat.addInputPath(job1, input);
//		FileOutputFormat.setOutputPath(job1, output);
		FileOutputFormat.setOutputPath(job1, temp);
		
		job1.setJarByClass(TopProductsChain.class);
		
		job1.setMapperClass(Mapper1.class);
		job1.setCombinerClass(Reducer1.class);
		job1.setReducerClass(Reducer1.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		succ = job1.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job1 failed, exiting");
			return -1;
		}

		/* JOB 2 */		
		Job job2 = new Job(conf, "top-prod-pass-2");
		
		FileInputFormat.setInputPaths(job2, temp);
		FileOutputFormat.setOutputPath(job2, output);
		job2.setJarByClass(TopProductsChain.class);
		
		job2.setMapperClass(Mapper2.class);
		job2.setCombinerClass(Reducer2.class);
		job2.setReducerClass(Reducer2.class);
		
		//job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setNumReduceTasks(1); //?
		
		succ = job2.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job2 failed, exiting");
			return -1;
		}
		/*
		*/
		return 0;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TopProductsChain(), args);
		System.exit(res);
	}
}