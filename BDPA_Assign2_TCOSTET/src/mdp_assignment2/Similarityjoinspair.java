package mdp_assignment2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class Similarityjoinspair extends Configured implements Tool{

 public static void main(String[] args) throws Exception {

    int res = ToolRunner.run(new Configuration(), new Similarityjoinspair(), args);
     
    System.exit(res);
 }
 
 public static enum CUSTOM_COUNTER {   // Create counter to count number of operations
		Counter_operations,
	};

 @Override
 public int run(String[] args) throws Exception {
	 
    Job job = Job.getInstance(getConf());
    

    
    job.setJarByClass(Similarityjoinspair.class);
    job.setReducerClass(Reduce.class);
    job.setOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(10);
    
    getConf().setBoolean("mapreduce.map.output.compress",true);
    getConf().set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
    
    job.setInputFormatClass(TextInputFormat.class);   
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
    job.waitForCompletion(true);
   

    return 0;
 }
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	 	// pairs will be used to store all pairs and avoid duplicate and prepro will serve to store our preprocessing file. 
		HashSet<String> pairs = new HashSet<String>();
		HashMap<String, String> prepro = new HashMap<String, String>();

		@Override
		
		// in the setup, we store the key and line of the preprocessing file in the HashMap. 
		protected void setup(Context context) throws IOException, InterruptedException {	   	
		   	prepro = new HashMap<String, String>(); 
		   	String path = "/home/cloudera/workspace/BDPA_Assign2_TCOSTET/preprocessing.txt";   // Change path here if preprocessed file has been moved
		   	
					BufferedReader Reader = new BufferedReader(new FileReader(new File(path)));
					String Stopline;
			    	while((Stopline = Reader.readLine()) !=null) {
			    		String[] array = Stopline.split(",");
			    		prepro.put(array[0].toLowerCase(),array[1]);
			    	}
			    	Reader.close();
		   }  
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// We take the key and the line from the value 
			String[] array = value.toString().split(",");
			String key1 = new String(array[0]);
			String value1 = new String(array[1]);
			
			// for each key, we loop over all key of the preprocessing file to create all possible pairs of key. 
			for (String key2 : prepro.keySet()) {
				if (!key1.equals(key2)) {
					// each pair is added in the two possible order in the pairs HashSet
					String pairs1 = new String(key1+key2);
					String pairs2 = new String(key2+key1);
				     
					// we check is the pair is not in the pairs HashSet to avoid duplicated pairs. 
					if (!pairs.contains(pairs1.toString())) {
						pairs.add(pairs1);
						pairs.add(pairs2);

						context.write(new Text(key1+","+key2), new Text(value1));
					}
				}
				}
			}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
        
		HashMap<String, String> prepro = new HashMap<String, String>();
		
		protected void setup(Context context) throws IOException, InterruptedException {  
		   	prepro = new HashMap<String, String>(); 
		   	String path = "/home/cloudera/workspace/BDPA_Assign2_TCOSTET/preprocessing.txt";   // Change path here if preprocessed file has been moved

					BufferedReader Reader = new BufferedReader(new FileReader(new File(path)));
					String Stopline;
			    	while((Stopline = Reader.readLine()) !=null) {
			    		String[] array = Stopline.split(",");
			    		prepro.put(array[0].toLowerCase(),array[1]);
			    	}
			    	Reader.close();
		   }  

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String[] array = key.toString().split(",");
			String key1 = new String(array[0]);
			String key2 = new String(array[1]);

			// We will use the three HashSet to compute the Jaccard Similarity. 
			//Words 1 will keep all words of line 1, same for words2. Union will keep all common words. 
			HashSet<String> union = new HashSet<String>();
			HashSet<String> words1 = new HashSet<String>();
			HashSet<String> words2 = new HashSet<String>();

			String line2 = prepro.get(key2);
			
			for (String word : line2.split(" ")) {
				words2.add(word);
				union.add(word);
			}

			for (String word : values.iterator().next().toString().split(" ")) {
				words1.add(word);
				union.add(word);
			}
			int inter = 0;

	        for (String word : union) {
	            if (words1.contains(word) && words2.contains(word)) {
	                inter++;
	            }
	        }
		
			double sim = 1.0 * inter / union.size();

			if (sim >= 0.8) {
				context.write(new Text("(" + key1 + ", " + key2 + ")"),new Text(String.valueOf(sim))); // write output in the requested format
			}
			context.getCounter(CUSTOM_COUNTER.Counter_operations).increment(1); // increment the operation counter
		}
	}
}