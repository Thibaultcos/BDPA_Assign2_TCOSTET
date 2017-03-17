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





public class Similarityjoinsindex extends Configured implements Tool{

 public static void main(String[] args) throws Exception {

    int res = ToolRunner.run(new Configuration(), new Similarityjoinsindex(), args);
     
    System.exit(res);
 }
 
 public static enum CUSTOM_COUNTER {   // Create counter to count number of operations
		Counter_operations,
	};

 @Override
 public int run(String[] args) throws Exception {
	 
    Job job = Job.getInstance(getConf());
    

    
    job.setJarByClass(Similarityjoinsindex.class);
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

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] array = value.toString().split(",");
			String key1 = new String(array[0]);
			String line = new String(array[1]);
			String[] words = line.toString().split("\\s+");
			
			int numberwords = (int) Math.round(words.length - (words.length * 0.8) + 1); // calculate number of words to keep

			for(int i = 0; i <= numberwords-1; i++) {
				context.write(new Text(words[i]),new Text(key1));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		HashSet<String> pairsV = new HashSet<String>();  // Hashset the store all pairs to avoid duplicate
		HashMap<String, String> lines = new HashMap<String, String>(); // save the preprocessed file in the HashMap

		protected void setup(Context context) throws IOException, InterruptedException {
			
			String path = "/home/cloudera/workspace/BDPA_Assign2_TCOSTET/preprocessing.txt";   // Change path here if preprocessed file has been moved
			
		   	lines = new HashMap<String, String>(); 
		   	
					BufferedReader Reader = new BufferedReader(new FileReader(new File(path)));
					String Stopline;
			    	while((Stopline = Reader.readLine()) !=null) {
			    		String[] array = Stopline.split(",");
			    		lines.put(array[0],array[1]);
			    	}
			    	Reader.close();
		   } 

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			ArrayList<String> docids = new ArrayList<String>(); 
			// create an arraylist of docid to calculate how many docid have the same word and generate pairs
			for (Text docid : values) {   
				docids.add(docid.toString());
			}
 
			// generate list of docID pairs, with docID containing the same words
			if (docids.size() > 1) {
				ArrayList<String> pairs = new ArrayList<String>();
				for (int i = 0; i < docids.size(); ++i) {
					for (int j = i + 1; j < docids.size(); ++j) { // two loops to generate all pairs
						if (!new String(docids.get(i)).equals(docids.get(j))) {   // check if docID are not the same 
						String pair = new String(docids.get(i) + " "+ docids.get(j)); // generate pairs string
						String pair2 = new String(docids.get(j) + " "+ docids.get(i));
						if (!pairsV.contains(pair) && !pairsV.contains(pair2) ) { // check if we have already done the pair
							pairsV.add(pair); // if not, add the pair to our pairs hashset
							pairsV.add(pair2); // in both orders
						pairs.add(pair);   // and add the pair to the ArrayList of selected pairs to calculate similarity
						}	
					}
				}
				}	
				
				// For each pair selected, calculate similarity and increment counter
				for (String pair : pairs) {
					
					context.getCounter(CUSTOM_COUNTER.Counter_operations).increment(1); // increment the operation counter
					
					HashSet<String> union = new HashSet<String>();
					HashSet<String> words1 = new HashSet<String>();
					HashSet<String> words2 = new HashSet<String>();
					String doc1 = pair.split(" ")[0].toString().toString();
					String doc2 = pair.split(" ")[1].toString().toString();			
					String line1 = lines.get(doc1);
					String line2 = lines.get(doc2);

					
					for (String word : line1.split(" ")) {
						words1.add(word);
						union.add(word);
					}
					
					for (String word : line2.split(" ")) {
						words2.add(word);
						union.add(word);
					}

					int inter = 0;

			        for (String word : union) {
			            if (words1.contains(word) && words2.contains(word)) {
			                inter++;
			            }
			        }

					double similarity = 1.0 * inter / union.size();

					if (similarity >= 0.8) {
						context.write(new Text("(" + doc1 + ", " + doc2 + ")"),new Text(String.valueOf(similarity))); // Add pairs and similarity in the requested format
					}		
				}
 
			}
		}
	}
}