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


public class Preprocessing extends Configured implements Tool{

	
 public static void main(String[] args) throws Exception {

    int res = ToolRunner.run(new Configuration(), new Preprocessing(), args); 
    System.exit(res);
 }
 
 public static enum CUSTOM_COUNTER {
		Counter_lines,
	};

 @Override
 public int run(String[] args) throws Exception {
	 
    Job job = Job.getInstance(getConf());
      
    job.setJarByClass(Preprocessing.class);
    job.setReducerClass(Reduce.class);
    job.setOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Combine.class);
    job.setNumReduceTasks(1);
    
    job.setInputFormatClass(TextInputFormat.class);   
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
    
    getConf().setBoolean("mapreduce.map.output.compress",true);
    getConf().set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
   
    job.waitForCompletion(true);
   

    return 0;
 }
 

 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private HashSet<String> stopwords;
    
    
    // Read the stopwords file and store stopword in the stopwords HaShset
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	 stopwords = new HashSet<String>();  	 
    	 String path = "/home/cloudera/workspace/BDPA_Assign2_TCOSTET/stopwords.csv";   // Change path here if stopword file has been moved
    	 
			BufferedReader Reader = new BufferedReader(new FileReader(new File(path)));
			String line = new String();
	    	while((line = Reader.readLine()) !=null) {
	    		String[] array = line.split(",");
	    		stopwords.add(array[0]);
	    	}
	    	Reader.close();
    }
    
    
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
 	
    // If the line is not empty, replace punctuation by space.
    if (!value.toString().isEmpty()) {
    	String line = value.toString().replaceAll("[\\p{Punct}]"," "); // As ask in the homework, we remove all punctuation (even ' or -) but we keep upper cases. 
    	for (String token : line.split("\\s+")) {
    		if (!stopwords.contains(token.toLowerCase()) && !token.isEmpty()) {  //remove stopwords
    			context.write(new Text("#"+token),new Text("1")); // write # words and 1
        		context.write(new Text(key.toString()),new Text(token));	// write key and id				
    		}
    	}
    	}
    }
 }  

 
 public static class Combine extends
 Reducer<Text, Text, Text, Text> {
	 	// in the combiner, we calculate the frequency per word
	   @Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
		    ArrayList<String> words= new ArrayList<String>();
			int count = 0;
			if (key.toString().contains("#")) { // only count frequency for words (beginning per #)
			for (Text val : values) {
				count++;
				words.add(val.toString());
			}
			context.write(key,new Text(Integer.toString(count))); // write word + new frequency calculated
			} else {
				for (Text val : values) {
			context.write(key,val); // if no word, just write the key and corresponding value
				}
			}
    }
 }
 
 public static class Reduce extends
 Reducer<Text, Text, Text, Text> {

	 HashMap<String, Integer> wordcount = new HashMap<String, Integer>();

	   @Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
		
		   ArrayList<Integer> freq =new ArrayList<Integer>();
		   ArrayList<String> words= new ArrayList<String>();
		   String keyS = key.toString();
		   
		  // if key contain #, then it is a word. We add the word + frequency in an HashMap
		   if (keyS.contains("#")) {
			   for (Text val : values) {
			   String word =  keyS.replaceAll("#","");
			   wordcount.put(word,Integer.valueOf(val.toString()));
			   }  
			  
		// if key does not contain #, then the key is a docID. For each word associated with the doc, we add it in the arraylist by increasing order. 
		   } else {
			   for (Text word : values) {
					  if(freq.isEmpty()) {
					        freq.add(wordcount.get(word.toString()));
					        words.add(word.toString()); 	         
				   } else {
					   if (!words.contains(word.toString())) {
						   int index = 0;
						   while(index < freq.size()) {
							   if (freq.get(index)>wordcount.get(word.toString())) {
								   break;
							   }
							   index = index + 1;
						   }					
						   freq.add(index, wordcount.get(word.toString()));
						   words.add(index, word.toString());
					   }
				   }
				   } 
			   
			   // Write the final output with words sorted by ascending order.
			   	StringBuilder stringBuilder = new StringBuilder();
			   	int index = 0;
			   	while(index < freq.size()) {
				   if(index==0){
					   //stringBuilder.append(words.get(index)+"#"+freq.get(index).toString());
					   stringBuilder.append(words.get(index));
				   } else {
					   //stringBuilder.append(" "+words.get(index)+"#"+freq.get(index).toString());	
					   stringBuilder.append(" "+words.get(index));	
				   	}
				   	index = index+1;
					}
			   	context.write(key,  new Text(stringBuilder.toString())); 
			   	// Increment the custom counter to count lines
			   	context.getCounter(CUSTOM_COUNTER.Counter_lines).increment(1);
				
		   }
	   	}
    }
 }