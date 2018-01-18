//Ashika Avula
//800972702
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Google_PageRank extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Google_PageRank.class);

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Google_PageRank(), args);
		System.exit(res);
	}

    public int run(String[] args) throws Exception {
                       
                Job job1 = Job.getInstance(getConf(), "Count N value");  // job1 for counting the N value
		job1.setJarByClass(this.getClass());
        
                job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPaths(job1, args[0]);             //setting the input and output paths
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.setMapperClass(CountMap.class);
		job1.setReducerClass(CountReduce.class);

		job1.setMapOutputKeyClass(Text.class);
	        job1.setMapOutputValueClass(IntWritable.class);
	        job1.setOutputKeyClass(Text.class);
	        job1.setOutputValueClass(LongWritable.class);
                job1.waitForCompletion(true);
        
                long total_pages = job1.getCounters().findCounter("totalpages_in_wiki", "totalpages_in_wiki").getValue(); //counting the no of files using getCounters()
		Configuration conf_for_count = getConf();                                                                 //creating new cofiguration object
		conf_for_count.set("totalpages_in_wiki", String.valueOf(total_pages));                                    //assigning N value to configuration object
        
                Job job2 = Job.getInstance(getConf(), "Calculate initial pagerank"); //job2 is for calculating the link graph and initial pagerank
		job2.setJarByClass(this.getClass());	
				
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPaths(job2, args[0]);                    //setting new output path for link graph as iteration0 and this will be input to iteration1
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"iteration"+"0"));

		job2.setMapperClass(InitialRankMap.class);
		job2.setReducerClass(InitialRankReduce.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
                job2.waitForCompletion(true);
        
                int job = 1;                             //job3 calculate final pagerank till 10 iterations and take final value at 10th iteration
		int n = 10;
		for (job = 1; job <= n; job++) {
			Job job3 = Job.getInstance(getConf(), "Calculate final pagerank");
			job3.setJarByClass(this.getClass());
            
                        job3.setInputFormatClass(TextInputFormat.class);
		        job3.setOutputFormatClass(TextOutputFormat.class);
            
			FileInputFormat.addInputPaths(job3, args[1]+"iteration"+(job - 1));       //creating the new output paths for every iteration and setting them as inputs for next iteration. 
			FileOutputFormat.setOutputPath(job3, new Path(args[1]+"iteration"+job));
			
			job3.setMapperClass(FinalRankMap.class);
			job3.setReducerClass(FinalRankReduce.class);
            
            
		        job3.setMapOutputKeyClass(Text.class);
		        job3.setMapOutputValueClass(Text.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
            
			job3.waitForCompletion(true);
	        }
        
                Job job4 = Job.getInstance(getConf(), "sorting"); //job4 for sorting the ranks in descending order
		job4.setJarByClass(this.getClass());
        
		FileInputFormat.addInputPaths(job4, args[1]+"iteration"+(job - 1));         //iteration10 output is given as input
		FileOutputFormat.setOutputPath(job4, new Path(args[1]+"Output_Sorted"));    //sorted output is obtained in this path
        
		job4.setMapperClass(SortingMap.class);
		job4.setReducerClass(SortingReduce.class);
        
		job4.setMapOutputKeyClass(DoubleWritable.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);
        
		job4.waitForCompletion(true);
	

		return 1;
    }

    
    public static class CountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static final Pattern title_pat = Pattern.compile("<title>(.*?)</title>"); //considering title pattern matching to get count of files
		
		@Override 
        public void map(LongWritable offset, Text input, Context context)
				throws IOException, InterruptedException {
			
		String line = input.toString();  //converting input to string
            
                if(!line.isEmpty()) {
			Text pageName = new Text();
			Matcher title1 = title_pat.matcher(line); //if the pattern is matched then it writes into title1
			
			
	      if(title1.find() ){
                pageName = new Text(title1.group(1)); //group() function returns value present between brackets i.e., page name
				context.write(new Text(pageName), new IntWritable(1)); // writing each page name and assinging the value as 1
			  }     
		    }
		}	
	}	    
	
	
	public static class CountReduce extends Reducer<Text, IntWritable, Text, LongWritable > {	
		@Override public void reduce(Text pagename, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum=0;
		for (IntWritable count : values) {
				sum+=count.get();  //we are summing up all the values of N from different nodes in reduce phase and writing that in the context
			}
			
			context.getCounter("totalpages_in_wiki","totalpages_in_wiki").increment(sum); 
		}
	}
    
    public static class InitialRankMap extends Mapper<LongWritable, Text, Text, Text> {
    
      private static final Pattern title_pat = Pattern.compile("<title>(.*?)</title>");      //pattern matching for the title tag 
      private static final Pattern text_pat = Pattern.compile(".*<text.*?>(.*?)</text>.*");  //patern matching for text tag
      private static final Pattern link_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");          //pattern matching for outlinks
    
      	@Override 
        public void map(LongWritable offset, Text input, Context context)
				throws IOException, InterruptedException {

			String line = input.toString();     //reading the input and converting to string
            
            Matcher title_matcher = title_pat.matcher(line);  //for each line if the input matches title then it is stored in title_matcher
            Matcher text_matcher = text_pat.matcher(line);    //for the same line if the input matches 
            
	if(title_matcher.find())              //if the title is present; then it enters the loop  
	{
             
              while(text_matcher.find())       
             {
                    String text = text_matcher.group(1);   //the whole data between text tags is copied
		            Matcher link_matcher = link_pat.matcher(text); //searching for outlinks
		            while(link_matcher.find())
                    {
                      Text url = new Text(title_matcher.group(1));  //storing the corresonding title
                      Text links = new Text(link_matcher.group(1)); //storing the outlink
                      context.write(url,links);                    //writing title as key and outlink as value.
                    }
                    
              }  
             
            }
            
        }
    }
    
    public static class InitialRankReduce extends Reducer<Text, Text, Text, Text > {	
		@Override 
        public void reduce(Text doc_title, Iterable<Text> list, Context context)
				throws IOException, InterruptedException {
                    
                Configuration conf_for_count = context.getConfiguration();   //retriving the configuration
	        double total_doc = Double.valueOf(conf_for_count.get("totalpages_in_wiki"));  //getting the N value
                String outlink_list ="";
                StringBuilder s = new StringBuilder();  //using stringbuilder to store values 
                 
			     for (Text outlink : list) {
				  outlink_list+=outlink+";"; //storing all outlinks by seperating with ; for corresponding title
			    }  
                 
			     double initial_rank = 1 / total_doc; //initial rank is taken as 1/N               
                             
                 s.append(outlink_list);   
	         s.append("#####");
	         s.append(initial_rank); // appending all outlinks and initial rank seperated by #####
                 context.write(doc_title,new Text(s.toString())); //writing title as key and "outlinks list and initial rank" as value
        }
    }
    
    	public static class FinalRankMap extends Mapper<LongWritable, Text, Text, Text> {
		    @Override 
            public void map(LongWritable offset, Text input, Context context)
				throws IOException, InterruptedException {
                    
                
	        String line = input.toString();            //reading input and converting to string  
                String doc_title[] = line.split("\t");     //splitting near title 


                String[] rank_split = doc_title[1].split(";#####");                  //splitting outlinks and rank near ;#####
                context.write(new Text(doc_title[0]), new Text("$$"+rank_split[0]));  //writing title as key and outlinks as value              
                StringBuffer sb1 = new StringBuffer();    
                if(!rank_split[0].contains(";"))                        //this logic is for one or zero outlinks
	        {
                  String singlelink = rank_split[0];  
	           if(!singlelink.isEmpty()){
	             double rank = Double.parseDouble(rank_split[1]);
                     sb1.append(rank);
                     String rankAsString = sb1.toString();
		     context.write(new Text(singlelink), new Text(rankAsString));
                   } 
                }
                else     //if more than one outlink then it goes into this loop
                 {
				String[] outlinks_list = rank_split[0].split(";");   
				double outlinks = outlinks_list.length;              //calculating no of outlinks
								
				double rank = Double.parseDouble(rank_split[1])/outlinks;  //calculating new rank based on the no of outlinks
				sb1.append(rank);
                                String rankAsString = sb1.toString();  //converting double to string
				for(int i = 0; i < outlinks ; i++)
				 {									
					context.write(new Text(outlinks_list[i]), new Text(rankAsString)); //writing each outlink and corresponding rank 
				 }
				}
			   
            }
        }
    
        public static class FinalRankReduce extends Reducer<Text, Text, Text, Text > {	
		    @Override 
            public void reduce(Text doc_title, Iterable<Text> counts, Context context)
				throws IOException, InterruptedException {

			
			StringBuffer sb = new StringBuffer();
			double finalpagerank = 0.0;
			double df = 0.85;            //setting initial damping factor
			
			for( Text count: counts)
			{
                              String list = count.toString();   //converting text to string 
				if(list.startsWith("$"))         //if it has list of outlinks then it goes into this loop
				{	
                                     int str = list.indexOf("$$");
                                     sb.append(list.substring(str+2));
				
				}
				
				else                                 //else all the ranks are added
				{
                                      finalpagerank+=Double.parseDouble(list);

		                }

			}

			finalpagerank=(1-df)+df*(finalpagerank);  //final page rank is calculated

			String value = sb.toString()+";#####"+finalpagerank;  //final rank is appended to outlinks list using delimiter ;#####
		
		        context.write(doc_title, new Text(value));					
            }
	}
    public static class SortingMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        @Override
		public void map(LongWritable offset, Text input, Context context)
				throws IOException, InterruptedException {
            
      		    String line = input.toString();         //reading input and converting to string
	            String doc_title[] = line.split("\t");   //splitting near tab space to get title

		    String[] array = doc_title[1].split(";#####");    //splitting near delimiter ";#####" to get outlinks and rank seperated         
		    double finalrank = Double.parseDouble(array[1]);  //storing rank in the finalrank and multiplying by -1 to get sorted ranks 
		    context.write(new DoubleWritable(finalrank * (-1)), new Text(doc_title[0]));
		}
	}
    
    public static class SortingReduce extends
			Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		public void reduce(DoubleWritable finalrank, Iterable<Text> input,
				Context context) throws IOException, InterruptedException {
			double Rank = 0;
			
			Rank = finalrank.get() * (-1);            //multiplying back by -1 to get original values
			for (Text doc_title : input) {
				context.write(new Text(doc_title), new DoubleWritable(Rank)); //writing title as key and final rank as value 
			}
	     }
	}
    }

