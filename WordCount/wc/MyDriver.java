package wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...]<out");
			System.exit(2);
		}
		
		Path iPath = new Path(otherArgs[otherArgs.length - 2]);
		Path oPath = new Path(otherArgs[otherArgs.length - 1]);
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJarByClass(MyDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, iPath);
		FileOutputFormat.setOutputPath(job, oPath);
		
		oPath.getFileSystem(conf).delete(oPath, true);
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	
		// contains all links to mapper, reducer, and driver program
		// input path, outpath
		// Export with .classpath and .project
		// hadoop jar C:\Users\kpvp2\Desktop\WordCount.jar wc.MyDriver /WordCount/Input/words.txt /WordCount/Output
	}
}
