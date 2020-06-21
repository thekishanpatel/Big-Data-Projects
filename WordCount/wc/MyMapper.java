package wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*word1.txt
apple ball apple
ball ball apple
*/

/*
LongWritable() Text()
0: apple ball apple
15: ball ball apple 
*/

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private final IntWritable one = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	
	System.out.println("Key ="+key.get());			  // First thing to do is output the 'Key'
	System.out.println("Value ="+value.toString());
	Text result = new Text();
	String words[] = value.toString().split(" "); // Hadoop Text class does not have a split method
	
	
	
	for (String word:words) {
		result.set(word);        // the 'Text()' object 'result' was created earlier--so it would not have to be created at each iteration
		context.write(result, one); // the constant object 'one' was created earlier--so it would not have to be created at each iteration
	}
	}

}



/* The Mapped Output
Text() IntWritable()
apple 1
ball  1
apple 1

ball  1
ball  1
apple 1
*/

/* The Shuffle Output
apple 1
ball  1
apple 1
ball  1
ball  1
apple 1
*/

/* The Sorted Output
apple 1
apple 1
apple 1
ball  1
ball  1
ball  1
*/

/* The Sorted Output
apple [1,1,1]
ball  [1,1,1]
*/