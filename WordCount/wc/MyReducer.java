package wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Input
/*
apple [1, 1, 1]
ball  [1, 1, 1]
*/

// Output from my Mapper is the Input to my Reducer
public class MyReducer extends Reducer<Text, IntWritable,Text, IntWritable>{
	@Override
	protected void reduce(Text word, Iterable<IntWritable> it, Context context) 
			throws IOException, InterruptedException {
		
		System.out.println("Word ="+word.toString());
		System.out.print("[");
		
		int count = 0;
		for (IntWritable i:it) {
			System.out.print(i.get() +", ");
			count = count + i.get(); // Because 'count' is INT and 'i' is IntWritable, you have to convert the latter into INT using the .get() method
		}
		
		System.out.print("]");
		
		context.write(word, new IntWritable(count));
		
	}
}


// Output
/*
Text () IntWritable()
apple 3
ball  3
*/