/*
  Author : Nikhila Chireddy
  Date : 04-25-2017
*/

package AmazonReviews;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PIDReducer extends Reducer<Text,Text,Text,NullWritable>{
	
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		HashMap<String,String> localPid=new HashMap<String,String>();
		for(Text val : values){
			String s = val.toString();
			if(!localPid.containsKey(s)){
				localPid.put(s, "XXXXXXXXXXXXXX");
			}
		}
		for(String s: localPid.keySet()){
			context.write(new Text(s),NullWritable.get());
		}
						
			
       
       
     
	}    	
		
}