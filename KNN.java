package edu.ucr.cs.cs226.rzhan120;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNN {
	
		public static class DoubleString implements Writable {
			private Double distance = 0.0;
//			private Double X;
//			private Double Y;
			private Long ID;
			private String model = null;
			
			public DoubleString(){}
			
			public void set(Double lhs, String rhs)
			{
				distance = lhs;
				model = rhs;
			}
			
			public void setDistance(Double dist)
			{
				this.distance = dist;
			}

			public Double getDistance(){
				return distance;
			}
			
			public void setModel()
			{
				this.model = this.getModel();
			}

			public String getModel(){
				//return model = ID+"distance: "+distance;
				return ID+" ";
			}
			public void setID(Long id){
				this.ID = id;
			}
			
			public Long getID(){
				return ID;
			}
						
			public void readFields(DataInput in) throws IOException{
				distance = in.readDouble();
//				X = in.readDouble();
//				Y = in.readDouble();
				ID = in.readLong();
				model = in.readUTF();
			}
			
			public void write(DataOutput out) throws IOException{
				out.writeDouble(distance);
//				out.writeDouble(X);
//				out.writeDouble(Y);
				out.writeUTF(model);
				out.writeLong(ID);
			}			
			
//			public static DoubleString read(DataInput in) throws IOException {
//				DoubleString w = new DoubleString();
//		        w.readFields(in);
//		        return w;
//	       }
		}
				
		// Mapper class 
		public static class KnnMapper extends Mapper<Object, Text, NullWritable, DoubleString>
		{
			//private LongWritable outKey = new LongWritable();
		    private DoubleString outVal = new DoubleString();
			//Configuration conf = new Configuration();			
			TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();	
			int K;
			//distance function
			private static double distance(double a1,double b1,double a2,double b2){				
				return Math.sqrt(Math.pow((a1-b1),2)+Math.pow((a2-b2),2));
		    }
			
			@Override
			// The map() method is run by MapReduce once for each row supplied as the input data
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException{	
				Configuration conf = context.getConfiguration();
				K = Integer.parseInt(conf.get("K"));
				//if (((Writable)key).get() != 0) {
					String[] statusCode = value.toString().split(",");
					double x = Double.parseDouble(statusCode[1]);
					double y = Double.parseDouble(statusCode[2]);										
					outVal.setID(Long.parseLong(statusCode[0]));
					double distance = distance(x,Double.parseDouble(conf.get("X")), y,Double.parseDouble(conf.get("Y")));
					outVal.setDistance(distance);
					outVal.setModel();
					KnnMap.put(outVal.distance, outVal.model);
					if (KnnMap.size() > K)
					{
						KnnMap.remove(KnnMap.lastKey());
					}
//					context.write(outKey,outVal);					
				//}				
			}
			protected void cleanup(Context context) throws IOException, InterruptedException
			{
				// Loop through the K key:values in the TreeMap
				for(Map.Entry<Double, String> entry : KnnMap.entrySet())
				{
					  Double knnDist = entry.getKey();
					  String knnModel = entry.getValue();
					  outVal.set(knnDist, knnModel);					 
				}
				 context.write(NullWritable.get(), outVal);
			}
		}
		
		
		public static class KnnReducer extends Reducer<NullWritable,DoubleString,NullWritable,Text>
		{
			TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
			int K;
			
			// The reduce() method sort the distance and find the largest K value
			public void reduce(NullWritable key, Iterable<DoubleString> values,
                    Context context) throws IOException, InterruptedException
			{
				// values are the K DoubleString objects which the mapper wrote to context
				// Loop through these
				Configuration conf_2 = context.getConfiguration();
		        K = Integer.parseInt(conf_2.get("K"));
		        DoubleString rd = new DoubleString();
		        
				for (DoubleString val : values){
					rd.distance = val.getDistance();
					rd.model = val.getModel();
					
					// Populate another TreeMap with the distance and model information extracted from the
					// DoubleString objects and trim it to size K as before.
					KnnMap.put(rd.distance, rd.model);
					if (KnnMap.size() > K)
					{
						KnnMap.remove(KnnMap.lastKey());
					}
				}				
			// Finally write to context another NullWritable as key and the most common model just counted as value.
			context.write(NullWritable.get(),new Text(KnnMap.toString()));	// Use this line to see all K nearest neighbours and distances
		}
	}
			
		public static void main( String[] args) throws Exception{
				// Create configuration	
				Configuration conf = new Configuration();
				conf.set("X", args[1]);
				conf.set("Y", args[2]);
				conf.set("K", args[3]);
    			   		
    			// Create job
    			Job job = Job.getInstance(conf, "Find K-Nearest Neighbour");
    			job.setJarByClass(KNN.class);
    			
    			// Setup MapReduce job
    			job.setMapperClass(KnnMapper.class);
    			job.setReducerClass(KnnReducer.class);
    			job.setNumReduceTasks(1); // Only one reducer in this design

    			// Specify key / value
    			job.setMapOutputKeyClass(NullWritable.class);
    			job.setMapOutputValueClass(DoubleString.class);
    			job.setOutputKeyClass(NullWritable.class);
    			job.setOutputValueClass(Text.class);
    					
    			// Input (the data file) and Output (the resulting classification)
    			FileInputFormat.addInputPath(job, new Path(args[0]));
    			FileOutputFormat.setOutputPath(job, new Path(args[4]));
    			// Execute job and return status
    			System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
