package MR.HW2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * ClimateAnalysisV3 class -InMapperCombiner version of the climate analysis hadoop
 * program with in-mapper combining
 * 
 * @author fibinfa
 * @since 2018-02-04
 */
public class ClimateAnalysisV3 {

	public static class ClimateAnalysisMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text temp = new Text();
		// hashmap for grouping the TMAX and TMIN values with accum sum and count
		// reduces the data traffic from Mapper to Reducer
		private Map<String,List<Integer>> minMap = new HashMap<>();
		private Map<String,List<Integer>> maxMap = new HashMap<>();
		/*
		 * it reads line by line from the input file and updates the hashmap with
		 * accum sum and count and finally emits entries of hashmaps
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			String stationId = tokens[0];
			if(tokens[2].contains("TMAX")) {
				//for tmax
				if(maxMap.containsKey(stationId)) {
					List<Integer> temp = maxMap.get(stationId);
					int newSum = temp.get(0)+ Integer.parseInt(tokens[3]);
					int newCount = temp.get(1)+1;
					temp.clear();
					temp.add(newSum);
					temp.add(newCount);
					maxMap.put(stationId, temp);
				} else {
					List<Integer> temp = new ArrayList<Integer>();
					temp.add(Integer.parseInt(tokens[3]));
					temp.add(1);
					maxMap.put(stationId, temp);
				}
			} else if(tokens[2].contains("TMIN")) {
				//for tmin
				if(minMap.containsKey(stationId)) {
					List<Integer> temp = minMap.get(stationId);
					int newSum = temp.get(0)+ Integer.parseInt(tokens[3]);
					int newCount = temp.get(1)+1;
					temp.clear();
					temp.add(newSum);
					temp.add(newCount);
					minMap.put(stationId, temp);
				} else {
					List<Integer> temp = new ArrayList<Integer>();
					temp.add(Integer.parseInt(tokens[3]));
					temp.add(1);
					minMap.put(stationId, temp);
				}
			}
		}
		/*
		 * Emits all emtries of hasmap line by line in the desired format
		 * This method is called only after all map operations are performed
		 */
		public void cleanup(Context context) throws IOException, InterruptedException {
			//emit maxmap
			for(String s: maxMap.keySet()) {
				word.set(s);
				List<Integer> tempList = maxMap.get(s);
				String finalRes = "TMAX,"+tempList.get(0)+","+tempList.get(1);
				temp.set(finalRes);
				context.write(word, temp);
			}
			//emit minmap
			for(String s: minMap.keySet()) {
				word.set(s);
				List<Integer> tempList = minMap.get(s);
				String finalRes = "TMIN,"+tempList.get(0)+","+tempList.get(1);
				temp.set(finalRes);
				context.write(word, temp);
			}
		}
	}

	public static class ClimateAnalysisReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		
		/*
		 * Receives list of values corresponding to a station, intermediate sum and
		 * count calculation is performed here and sent to the reducer for final
		 * processing
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double sumMax = 0.0, sumMin = 0.0;
			int countMin = 0, countMax = 0;
			for (Text val : values) {
				//sum and count is calculated based on record
				if (val.toString().contains("TMAX")) {
					sumMax += Double.parseDouble(val.toString().split(",")[1]);
					countMax+= Integer.parseInt(val.toString().split(",")[2]);
				} else if(val.toString().contains("TMIN")){
					sumMin += Double.parseDouble(val.toString().split(",")[1]);
					countMin+=Integer.parseInt(val.toString().split(",")[2]);
				}
			}
			//average calculation
			String tMaxAvg = " ", tMinAvg=" ";
			if (countMax > 0) {
				tMaxAvg = Double.toString(sumMax / countMax);
			} else {
				tMaxAvg = "-";
			}
			if (countMin > 0) {
				tMinAvg = Double.toString(sumMin / countMin);
			}  else {
				tMinAvg = "-";
			}
			//final output is in the form of stationId, tmin and tmax
			String finalRes = tMinAvg+", "+tMaxAvg;
			result.set(finalRes);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: climateanalysis <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "climate analysis");
		job.setJarByClass(ClimateAnalysisV3.class);
		job.setMapperClass(ClimateAnalysisMapper.class);
		job.setReducerClass(ClimateAnalysisReducer.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
