package MR.HW2;

import java.io.IOException;

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
 * ClimateAnalysisV2 class -Combiner version of the climate analysis hadoop
 * program with Combiner class
 * 
 * @author fibinfa
 * @since 2018-02-04
 */
public class ClimateAnalysisV2 {

	public static class ClimateAnalysisMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text temp = new Text();

		/*
		 * it reads line by line from the input file and emits stationId as the key and
		 * corresponding TMAX or TMIN val as the value. To differentiate between TMAX
		 * and TMIN "TMAX" is appended with TMAX value and TMIN is appended with TMIN
		 * value
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			String stationId = tokens[0];

			if (tokens[2].contains("TMAX") || tokens[2].contains("TMIN")) {
				word.set(stationId);
				if (tokens[2].contains("TMAX")) {
					temp.set("TMAX," + tokens[3]);
				} else if (tokens[2].contains("TMIN")) {
					temp.set("TMIN," + tokens[3]);
				}
				// key is stationId and value is temp value appended with type of record
				context.write(word, temp);
			}
		}
	}

	public static class ClimateAnalysisCombiner extends Reducer<Text, Text, Text, Text> {
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
				if (val.toString().contains("TMAX")) {
					sumMax += Double.parseDouble(val.toString().split(",")[1]);
					countMax++;
				} else if (val.toString().contains("TMIN")) {
					sumMin += Double.parseDouble(val.toString().split(",")[1]);
					countMin++;
				}
			}
			// stationId, maxSum, maxCount, minSum and minCount is passed to reducer
			String finalRes = sumMax + "," + countMax + "," + sumMin + "," + countMin;
			result.set(finalRes);
			context.write(key, result);
		}

	}

	public static class ClimateAnalysisReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		/*
		 * Output of combiner is received with sum and count of TMAX and TMIN for all
		 * stations Final computation of average is calculated here
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double sumMax = 0.0, sumMin = 0.0, countMax = 0.0, countMin = 0.0;
			for (Text val : values) {
				String[] tokens = val.toString().split(",");
				sumMax += Double.parseDouble(tokens[0]);
				countMax += Double.parseDouble(tokens[1]);
				sumMin += Double.parseDouble(tokens[2]);
				countMin += Double.parseDouble(tokens[3]);
			}
			//average calculation
			String tMaxAvg = " ", tMinAvg = " ";
			if (countMax > 0) {
				tMaxAvg = Double.toString(sumMax / countMax);
			} else {
				tMaxAvg = "-";
			}
			if (countMin > 0) {
				tMinAvg = Double.toString(sumMin / countMin);
			} else {
				tMinAvg = "-";
			}
			//final output in the format: stationId, tmin and tMax
			String finalRes = tMinAvg + ", " + tMaxAvg;
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
		job.setJarByClass(ClimateAnalysisV2.class);
		job.setMapperClass(ClimateAnalysisMapper.class);
		job.setCombinerClass(ClimateAnalysisCombiner.class);
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
