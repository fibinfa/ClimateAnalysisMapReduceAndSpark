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
 * SecondarySort implementation with composite key as stationID and year and
 * temp value as corresponding values
 * 
 * @author fibinfa
 * @since 2018-02-04
 */
public class SecondarySort {

	public static class ClimateAnalysisMapper extends Mapper<Object, Text, StationYearPair, Text> {

		private Text temp = new Text();

		/*
		 * it reads line by line from the input file and emits (stationId, year) as the
		 * key and TMAX or TMIN temp as the value. We are appending TMAX or TMIN string
		 * to differentiate between TMAX and TMIN records
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			String stationId = tokens[0];
			String year = tokens[1].substring(0, 4);
			if (tokens[2].contains("TMAX") || tokens[2].contains("TMIN")) {
				if (tokens[2].contains("TMAX")) {
					temp.set("TMAX," + tokens[3]);
					context.write(new StationYearPair(stationId, year), temp);
				} else if (tokens[2].contains("TMIN")) {
					temp.set("TMIN," + tokens[3]);
					context.write(new StationYearPair(stationId, year), temp);
				}
			}
		}
	}

	public static class ClimateAnalysisReducer extends Reducer<StationYearPair, Text, Text, Text> {
		StringBuffer sb = new StringBuffer("[");
		String previousStation = null;

		/*
		 * Since we are using a grouping comparator, all records for a single station,
		 * irrespective of the year. While iterating we are checking whether current
		 * year and previous year are same to make sure that computations are correct.
		 * Finally output is written in the desired format
		 * 
		 */
		public void reduce(StationYearPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//in a single reduce call we receive all the records for 
			// a single stationId because we wrote a logic for doing that
			// in comparator
			String prevYear = key.getYear();
			String curYear = key.getYear();
			double sumMax = 0.0, sumMin = 0.0;
			int countMin = 0, countMax = 0;
			String tMinAvg = "", tMaxAvg = "";
			for (Text val : values) {
				curYear = key.getYear();
				// years are compared so that even if the values of combination
				// of year and station id changes , we are able to put it as
				// a separate entry
				if (prevYear.equals(curYear)) {
					if (val.toString().contains("TMAX")) {
						sumMax += Double.parseDouble(val.toString().split(",")[1]);
						countMax++;
					} else if (val.toString().contains("TMIN")) {
						sumMin += Double.parseDouble(val.toString().split(",")[1]);
						countMin++;
					}
				} else {
					tMaxAvg = " ";
					tMinAvg = " ";
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
					sb.append("(" + key.getYear() + ", " + tMinAvg + ", " + tMaxAvg + "),");
					prevYear = key.getYear();
					sumMax = 0.0;
					sumMin = 0.0;
					countMin = 0;
					countMax = 0;
				}
			}
			tMaxAvg = " ";
			tMinAvg = " ";
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

			// Since Group comparator function is implemented
			// previous station is null or same as last value indicates
			// that new record is of same station but with different year
			
			//output is printed in correct format using a string buffer
			if (previousStation == null || key.getStationId().equals(previousStation)) {
				sb.append("(" + key.getYear() + ", " + tMinAvg + ", " + tMaxAvg + "),");
				previousStation = key.getStationId();
			} else {
				context.write(new Text(previousStation),
						new Text(sb.deleteCharAt(sb.length() - 1).append("]").toString()));
				previousStation = key.getStationId();
				sb = new StringBuffer("[");
				sb.append("(" + key.getYear() + "," + tMinAvg + ", " + tMaxAvg + "),");
			}
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
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(ClimateAnalysisMapper.class);
		job.setMapOutputKeyClass(StationYearPair.class);
		job.setMapOutputValueClass(Text.class);
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
