package MR.HW2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//Data to the reducer comes as (kev, val), (key,val),...
//grouping comparator groups multiple key value pairs into single 
//reduce call based on key. Here we aare overriding that comparison of 
// key to only compare statioId instead of comparing both stationId and year
public class GroupingComparator extends WritableComparator {
	public GroupingComparator() {
		super(StationYearPair.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		//compares based on stationId
		StationYearPair key1 = (StationYearPair) w1;
		StationYearPair key2 = (StationYearPair) w2;
		return key1.getStationId().compareTo(key2.getStationId());
	}
}
