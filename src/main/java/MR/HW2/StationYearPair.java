package MR.HW2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Custom class defined for composite key to perform secondary sorting
 * 
 * @author fibinfa
 * @since 2018-02-04
 */
public class StationYearPair implements WritableComparable<StationYearPair> {

	private String stationId;
	private String year;

	public StationYearPair(String stationId, String year) {
		this.stationId = stationId;
		this.year = year;
	}
	
	public StationYearPair() {
		//this is for driver code to work
	}

	public String getStationId() {
		return stationId;
	}

	public String getYear() {
		return year;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, stationId);
		WritableUtils.writeString(out, year);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		stationId = WritableUtils.readString(in);
		year = WritableUtils.readString(in);
	}

	@Override
	public int compareTo(StationYearPair o) {
		if(o == null)
			return 0;
		int stationComp = stationId.compareTo(o.stationId);
		return stationComp==0?year.compareTo(o.year): stationComp;
	}

}
