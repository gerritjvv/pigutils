package org.nts.pigutils.udfs;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * 
 * Input: key, timestamp, time window in milliseconds
 * <p/>
 * E.g. userid, userid.ts, 1800000<br/>
 * This method will print out different groups separating the input into groups
 * of timestamps withing the window timeperiod.<br/>
 * The data must be sorted by timestamp before being sent to this UDF.<br/>
 * 
 */
public class TimeStampGrouping extends EvalFunc<String> {

	String currentKey = "";
	TSDimension currentDimension;
	Integer groupI;
	String currentDimensionKey;

	@Override
	public String exec(Tuple tuple) throws IOException {

		if (tuple != null && tuple.size() == 4) {

			final String key = tuple.get(0).toString();
			final long ts1 = ((Number) tuple.get(1)).longValue();
			final long ts2 = ((Number) tuple.get(2)).longValue();
			final long timeWindow = ((Number) tuple.get(3)).longValue();

			if (!currentKey.equals(key)) {
				groupI = new Integer(1);
				currentKey = key;
				currentDimension = new TSDimension(ts1, ts2);
			} else {

				if (!isInWindow(currentDimension, ts1, ts2, timeWindow)) {
					// if not in window we increment to a new group and
					// tsdimension window
					groupI = new Integer(groupI.intValue() + 1);
					currentDimension = new TSDimension(ts1, ts2);

				} else {
					// else adjust the ts dimension window to include the new
					// timestamp value
					currentDimension.minTs = Math.min(ts1,
							currentDimension.minTs);
					currentDimension.maxTs = Math.max(ts2,
							currentDimension.maxTs);
				}

			}

			return key + groupI;

		} else {
			return null;
		}

	}

	/**
	 * Calculate to see if
	 * 
	 * @param prevts
	 * @param ts
	 * @param window
	 * @return
	 */
	private static final boolean isInWindow(TSDimension prevts, long ts1,
			long ts2, long window) {

		if (ts1 <= prevts.maxTs && ts1 >= prevts.minTs || ts2 <= prevts.maxTs
				&& ts2 >= prevts.minTs) {
			// check to see if ts1 or ts2 is inside the current time window
			return true;
		} else {
			// otherwise see if any of the points are within the min max window
			// range
			final long diff1 = positive(ts1 - prevts.minTs);
			final long diff2 = positive(ts1 - prevts.maxTs);
			final long diff3 = positive(ts2 - prevts.minTs);
			final long diff4 = positive(ts2 - prevts.maxTs);
			return diff1 <= window || diff2 <= window || diff3 <= window
					|| diff4 <= window;
		}
	}

	private static final long positive(long res) {
		return (res < 0) ? res * -1 : res;
	}

	static class TSDimension {

		long minTs;
		long maxTs;

		public TSDimension(long ts1, long ts2) {
			minTs = ts1;
			maxTs = ts2;
		}

	}

}
