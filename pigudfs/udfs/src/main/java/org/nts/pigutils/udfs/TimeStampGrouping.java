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

		if (tuple != null && tuple.size() == 3) {

			final String key = tuple.get(0).toString();
			final long ts = ((Number) tuple.get(1)).longValue();
			final long timeWindow = ((Number) tuple.get(2)).longValue();

			if (!currentKey.equals(key)) {
				groupI = new Integer(1);
				currentKey = key;
				currentDimension = new TSDimension(ts);
			} else {

				if (!isInWindow(currentDimension, ts, timeWindow)) {
					// if not in window we increment to a new group and
					// tsdimension window
					groupI = new Integer(groupI.intValue() + 1);
					currentDimension = new TSDimension(ts);

				} else {
					// else adjust the ts dimension window to include the new
					// timestamp value
					currentDimension.minTs = Math.min(ts,
							currentDimension.minTs);
					currentDimension.maxTs = Math.max(ts,
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
	private static final boolean isInWindow(TSDimension prevts, long ts,
			long window) {
		return positive(ts - prevts.minTs) < window
				|| positive(ts - prevts.maxTs) < window;
	}

	private static final long positive(long res) {
		return (res < 0) ? res * -1 : res;
	}

	static class TSDimension {

		long minTs;
		long maxTs;

		public TSDimension(long ts) {
			minTs = ts;
			maxTs = ts;
		}

	}

}
