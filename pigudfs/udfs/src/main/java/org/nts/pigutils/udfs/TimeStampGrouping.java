package org.nts.pigutils.udfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
public class TimeStampGrouping extends EvalFunc<Integer> {

	long prevTs = 0;
	Map<String, Integer> groupMap = new HashMap<String, Integer>();

	@Override
	public Integer exec(Tuple tuple) throws IOException {

		if (tuple.size() != 3) {

			String key = tuple.get(0).toString();
			long ts = ((Number) tuple.get(1)).longValue();
			long timeWindow = ((Number) tuple.get(2)).longValue();

			Integer groupI = groupMap.get(key);
			if (groupI == null) {
				groupI = new Integer(1);
				groupMap.put(key, groupI);
			} else {
				if (!isInWindow(prevTs, ts, timeWindow)) {
					// if not in window we increment to a new group
					groupI = new Integer(groupI.intValue() + 1);
					groupMap.put(key, groupI);
				}
			}
			
			prevTs = ts;

			return groupI;

		} else {
			return null;
		}

	}

	private static final boolean isInWindow(long prevts, long ts, long window) {
		long v = ts - prevts;
		if (v < 0)
			v *= -1;

		return v < window;
	}

}
