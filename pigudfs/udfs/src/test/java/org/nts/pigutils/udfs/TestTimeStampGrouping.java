package org.nts.pigutils.udfs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/**
 * 
 * Test that the TimeStampGrouping groups data correctly based on timestamp
 * windows.
 * 
 */
public class TestTimeStampGrouping {

	@Test
	public void testGrouping() throws IOException {

		Tuple[] data = getData();

		TimeStampGrouping udf = new TimeStampGrouping();
		Set<String> iSet = new HashSet<String>();

		for (Tuple tuple : data) {
			iSet.add(udf.exec(tuple));
		}

		assertEquals(4, iSet.size());

	}

	@SuppressWarnings("unchecked")
	private static final Tuple[] getData() {

		// create two groups key = A, key = B,
		// A contains all timestamps within 10 minutes of each other
		// B contains three groups each group contains timestamps withing 10
		// minutes of each other, each group is separated by 1 hour.
		// window size is 15 minutes.

		Calendar cal = Calendar.getInstance();

		int aSize = 10;
		int bSize = 10;

		int bGroups = 3;

		int totalSize = aSize + bSize * bGroups;

		Tuple[] data = new Tuple[totalSize];
		TupleFactory fact = TupleFactory.getInstance();

		for (int i = 0; i < aSize; i++) {
			Tuple t = fact.newTuple(Arrays.asList("A", cal.getTimeInMillis(),
					900000L));
			data[i] = t;
			cal.add(Calendar.MINUTE, 10);

		}

		int groupIndex = 0;
		for (int i = aSize; i < totalSize; i++) {

			Tuple t = fact.newTuple(Arrays.asList("B", cal.getTimeInMillis(),
					900000L));
			data[i] = t;
			cal.add(Calendar.MINUTE, 10);

			if (groupIndex++ > bSize) {
				groupIndex = 0;
				cal.add(Calendar.HOUR, 1);
			}

		}

		return data;

	}

	static class DataHolder {
		String key;
		long ts;
	}

}
