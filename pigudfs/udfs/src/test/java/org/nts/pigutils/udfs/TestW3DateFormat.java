package org.nts.pigutils.udfs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/**
 * 
 * Assert that a timestamp can be formatted to a format similar as with
 * SimpleDateFormat.
 * 
 */
public class TestW3DateFormat {

	@Test
	public void testDateFormat() throws IOException {

		Date d = new Date();

		W3DateFormat udf = new W3DateFormat();
		String df = udf.exec(TupleFactory.getInstance().newTuple(
				Arrays.asList(d.getTime())));

		System.out.println("DF: " + df);
		assertEquals(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(d), df);

	}

}
