package org.nts.pigutils.udfs;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * 
 * From a BAG of tuples this function will extract the first non null and empty
 * string if any.<br/>
 * Usage: BAGExtractString(bag, fieldIndex, defaultValue)
 * 
 */
public class BAGExtractLong extends EvalFunc<Long> {

	@Override
	public Long exec(Tuple input) throws IOException {

		if (!(input == null || input.size() < 3)) {

			Object obj = input.get(0);
			int index = (Integer) input.get(1);
			Number def = (Number) input.get(2);

			if (obj != null && obj instanceof DataBag) {
				DataBag bag = (DataBag) obj;
				for (Tuple tuple : bag) {

					Object val = tuple.get(index);
					if (val != null && val instanceof Number) {
						return ((Number) val).longValue();
					}
				}
			}

			return def.longValue();
		}

		return new Long(-1);
	}

}
