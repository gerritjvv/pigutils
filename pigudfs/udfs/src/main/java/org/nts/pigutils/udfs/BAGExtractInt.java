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
public class BAGExtractInt extends EvalFunc<Integer> {

	@Override
	public Integer exec(Tuple input) throws IOException {

		if (!(input == null || input.size() < 3)) {

			Object obj = input.get(0);
			int index = (Integer) input.get(1);
			Integer def = (Integer) input.get(2);

			if (obj != null && obj instanceof DataBag) {
				DataBag bag = (DataBag) obj;
				for (Tuple tuple : bag) {

					Object val = tuple.get(index);
					if (val != null && val instanceof Integer) {
						return (Integer) val;
					}
				}
			}

			return def;
		}

		return new Integer(-1);
	}

}
