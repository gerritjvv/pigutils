package org.nts.pigutils.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * 
 * Takes out any illegal chars that are not numbers from a supposed to be number
 * value.<br/>
 * If after all chars are checked no number char was found -1 is returned.
 * 
 */
public class PurifyInt extends EvalFunc<Integer> {

	@Override
	public Integer exec(Tuple tuple) throws IOException {

		int ret = -1;
		String val = null;
		if (tuple.size() > 0 && (val = (String) tuple.get(0)) != null) {
			StringBuilder buff = new StringBuilder();
			for (int i = 0; i < val.length(); i++) {
				char ch = val.charAt(i);

				if (ch >= '0' && ch <= '9') {
					buff.append(ch);
				}

			}

			if (buff.length() > 0)
				ret = Integer.parseInt(buff.toString());
		}

		return ret;
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();

		List<Schema.FieldSchema> arguments = Arrays
				.asList(new Schema.FieldSchema(null, DataType.CHARARRAY));

		funcList.add(new FuncSpec(this.getClass().getName(), new Schema(
				arguments)));
		return funcList;
	}

}
