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
 * Converts any object to a String by calling toString on it
 * 
 */
public class TO_STR extends EvalFunc<String> {

	@Override
	public String exec(Tuple tuple) throws IOException {

		Object val = null;
		
		return (tuple.size() > 0 && (val = tuple.get(0)) != null)
			? val.toString() : null;
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
