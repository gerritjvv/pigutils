package org.nts.pigutils.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Finds a partial key based on a regex and return the value
 * 
 */
public class MAP_FIND extends EvalFunc<Object> {

	final Pattern pattern;
	
	public MAP_FIND(String regex){
		pattern = Pattern.compile(regex);
	}
	
	@Override
	public Object exec(Tuple tuple) throws IOException {

		Object val = null;
		if(tuple.size() > 0 && (val = tuple.get(0)) != null){
			if(val instanceof Map){
				Map map = (Map)val;
				
				for(Object key : map.keySet()){
					if(pattern.matcher(key.toString()).find())
						return map.get(key);
				}
				
			}
		}
		
		return null;
		
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();

		List<Schema.FieldSchema> arguments = Arrays
				.asList(new Schema.FieldSchema(null, DataType.UNKNOWN));

		funcList.add(new FuncSpec(this.getClass().getName(), new Schema(
				arguments)));
		return funcList;
	}

}
