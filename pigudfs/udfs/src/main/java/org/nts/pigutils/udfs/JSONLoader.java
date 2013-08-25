package org.nts.pigutils.udfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.codehaus.jackson.map.ObjectMapper;

public class JSONLoader extends PigStorage {

	final ObjectMapper mapper = new ObjectMapper();
	final static TupleFactory tupleFactory = TupleFactory.getInstance();

	@Override
	public Tuple getNext() throws IOException {
		return _getNext(super.getNext());
	}

	public final Tuple _getNext(Tuple tuple) throws IOException {

		Object str = null;
		if (tuple.size() > 0 && (str = tuple.get(0)) != null) {
			@SuppressWarnings("rawtypes")
			Map map = removeLists(mapper.readValue(str.toString(),
					HashMap.class));

			return tupleFactory.newTuple(map);
		}
		return null;
	}

	@SuppressWarnings("rawtypes")
	private static final Tuple listToTuple(List list){
		return tupleFactory.newTuple(list);
	}
	
	@SuppressWarnings("rawtypes")
	private static final void removeLists(List list) {
		for (Object val : list) {
			if (val instanceof Map)
				removeLists((Map) val);
			else if (val instanceof List)
				removeLists((List) val);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static final Map removeLists(Map map) {
		for (Object key : map.keySet()) {
			Object val = map.get(key);

			if (val instanceof Map) {
				removeLists((Map) val);
			}else if (val instanceof List) {
				removeLists((List) val);
				map.put(key, listToTuple((List) val) );
			}else if(val instanceof int[]){
				List l = Arrays.asList((int[])val);
				removeLists(l);
				map.put(key, listToTuple(l));
			}else if(val instanceof boolean[]){
				List l = Arrays.asList((boolean[])val);
				removeLists(l);
				map.put(key, listToTuple(l));
			}else if(val instanceof long[]){
				List l = Arrays.asList((long[])val);
				removeLists(l);
				map.put(key, listToTuple(l));
			}else if(val instanceof short[]){
				List l = Arrays.asList((short[])val);
				removeLists(l);
				map.put(key, listToTuple(l));
			}else if(val instanceof float[]){
				List l = Arrays.asList((float[])val);
				removeLists(l);
				map.put(key, listToTuple(l));
			}else if(val instanceof Object[]){
				List l = Arrays.asList((Object[])val);
				removeLists(l);
				map.put(key, listToTuple(l));
			}else if(val instanceof char[]){
				List l = Arrays.asList((char[])val);
				removeLists(l);
				map.put(key, listToTuple(l));
			}else if(val instanceof double[]){
				List l = Arrays.asList((double[])val);
				removeLists(l);
				map.put(key, listToTuple(l));
			}else if(val instanceof byte[]){
				List l = Arrays.asList((byte[])val);
				removeLists(l);
				map.put(key, listToTuple(l));
			}

		}

		return map;
	}

	@Override
	public ResourceSchema getSchema(String arg0, Job arg1) throws IOException {
		return new ResourceSchema(
				new Schema(new FieldSchema("m", DataType.MAP)));
	}

}
