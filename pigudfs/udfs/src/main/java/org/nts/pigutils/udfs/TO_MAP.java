package org.nts.pigutils.udfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * 
 * To Map converts a Tuple or a Bag of Tuples to a Map Structure.
 * This makes it easy to work with map structures<br/>
 * <p/>
 * The Tuple or Tuples in the Bag is expected to have between one and two keys.<br/>
 * If the Tuple has:<br/>
 * One entry, the entry is placed as a key and the value as null<br/>
 * Two entries, the first is taken as the key and the second as the value<br/>
 * Three or more, the first is taken as the key and the rest placed as a Tuple and taken as the value<br/>
 * 
 * <p/>
 * Important, keys are always converted to Strings.
 */
public class TO_MAP extends EvalFunc<Map<String, Object>>{

	@Override
	public Map<String, Object> exec(Tuple input) throws IOException {
		
		final Object obj;
		if(input == null || input.size() < 1 || (obj = input.get(0)) == null)
			return new HashMap<String, Object>();
		
		
		//if we have a database and the size is 1 then accept that we shoud unroll this into a map.
		if(obj instanceof DataBag && input.size() == 1){
			return convertBag((DataBag)obj);
		}else{
			Map<String, Object> hashMap = new HashMap<String, Object>();
			addTuple(hashMap, input);
			return hashMap;
		}
		
	}

	private Map<String, Object> convertBag(DataBag bag) throws ExecException {
		Map<String, Object> map = new HashMap<String, Object>();
		
		for(Tuple tuple : bag)
			addTuple(map, tuple);
		
		return map;
	}

	private void addTuple(Map<String, Object> hashMap, Tuple tuple) throws ExecException {
		
		if(tuple.size() < 1 || tuple.get(0) == null)
			return;
		
		if(tuple.get(0) instanceof DataBag){
			throw new ExecException("For an input with size > 1 the first entry cannot be a Bag");
		}
		
		if(tuple.size() == 1){
			hashMap.put(tuple.get(0).toString(), null);
		}else if(tuple.size() == 2){
			hashMap.put(tuple.get(0).toString(), tuple.get(1));
		}else{
			int size = tuple.size();
			Tuple innerTuple = TupleFactory.getInstance().newTuple(size-1);
			
			for(int i = 1; i < size; i++)
				innerTuple.set(i-1, tuple.get(i));
			
			hashMap.put(tuple.get(0).toString(), innerTuple);
		}
			
		
	}
	
	

}
