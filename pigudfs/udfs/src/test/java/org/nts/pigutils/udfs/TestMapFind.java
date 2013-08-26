package org.nts.pigutils.udfs;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TestMapFind {

	@Test
	public void testFind() throws Exception{
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("my_info", 99);
		map.put("abc", 1);
		
		Object t = new MAP_FIND(".*(_info)+").exec(TupleFactory.getInstance().newTuple(
				map
				));
		
		System.out.println("Tuple: " + t);
	}
	
}
