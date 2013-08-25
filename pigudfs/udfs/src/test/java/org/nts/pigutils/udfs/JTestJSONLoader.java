package org.nts.pigutils.udfs;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/**
 * 
 * Test loading map values with the JSON Loader
 * 
 */
public class JTestJSONLoader {

	
	@Test
	public void testLoadSimpleMap() throws Exception{
		
		String json = "{\"age\":29,\"messages\":[\"msg 1\",\"msg 2\",\"msg 3\"],\"name\":\"mkyong\"}";
		
		Tuple tuple = new JSONLoader()._getNext(TupleFactory.getInstance().newTuple(json));
		assertEquals(HashMap.class, tuple.get(0).getClass());
		
	}
	
	@Test
	public void testLoadEmbeddedMap() throws Exception{
		
		String json1 = "{\"age\":29,\"messages\":[\"msg 1\",\"msg 2\",\"msg 3\"],\"name\":\"mkyong\"}";
		String json = "{\"map\":" + json1 + "}";
		
		Tuple tuple = new JSONLoader()._getNext(TupleFactory.getInstance().newTuple(json));
		assertEquals(29, ((Map) ((Map)tuple.get(0)).get("map")).get("age") );
	}
}
