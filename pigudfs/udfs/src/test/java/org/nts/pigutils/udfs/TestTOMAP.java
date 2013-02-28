package org.nts.pigutils.udfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TestTOMAP {

	TO_MAP tomap = new TO_MAP();
	
	@Test
	public void testSingleTuple1Entry()throws Exception{
		
		Tuple tuple = TupleFactory.getInstance().newTuple(1);
		tuple.set(0, "A");
		
		Map<String, Object> map = tomap.exec(tuple);
		
		assertEquals(1, map.keySet().size());
		assertEquals("A", map.keySet().toArray()[0]);
		assertNull(map.get(map.keySet().toArray()[0]));
		
	}
	
	@Test
	public void testNull()throws Exception{
		
		Tuple tuple = TupleFactory.getInstance().newTuple(1);
		
		Map<String, Object> map = tomap.exec(tuple);
		
		assertEquals(0, map.keySet().size());
		
		map = tomap.exec(null);
		
		assertEquals(0, map.keySet().size());
		
		tuple.set(0, null);
		map = tomap.exec(tuple);
		
		assertEquals(0, map.keySet().size());
	}
	
	@Test(expected=ExecException.class)
	public void testTupleWithNestedBag()throws Exception{
		
		Tuple tuple = TupleFactory.getInstance().newTuple(2);
		tuple.set(0, "A");
		tuple.set(1, 1);
		
		DataBag bag = new DefaultDataBag();
		bag.add(tuple);
		

		Tuple tuple2 = TupleFactory.getInstance().newTuple(2);
		tuple.set(0, "B");
		tuple.set(1, 2);
		
		Tuple input = TupleFactory.getInstance().newTuple(2);
		input.set(0, bag);
		input.set(1, tuple2);
		
		//we expect an exec exception here
		tomap.exec(input);
		
	}
	
	@Test
	public void testSingleTuple2Entries()throws Exception{
		
		Tuple tuple = TupleFactory.getInstance().newTuple(2);
		tuple.set(0, "A");
		tuple.set(1, 1);
		
		Map<String, Object> map = tomap.exec(tuple);
		
		assertEquals(1, map.keySet().size());
		assertEquals("A", map.keySet().toArray()[0]);
		assertEquals(1, map.get(map.keySet().toArray()[0]));
		
	}
	
	@Test
	public void testSingleTuple4Entries()throws Exception{
		
		Tuple tuple = TupleFactory.getInstance().newTuple(4);
		tuple.set(0, "A");
		tuple.set(1, 1);
		tuple.set(2, "B");
		tuple.set(3, 2);
		
		Map<String, Object> map = tomap.exec(tuple);
		
		assertEquals(1, map.keySet().size());
		assertEquals("A", map.keySet().toArray()[0]);
		Tuple dataTuple = (Tuple)map.get(map.keySet().toArray()[0]);
		
		assertEquals(3, dataTuple.size());
		assertEquals(1, dataTuple.get(0));
		assertEquals("B", dataTuple.get(1));
		assertEquals(2, dataTuple.get(2));
		
	}

	@Test
	public void testBagTuple2Entries()throws Exception{
		
		Tuple tuple1 = TupleFactory.getInstance().newTuple(2);
		tuple1.set(0, "A");
		tuple1.set(1, 1);
		

		Tuple tuple2 = TupleFactory.getInstance().newTuple(2);
		tuple2.set(0, "B");
		tuple2.set(1, 2);
		
		
		DataBag bag = new DefaultDataBag();
		bag.add(tuple1);
		bag.add(tuple2);
		
		Map<String, Object> map = tomap.exec(TupleFactory.getInstance().newTuple(bag));
		
		assertEquals(2, map.keySet().size());
		assertEquals("A", map.keySet().toArray()[0]);
		assertEquals(1, map.get(map.keySet().toArray()[0]));
		assertEquals("B", map.keySet().toArray()[1]);
		assertEquals(2, map.get(map.keySet().toArray()[1]));
		
	}
	
}
