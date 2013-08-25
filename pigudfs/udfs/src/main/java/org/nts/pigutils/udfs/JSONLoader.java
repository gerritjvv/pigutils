package org.nts.pigutils.udfs;

import java.io.IOException;
import java.util.HashMap;

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
	final TupleFactory tupleFactory = TupleFactory.getInstance();

	@Override
	public Tuple getNext() throws IOException{
		return _getNext(super.getNext());
	}
	
	public final Tuple _getNext(Tuple tuple) throws IOException {

		Object str = null;
		if (tuple.size() > 0 && (str = tuple.get(0)) != null) {
			@SuppressWarnings("rawtypes")
			HashMap map = mapper.readValue(str.toString(), HashMap.class);

			return tupleFactory.newTuple(map);
		}
		return null;
	}

	@Override
	public ResourceSchema getSchema(String arg0, Job arg1) throws IOException {
		return new ResourceSchema(new Schema(new FieldSchema("map", DataType.MAP)));
	}
	
}
