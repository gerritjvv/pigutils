package org.nts.pigutils.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 *
 * Converts any object to a String by calling toString on it
 * 
 */
public class TO_BAG extends EvalFunc<DataBag> {

	private TupleFactory factory = TupleFactory.getInstance();
	
	@Override
	public DataBag exec(Tuple tuple) throws IOException {

		if(tuple != null && tuple.size() > 0){

			List<Tuple> tuples = new ArrayList<Tuple>(tuple.size());
			for(int i = 0; i < tuple.size(); i++){
				Object val = tuple.get(i);
				if(val != null){
					tuples.add(factory.newTuple(unwrap(val)));
				}
			}
			
			if(tuple.size() > 0)
				return new DefaultDataBag(tuples);
			else
				return null;
		}else{
			return null;
		}
		
	}
	
	private static final Object unwrap(Object obj) throws ExecException{
		if(obj == null)
			return null;
		else if(obj instanceof Tuple){
			Tuple tuple = (Tuple)obj;
			if(tuple.size() > 0){
				return unwrap(tuple.get(0));
			}else{
				return null;
			}
		}else{
			return obj;
		}
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
