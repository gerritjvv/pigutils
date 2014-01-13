package org.nts.pigutils.udfs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Converts time in milliseconds to the W3 DateFormat specification  (http://www.w3.org/TR/xmlschema-2/)
 */
public class W3DateFormat extends EvalFunc<String>{

	enum ERROR { WRONG_TS_TYPE, TS_NULL }
	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	@Override
	public String exec(Tuple tuple) throws IOException {
		
		 
		Object tsObj = tuple.get(0);
		if(tsObj == null){
			warn("null", ERROR.TS_NULL);
			return null;
		}
		
		long ts;
		try{
			ts = Long.parseLong(tsObj.toString());
		}catch(NumberFormatException fe){
			warn("Wrong Format: " + tsObj, ERROR.WRONG_TS_TYPE);
			return null;
		}
		
		return df.format(new Date(ts));
		
	}

}
