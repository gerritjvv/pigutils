package org.nts.pigutils.udfs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Takes a timestamp as input for formats it into a date pattern based on
 * SimpleDateFormat.<br/>
 * To use:<br/>
 * DateFormat(ts, '${FORMAT STRING}', 'timezone')<br/>
 * e.g.<br/>
 * DateFormat(ts, 'yyyy-MM-dd', 'timezone')<br/>
 * 
 */
public class DateFormat extends EvalFunc<String>{

	enum ERROR { WRONG_TS_TYPE, TS_NULL }
	
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
		
		String datePattern = tuple.get(1).toString();
	
		SimpleDateFormat format = new SimpleDateFormat(datePattern);
		if(tuple.size() > 2){
			format.setTimeZone(TimeZone.getTimeZone(tuple.get(2).toString()));
		}
		
		return new SimpleDateFormat(datePattern).format(new Date(ts));
		
	}

}
