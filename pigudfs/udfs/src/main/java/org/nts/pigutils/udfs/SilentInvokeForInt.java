package org.nts.pigutils.udfs;

import java.io.IOException;

import org.apache.pig.builtin.InvokeForInt;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * On exception returns -1
 * 
 */
public class SilentInvokeForInt extends InvokeForInt {

	enum ERROR {
		BAD_FORMAT
	};
	
	

	public SilentInvokeForInt() {
		super();
	}



	public SilentInvokeForInt(String fullName, String paramSpecsStr,
			String isStatic) throws ClassNotFoundException, FrontendException,
			SecurityException, NoSuchMethodException {
		super(fullName, paramSpecsStr, isStatic);
	}



	public SilentInvokeForInt(String fullName, String paramSpecsStr)
			throws FrontendException, SecurityException,
			ClassNotFoundException, NoSuchMethodException {
		super(fullName, paramSpecsStr);
	}



	public SilentInvokeForInt(String fullName) throws FrontendException,
			SecurityException, ClassNotFoundException, NoSuchMethodException {
		super(fullName);
	}



	@Override
	public Integer exec(Tuple input) throws IOException {
		try {
			return super.exec(input);
		} catch (Throwable t) {
			getPigLogger().warn(t, t.toString(), ERROR.BAD_FORMAT);
			return -1;
		}
	}

}
