package hiveutils.serde.gpb;

import org.apache.hadoop.io.Text;

public class GPBSerialized extends Text{

	public GPBSerialized() {
		super();
	}

	public GPBSerialized(byte[] utf8) {
		super(utf8);
	}

	public GPBSerialized(String string) {
		super(string);
	}

	public GPBSerialized(Text utf8) {
		super(utf8);
	}

	
}
