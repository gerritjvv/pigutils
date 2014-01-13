package hiveutils.serde.gpb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message;

/**
 * 
 * Converts GPB objects into Maps
 * 
 */
public class GPBTransform {

	
	
	@SuppressWarnings("unchecked")
	public static Map<String, Object> convert(Message gpb) {

		final Map<String, Object> objMap = new HashMap<String, Object>();

		for (FieldDescriptor d : gpb.getDescriptorForType().getFields()) {
			final Type o = d.getType();

			if (d.isRepeated()) {

				final List<Object> l = new ArrayList<Object>();

				for (Object obj : (Collection<Object>) gpb.getField(d))
					l.add(parseMessage(obj));

				objMap.put(d.getName().toLowerCase(), l);
			} else {
				objMap.put(d.getName().toLowerCase(), parseMessage(d, o, gpb));
			}
		}

		return objMap;
	}

	@SuppressWarnings("rawtypes")
	private static final Object parseMessage(Object obj) {
		if (obj instanceof Message)
			return convert((Message) obj);
		else if(obj.getClass().isEnum())
			return ((Enum)obj).name();
		else if(obj instanceof EnumValueDescriptor)
			return ((EnumValueDescriptor)obj).getName();
		else
			return obj;
	}

	private static final Object parseMessage(FieldDescriptor fd, Type o,
			Message obj) {
		if (o == Type.MESSAGE)
			return convert((Message) obj.getField(fd));
		else
			return obj.getField(fd);
	}

}
