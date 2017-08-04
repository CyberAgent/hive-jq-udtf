package jp.co.cyberagent.hive.udtf.jsonquery.internal;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;

public class ResultObjectMarshaller {
	private final boolean whole;
	private final StructObjectInspector oi;

	public ResultObjectMarshaller(final boolean whole, final StructObjectInspector oi) {
		this.whole = whole;
		this.oi = oi;
	}

	public StructObjectInspector objectInspector() {
		return oi;
	}

	public Object marshal(final JsonNode json) {
		if (whole) {
			final Object[] values = new Object[1];
			values[0] = marshal(oi.getAllStructFieldRefs().get(0).getFieldObjectInspector(), json);
			return values;
		} else {
			return marshal(oi, json);
		}
	}

	private static Object marshal(final ObjectInspector iface, final JsonNode json) {
		if (json == null || json.isNull())
			return null;
		if (iface instanceof WritableStringObjectInspector) {
			final WritableStringObjectInspector inspector = (WritableStringObjectInspector) iface;
			if (json.isTextual())
				return inspector.create(json.asText());
			return inspector.create(json.toString());
		} else if (iface instanceof StandardStructObjectInspector) {
			final StandardStructObjectInspector inspector = (StandardStructObjectInspector) iface;
			final Object out = inspector.create();
			for (final StructField field : inspector.getAllStructFieldRefs()) {
				inspector.setStructFieldData(out, field,
						marshal(field.getFieldObjectInspector(), json.get(field.getFieldName())));
			}
			return out;
		} else if (iface instanceof StandardMapObjectInspector) {
			final StandardMapObjectInspector inspector = (StandardMapObjectInspector) iface;
			final Object out = inspector.create();
			final Iterator<Entry<String, JsonNode>> iter = json.fields();
			while (iter.hasNext()) {
				final Entry<String, JsonNode> item = iter.next();
				inspector.put(out, marshal(inspector.getMapKeyObjectInspector(), new TextNode(item.getKey())),
						marshal(inspector.getMapValueObjectInspector(), item.getValue()));
			}
			return out;
		} else if (iface instanceof StandardListObjectInspector) {
			final StandardListObjectInspector inspector = (StandardListObjectInspector) iface;
			final Object out = inspector.create(json.size());
			final Iterator<JsonNode> iter = json.elements();
			for (int i = 0; iter.hasNext(); ++i)
				inspector.set(out, i,
						marshal(inspector.getListElementObjectInspector(), iter.next()));
			return out;
		} else if (iface instanceof WritableIntObjectInspector) {
			final WritableIntObjectInspector inspector = (WritableIntObjectInspector) iface;
			return inspector.create(json.asInt());
		} else if (iface instanceof WritableFloatObjectInspector) {
			final WritableFloatObjectInspector inspector = (WritableFloatObjectInspector) iface;
			return inspector.create((float) json.asDouble());
		} else if (iface instanceof WritableDoubleObjectInspector) {
			final WritableDoubleObjectInspector inspector = (WritableDoubleObjectInspector) iface;
			return inspector.create(json.asDouble());
		} else if (iface instanceof WritableLongObjectInspector) {
			final WritableLongObjectInspector inspector = (WritableLongObjectInspector) iface;
			return inspector.create(json.asLong());
		} else if (iface instanceof WritableBooleanObjectInspector) {
			final WritableBooleanObjectInspector inspector = (WritableBooleanObjectInspector) iface;
			return inspector.create(json.asBoolean());
		} else {
			throw new IllegalArgumentException("unsupported inspector: " + iface.getTypeName());
		}
	}
}