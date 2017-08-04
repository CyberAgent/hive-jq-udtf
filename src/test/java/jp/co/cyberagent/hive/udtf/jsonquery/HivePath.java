package jp.co.cyberagent.hive.udtf.jsonquery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;

public class HivePath {
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static final Pattern STRUCT_ACCESS_PATTERN = Pattern.compile("^\\.(?<field>[a-zA-Z_][a-zA-Z0-9_]*)");
	private static final Pattern ARRAY_ACCESS_PATTERN = Pattern.compile("^\\[(?<index>0|[1-9][0-9]*)\\]");
	private static final Pattern MAP_ACCESS_PATTERN = Pattern.compile("^\\[(?<keyjson>\"([^\"]|\\\\\")+\")\\]");

	public interface Accessor {
		HivePath.Value extract(ObjectInspector oi, Object obj);

		default HivePath.Value extract(HivePath.Value value) {
			return extract(value.oi, value.obj);
		}
	}

	public static class Value {
		public final ObjectInspector oi;
		public final Object obj;

		public Value(final ObjectInspector oi, final Object obj) {
			this.oi = oi;
			this.obj = obj;
		}

		public int asInt() {
			return ((IntObjectInspector) oi).get(obj);
		}

		public Object asString() {
			return ((StringObjectInspector) oi).getPrimitiveJavaObject(obj);
		}

		public long asLong() {
			return ((LongObjectInspector) oi).get(obj);
		}

		public float asFloat() {
			return ((FloatObjectInspector) oi).get(obj);
		}

		public double asDouble() {
			return ((DoubleObjectInspector) oi).get(obj);
		}

		public Object asBoolean() {
			return ((BooleanObjectInspector) oi).get(obj);
		}

		public boolean isNull() {
			return obj == null;
		}
	}

	public static class MapAccessor implements HivePath.Accessor {
		private final String key;

		public MapAccessor(final String key) {
			this.key = key;
		}

		@Override
		public String toString() {
			return "[" + new TextNode(key) + "]";
		}

		@Override
		public HivePath.Value extract(final ObjectInspector oi, final Object obj) {
			final ObjectInspector retOI = ((MapObjectInspector) oi).getMapValueObjectInspector();
			final Object retObj = ((MapObjectInspector) oi).getMapValueElement(obj, PrimitiveObjectInspectorFactory.writableStringObjectInspector.create(key));
			return new Value(retOI, retObj);
		}
	}

	public static class ArrayAccessor implements HivePath.Accessor {
		private final int index;

		public ArrayAccessor(final int index) {
			this.index = index;
		}

		@Override
		public String toString() {
			return "[" + index + "]";
		}

		@Override
		public HivePath.Value extract(final ObjectInspector oi, final Object obj) {
			final ObjectInspector retOI = ((ListObjectInspector) oi).getListElementObjectInspector();
			final Object retObj = ((ListObjectInspector) oi).getListElement(obj, index);
			return new Value(retOI, retObj);
		}
	}

	public static class StructAccessor implements HivePath.Accessor {
		private final String field;

		public StructAccessor(final String field) {
			this.field = field;
		}

		@Override
		public String toString() {
			return "." + field;
		}

		@Override
		public HivePath.Value extract(final ObjectInspector oi, final Object obj) {
			final StructField fieldRef = ((StructObjectInspector) oi).getStructFieldRef(field);
			final ObjectInspector retOI = fieldRef.getFieldObjectInspector();
			final Object retObj = ((StructObjectInspector) oi).getStructFieldData(obj, fieldRef);
			return new Value(retOI, retObj);
		}
	}

	private final List<HivePath.Accessor> accessors;
	private final ObjectInspector oi;

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		for (final HivePath.Accessor accessor : accessors)
			builder.append(accessor);
		return builder.toString();
	}

	public HivePath(final ObjectInspector oi, final String path) {
		this.oi = oi;
		this.accessors = new ArrayList<>();

		final Matcher m = STRUCT_ACCESS_PATTERN.matcher(path);

		while (m.usePattern(STRUCT_ACCESS_PATTERN).find()) {
			accessors.add(new StructAccessor(m.group("field")));
			m.region(m.end(), m.regionEnd());

			boolean match;
			do {
				match = false;
				if (m.usePattern(ARRAY_ACCESS_PATTERN).find()) {
					accessors.add(new ArrayAccessor(Integer.parseInt(m.group("index"))));
					m.region(m.end(), m.regionEnd());
					match = true;
				}
				if (m.usePattern(MAP_ACCESS_PATTERN).find()) {
					final String key;
					try {
						key = MAPPER.readValue(m.group("keyjson"), String.class);
					} catch (IOException e) {
						throw new IllegalArgumentException(e);
					}
					accessors.add(new MapAccessor(key));
					m.region(m.end(), m.regionEnd());
					match = true;
				}
			} while (match);
		}

		if (!m.hitEnd())
			throw new IllegalArgumentException("error at " + m.regionStart());
	}

	public HivePath.Value extract(final Object obj) {
		HivePath.Value v = new Value(oi, obj);
		for (final HivePath.Accessor accessor : accessors)
			v = accessor.extract(v);
		return v;
	}
}