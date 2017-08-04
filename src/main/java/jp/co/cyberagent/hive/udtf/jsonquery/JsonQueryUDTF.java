package jp.co.cyberagent.hive.udtf.jsonquery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

import jp.co.cyberagent.hive.udtf.jsonquery.internal.ObjectInspectors;
import jp.co.cyberagent.hive.udtf.jsonquery.internal.Pair;
import jp.co.cyberagent.hive.udtf.jsonquery.internal.ResultObjectMarshaller;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.exception.JsonQueryException;

@Description(name = "jq", value = "_FUNC_(JSON, 'JQ', 'TYPE'), _FUNC_(JSON, 'JQ', 'FIELD_1:TYPE_1', ..., 'FIELD_N:TYPE_N') - jq for hive")
public class JsonQueryUDTF extends GenericUDTF {

	private transient JsonQuery jq;
	private transient StringObjectInspector in;
	private transient ResultObjectMarshaller marshaller;

	// mapper is created per instance because it has costly synchronized block inside which causes heavy lock contention
	private transient ObjectMapper mapper;
	// scope is created per instance because Scope is not thread-safe.
	private transient Scope scope;

	private StructObjectInspector initialize(final ObjectInspector jsonArg, final ObjectInspector jqArg, final List<ObjectInspector> nameAndTypeArgs) throws UDFArgumentException {
		this.in = Arguments.asString(jsonArg, "JSON");

		try {
			this.jq = JsonQuery.compile(Arguments.asConstantNonNullString(jqArg, "JQ"));
		} catch (final JsonQueryException e) {
			throw new UDFArgumentException("JQ is invalid: " + e.getMessage());
		}

		this.marshaller = ResultObjectMarshallers.create(Arguments.asConstantNonNullStrings(nameAndTypeArgs, "TYPE or NAME:TYPE"));

		this.scope = new Scope();
		this.mapper = new ObjectMapper(new JsonFactory().enable(Feature.ALLOW_UNQUOTED_CONTROL_CHARS));

		return marshaller.objectInspector();
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (args.length < 3)
			throw new UDFArgumentException("jq() takes at least three arguments.");
		return initialize(args[0], args[1], Arrays.asList(Arrays.copyOfRange(args, 2, args.length)));
	}

	@Override
	public void process(Object[] o) throws HiveException {
		final String jsonText = in.getPrimitiveJavaObject(o[0]);

		JsonNode json;
		JsonNode error;
		try {
			if (jsonText == null) {
				json = NullNode.getInstance();
			} else {
				try (final JsonParser parser = mapper.getFactory().createParser(jsonText)) {
					json = mapper.readTree(parser);
					if (parser.nextToken() != null)
						throw new JsonParseException(parser, "trailing characters");
				}
			}
			error = NullNode.getInstance();
		} catch (final Exception e) {
			json = NullNode.getInstance();
			error = mapper.valueToTree(new ErrorObject(e.getMessage(), e.getClass().getName(), jsonText));
		}

		scope.setValue("error", error);

		final List<JsonNode> outs;
		try {
			outs = jq.apply(scope, json);
		} catch (final Exception e) {
			throw new HiveException("jq returned an error \"" + e.getMessage() + "\" from input: " + jsonText);
		}

		for (final JsonNode n : outs) {
			forward(marshaller.marshal(n));
		}
	}

	@Override
	public String toString() {
		return "jq";
	}

	@Override
	public void close() throws HiveException {}

	@JsonIgnoreProperties(ignoreUnknown = true)
	private static class ErrorObject {
		@JsonProperty("message")
		public String message;

		@JsonProperty("class")
		public String clazz;

		@JsonProperty("input")
		public String input;

		public ErrorObject(final String message, final String clazz, final String input) {
			this.message = message;
			this.clazz = clazz;
			this.input = input;
		}
	}

	private static class Arguments {
		public static String asConstantNonNullString(final ObjectInspector oi, final String name) throws UDFArgumentException {
			if (!(oi instanceof WritableConstantStringObjectInspector))
				throw new UDFArgumentException(name + " must be a constant string.");
			final Text text = ((WritableConstantStringObjectInspector) oi).getWritableConstantValue();
			if (text == null)
				throw new UDFArgumentException(name + " must not be NULL.");
			return text.toString();
		}

		public static List<String> asConstantNonNullStrings(final List<ObjectInspector> ois, final String name) throws UDFArgumentException {
			final List<String> strs = new ArrayList<>();
			for (final ObjectInspector oi : ois)
				strs.add(asConstantNonNullString(oi, name));
			return strs;
		}

		public static StringObjectInspector asString(final ObjectInspector oi, final String name) throws UDFArgumentException {
			if (!(oi instanceof StringObjectInspector))
				throw new UDFArgumentException(name + " must be of string type.");
			return (StringObjectInspector) oi;
		}
	}

	private static class ResultObjectMarshallers {
		private static final Pattern NAME_AND_TYPE_PATTERN = Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*):[a-zA-Z_].*");

		private static Pair<String, ObjectInspector> parseNameAndType(final String nameAndType, final boolean requireName) throws UDFArgumentException {
			try {
				final Matcher m = NAME_AND_TYPE_PATTERN.matcher(nameAndType);
				if (m.matches()) {
					final String name = m.group(1);
					final ObjectInspector oi = ObjectInspectors.newObjectInspectorFromHiveType(nameAndType.substring(name.length() + 1));
					return Pair.of(name, oi);
				}
				if (requireName)
					throw new UDFArgumentException("Can't parse NAME:TYPE from \"" + nameAndType + "\". NAME is required.");
				return Pair.of(null, ObjectInspectors.newObjectInspectorFromHiveType(nameAndType));
			} catch (final Exception e) {
				throw new UDFArgumentException("Can't parse NAME:TYPE or TYPE from \"" + nameAndType + "\". " + e.getMessage());
			}
		}

		public static ResultObjectMarshaller create(final List<String> nameAndTypeArgs) throws UDFArgumentException {
			final List<String> columns = new ArrayList<>(nameAndTypeArgs.size());
			final List<ObjectInspector> inspectors = new ArrayList<>(nameAndTypeArgs.size());

			for (int i = 0; i < nameAndTypeArgs.size(); ++i) {
				final Pair<String, ObjectInspector> nameAndType = parseNameAndType(nameAndTypeArgs.get(i), i > 0);
				columns.add(nameAndType._1);
				inspectors.add(nameAndType._2);
			}

			if (columns.size() == 1 && columns.get(0) == null) { // _FUNC_(JSON, 'JQ', 'TYPE') form
				columns.set(0, "col1");
				return new ResultObjectMarshaller(true, ObjectInspectorFactory.getStandardStructObjectInspector(columns, inspectors));
			} else { // _FUNC_(JSON, 'JQ', 'FIELD_1:TYPE_1', ..., 'FIELD_N:TYPE_N') form
				return new ResultObjectMarshaller(false, ObjectInspectorFactory.getStandardStructObjectInspector(columns, inspectors));
			}
		}
	}
}
