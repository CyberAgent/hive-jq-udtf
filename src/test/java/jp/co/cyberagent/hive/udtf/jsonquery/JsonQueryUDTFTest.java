package jp.co.cyberagent.hive.udtf.jsonquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class JsonQueryUDTFTest {
	private static List<Object> evaluate(final GenericUDTF udtf, final Object... ins) throws HiveException {
		final List<Object> out = new ArrayList<>();
		udtf.setCollector(new Collector() {
			@Override
			public void collect(Object input) throws HiveException {
				out.add(input);
			}
		});
		for (Object in : ins)
			udtf.process(new Object[] { in });
		return out;
	}

	private static final String TEST_JSON = "{\"region\": \"Asia\", \"timezones\": [{\"name\": \"Tokyo\", \"offset\": 540}, {\"name\": \"Taipei\", \"offset\": 480}, {\"name\": \"Kamchatka\", \"offset\": 720}]}";

	private static ConstantObjectInspector toConstantOI(final String text) {
		return PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text(text));
	}

	private static Object toObject(final String text) {
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector.create(text);
	}

	@Test
	public void testSingleColumn1() throws HiveException {
		final JsonQueryUDTF sut = new JsonQueryUDTF();
		final StructObjectInspector oi = sut.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableStringObjectInspector,
				toConstantOI(".timezones[]|select(.name == \"Tokyo\").offset"),
				toConstantOI("int"),
		});
		assertEquals("struct<col1:int>", oi.getTypeName());

		final List<Object> rows = evaluate(sut, toObject(TEST_JSON));
		assertEquals(1, rows.size());
		assertEquals(540, new HivePath(oi, ".col1").extract(rows.get(0)).asInt());
	}

	@Test
	public void testSingleColumn2() throws HiveException {
		final JsonQueryUDTF sut = new JsonQueryUDTF();
		final StructObjectInspector oi = sut.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableStringObjectInspector,
				toConstantOI(".region as $region | .timezones[] | {name: ($region + \"/\" + .name), offset}"),
				toConstantOI("struct<name:string,offset:int>"),
		});
		assertEquals("struct<col1:struct<name:string,offset:int>>", oi.getTypeName());

		final List<Object> results = evaluate(sut, toObject(TEST_JSON));
		assertEquals(3, results.size());

		final HivePath namePath = new HivePath(oi, ".col1.name");
		final HivePath offsetPath = new HivePath(oi, ".col1.offset");

		assertEquals("Asia/Tokyo", namePath.extract(results.get(0)).asString());
		assertEquals(540, offsetPath.extract(results.get(0)).asInt());

		assertEquals("Asia/Taipei", namePath.extract(results.get(1)).asString());
		assertEquals(480, offsetPath.extract(results.get(1)).asInt());

		assertEquals("Asia/Kamchatka", namePath.extract(results.get(2)).asString());
		assertEquals(720, offsetPath.extract(results.get(2)).asInt());
	}

	@Test
	public void testMultiColumn() throws HiveException {
		final JsonQueryUDTF sut = new JsonQueryUDTF();
		final StructObjectInspector oi = sut.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableStringObjectInspector,
				toConstantOI(".region as $region | .timezones[] | {name: ($region + \"/\" + .name), offset}"),
				toConstantOI("name:string"),
				toConstantOI("offset:int"),
		});
		assertEquals("struct<name:string,offset:int>", oi.getTypeName());

		final List<Object> results = evaluate(sut, toObject(TEST_JSON));
		assertEquals(3, results.size());

		final HivePath namePath = new HivePath(oi, ".name");
		final HivePath offsetPath = new HivePath(oi, ".offset");

		assertEquals("Asia/Tokyo", namePath.extract(results.get(0)).asString());
		assertEquals(540, offsetPath.extract(results.get(0)).asInt());

		assertEquals("Asia/Taipei", namePath.extract(results.get(1)).asString());
		assertEquals(480, offsetPath.extract(results.get(1)).asInt());

		assertEquals("Asia/Kamchatka", namePath.extract(results.get(2)).asString());
		assertEquals(720, offsetPath.extract(results.get(2)).asInt());
	}

	@Test
	public void testSubstituteOnError() throws HiveException {
		final JsonQueryUDTF sut = new JsonQueryUDTF();
		final StructObjectInspector oi = sut.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableStringObjectInspector,
				toConstantOI("if $error then \"INVALID\" else . end"),
				toConstantOI("string"),
		});

		final List<Object> results = evaluate(sut, toObject("\"corrupt \"string"));
		assertEquals(1, results.size());
		assertEquals("INVALID", new HivePath(oi, ".col1").extract(results.get(0)).asString());
	}

	@Test
	public void testSkipOnError() throws HiveException {
		final JsonQueryUDTF sut = new JsonQueryUDTF();
		@SuppressWarnings("unused")
		final StructObjectInspector oi = sut.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableStringObjectInspector,
				toConstantOI("if $error then empty else . end"),
				toConstantOI("string"),
		});

		final List<Object> results = evaluate(sut, toObject("\"corrupt \"string"));
		assertEquals(0, results.size());
	}

	@Test
	public void testAbortOnError() throws HiveException {
		final JsonQueryUDTF sut = new JsonQueryUDTF();
		@SuppressWarnings("unused")
		final StructObjectInspector oi = sut.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableStringObjectInspector,
				toConstantOI("if $error then error($error.message) else . end"),
				toConstantOI("string"),
		});

		try {
			evaluate(sut, toObject("\"corrupt \"string"));
			fail("should fail");
		} catch (final HiveException e) {
			assertTrue(e.getMessage().contains("Unrecognized token 'string'"));
		}
	}

	@Test
	public void testTypes() throws HiveException {
		final JsonQueryUDTF sut = new JsonQueryUDTF();

		final StructObjectInspector oi = sut.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableStringObjectInspector,
				toConstantOI("{int:2147483647,bigint:9223372036854775807,float:0.1,double:0.2,boolean:true,string:\"string\",map:{\"foo\":1},array:[\"foo\", \"bar\"],struct:{\"foo\":1}}"),
				toConstantOI("int:int"),
				toConstantOI("bigint:bigint"),
				toConstantOI("float:float"),
				toConstantOI("double:double"),
				toConstantOI("boolean:boolean"),
				toConstantOI("string:string"),
				toConstantOI("map:map<string,int>"),
				toConstantOI("array:array<string>"),
				toConstantOI("struct:struct<foo:int>"),
		});

		final List<Object> results = evaluate(sut, toObject(null));
		assertEquals(1, results.size());

		final Object obj = results.get(0);
		assertEquals(2147483647, new HivePath(oi, ".int").extract(obj).asInt());
		assertEquals(9223372036854775807L, new HivePath(oi, ".bigint").extract(obj).asLong());
		assertEquals(0, Float.compare(0.1f, new HivePath(oi, ".float").extract(obj).asFloat()));
		assertEquals(0, Double.compare(0.2, new HivePath(oi, ".double").extract(obj).asDouble()));
		assertEquals(true, new HivePath(oi, ".boolean").extract(obj).asBoolean());
		assertEquals("string", new HivePath(oi, ".string").extract(obj).asString());

		assertEquals(1, new HivePath(oi, ".map[\"foo\"]").extract(obj).asInt());

		assertEquals("foo", new HivePath(oi, ".array[0]").extract(obj).asString());
		assertEquals("bar", new HivePath(oi, ".array[1]").extract(obj).asString());

		assertEquals(1, new HivePath(oi, ".struct.foo").extract(obj).asInt());
	}

	@Test
	public void testNullOutputs() throws HiveException {
		final JsonQueryUDTF sut = new JsonQueryUDTF();

		final StructObjectInspector oi = sut.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableStringObjectInspector,
				toConstantOI("{int:null,bigint:null,float:null,double:null,boolean:null,string:null,map:null,array:null,struct:{\"foo\":null}}"),
				toConstantOI("int:int"),
				toConstantOI("bigint:bigint"),
				toConstantOI("float:float"),
				toConstantOI("double:double"),
				toConstantOI("boolean:boolean"),
				toConstantOI("string:string"),
				toConstantOI("map:map<string,int>"),
				toConstantOI("array:array<string>"),
				toConstantOI("struct:struct<foo:int>"),
		});

		final List<Object> results = evaluate(sut, toObject(null));
		assertEquals(1, results.size());

		final Object obj = results.get(0);
		assertTrue(new HivePath(oi, ".int").extract(obj).isNull());
		assertTrue(new HivePath(oi, ".bigint").extract(obj).isNull());
		assertTrue(new HivePath(oi, ".float").extract(obj).isNull());
		assertTrue(new HivePath(oi, ".double").extract(obj).isNull());
		assertTrue(new HivePath(oi, ".boolean").extract(obj).isNull());
		assertTrue(new HivePath(oi, ".string").extract(obj).isNull());
		assertTrue(new HivePath(oi, ".map").extract(obj).isNull());
		assertTrue(new HivePath(oi, ".array").extract(obj).isNull());
		assertTrue(new HivePath(oi, ".struct.foo").extract(obj).isNull());
	}

	@Test
	public void testMoreOnStringOutputConversions() throws HiveException {
		final JsonQueryUDTF sut = new JsonQueryUDTF();

		final StructObjectInspector oi = sut.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableStringObjectInspector,
				toConstantOI("{foo: {a: 1}, bar: null, baz: \"baz\"}"),
				toConstantOI("foo:string"),
				toConstantOI("bar:string"),
				toConstantOI("baz:string"),
		});

		final List<Object> results = evaluate(sut, toObject("null"));
		assertEquals(1, results.size());

		final Object obj = results.get(0);
		assertEquals("{\"a\":1}", new HivePath(oi, ".foo").extract(obj).asString());
		assertTrue(new HivePath(oi, ".bar").extract(obj).isNull());
		assertEquals("baz", new HivePath(oi, ".baz").extract(obj).asString());
	}

	@Test
	public void testNullInputs() throws HiveException {
		final JsonQueryUDTF sut = new JsonQueryUDTF();

		final StructObjectInspector oi = sut.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableStringObjectInspector,
				toConstantOI("."),
				toConstantOI("string"),
		});

		final List<Object> results = evaluate(sut, toObject("null"), null, toObject(null));
		assertEquals(3, results.size());

		assertTrue(new HivePath(oi, ".col1").extract(results.get(0)).isNull());
		assertTrue(new HivePath(oi, ".col1").extract(results.get(1)).isNull());
		assertTrue(new HivePath(oi, ".col1").extract(results.get(2)).isNull());
	}

	@Test
	public void testMissingFieldsInConversions() throws HiveException {
		final JsonQueryUDTF sut = new JsonQueryUDTF();

		final StructObjectInspector oi = sut.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableStringObjectInspector,
				toConstantOI("{foo: 10}"),
				toConstantOI("foo:int"),
				toConstantOI("bar:int"),
		});

		final List<Object> results = evaluate(sut, toObject(null));
		assertEquals(1, results.size());

		assertEquals(10, new HivePath(oi, ".foo").extract(results.get(0)).asInt());
		assertTrue(new HivePath(oi, ".bar").extract(results.get(0)).isNull());
	}
}
