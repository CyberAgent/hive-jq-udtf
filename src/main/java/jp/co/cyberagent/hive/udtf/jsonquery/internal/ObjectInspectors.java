package jp.co.cyberagent.hive.udtf.jsonquery.internal;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveLexer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ObjectInspectors {
	public static ObjectInspector newObjectInspectorFromHiveType(final String hiveType) {
		return newObjectInspectorFromHiveType(parseHiveType(hiveType));
	}

	private static ObjectInspector newObjectInspectorFromHiveType(final ASTNode type) {
		// matching by token names, because token IDs (which are static final) drastically change between versions.
		switch (type.getToken().getText()) {
			case "TOK_STRING":
				return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
			case "TOK_INT":
				return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
			case "TOK_DOUBLE":
				return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
			case "TOK_FLOAT":
				return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
			case "TOK_BIGINT":
				return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
			case "TOK_BOOLEAN": {
				return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
			}
			case "TOK_STRUCT": {
				final ASTNode tabColList = (ASTNode) type.getChild(0);
				final List<String> names = new ArrayList<>();
				final List<ObjectInspector> ois = new ArrayList<>();
				for (final Node tabCol : tabColList.getChildren()) {
					final ASTNode a = (ASTNode) tabCol;
					names.add(a.getChild(0).toString());
					ois.add(newObjectInspectorFromHiveType((ASTNode) a.getChild(1)));
				}
				return ObjectInspectorFactory.getStandardStructObjectInspector(names, ois);
			}
			case "TOK_MAP": {
				final ObjectInspector keyType = newObjectInspectorFromHiveType((ASTNode) type.getChild(0));
				final ObjectInspector valueType = newObjectInspectorFromHiveType((ASTNode) type.getChild(1));
				return ObjectInspectorFactory.getStandardMapObjectInspector(keyType, valueType);
			}
			case "TOK_LIST": {
				final ObjectInspector itemType = newObjectInspectorFromHiveType((ASTNode) type.getChild(0));
				return ObjectInspectorFactory.getStandardListObjectInspector(itemType);
			}
			default:
				throw new IllegalArgumentException("unsupported type: " + type.toStringTree());
		}
	}

	private static ASTNode parseHiveType(final String hiveType) {
		try {
			final ParseDriver driver = new ParseDriver();
			final HiveLexer lexer = new HiveLexer(driver.new ANTLRNoCaseStringStream(hiveType));
			final HiveParser parser = new HiveParser(new TokenRewriteStream(lexer));
			parser.setTreeAdaptor(ParseDriver.adaptor);

			final HiveParser.type_return type = parser.type();

			final ASTNode ast = (ASTNode) type.getTree();
			ast.setUnknownTokenBoundaries();
			return ast;
		} catch (Exception e) {
			throw new IllegalArgumentException("invalid type: " + hiveType, e);
		}
	}
}