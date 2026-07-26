package com.tagadvance.seastar.handlers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.ArrayLiteral;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Ordering;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.cql3.TypeCast;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.functions.FunctionCall;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.masking.ColumnMask;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.cql3.statements.schema.AlterTypeStatement;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.schema.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.DropTableStatement;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.cql3.statements.schema.KeyspaceAttributes;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.utils.Pair;

/**
 * Every piece of Cassandra parse-tree state SeaStar reads reflectively, in one table.
 *
 * <p>Only state with no public accessor belongs here. Where cassandra-all exposes the same value
 * publicly - {@code ModificationStatement.Parsed#getConditions()},
 * {@code ColumnCondition.Raw#getValue()}, {@code QualifiedStatement#keyspace()}/{@code name()},
 * {@code Selectable.RawIdentifier#toFieldIdentifier()}, {@code CreateTableStatement.Raw#keyspace()}
 * /{@code table()}, {@code CreateKeyspaceStatement.Raw#keyspaceName} - handlers call that instead.
 *
 * <p>Everything resolves when this class initializes, so a cassandra-all upgrade that renames a
 * field fails loudly and immediately (see {@code FieldBindingsTest}) rather than silently defaulting
 * and answering wrong months later.
 *
 * <p>A handful of the owning classes are package-private in cassandra-all and cannot be named in
 * source, so they are resolved by name; those names are as load-bearing as the field names and are
 * guarded the same way.
 */
final class FieldBindings {

	/**
	 * {@code CQL3Type.Raw} has public {@code isUDT()}/{@code isTuple()}/{@code isVector()} but no
	 * {@code isCollection()}, so identifying a collection means recognizing its implementation class.
	 */
	static final Class<?> RAW_COLLECTION = Reflections.requireClass(
		"org.apache.cassandra.cql3.CQL3Type$Raw$RawCollection");
	static final Class<?> RAW_TYPE = Reflections.requireClass(
		"org.apache.cassandra.cql3.CQL3Type$Raw$RawType");
	private static final Class<?> RAW_TUPLE = Reflections.requireClass(
		"org.apache.cassandra.cql3.CQL3Type$Raw$RawTuple");
	private static final Class<?> RAW_VECTOR = Reflections.requireClass(
		"org.apache.cassandra.cql3.CQL3Type$Raw$RawVector");
	private static final Class<?> RAW_UT = Reflections.requireClass(
		"org.apache.cassandra.cql3.CQL3Type$Raw$RawUT");
	/**
	 * {@code rawType} and {@code rawMask} are public fields, but their declaring class is nested in a
	 * package-private one, so it cannot be named in source and the fields cannot be read directly.
	 */
	private static final Class<?> COLUMN_PROPERTIES_RAW = Reflections.requireClass(
		"org.apache.cassandra.cql3.statements.schema.CreateTableStatement$ColumnProperties$Raw");
	/**
	 * {@code Constants.NULL_LITERAL} is public, but its class is not, so a {@code null} term can only
	 * be recognized by asking the class itself.
	 */
	static final Class<?> NULL_LITERAL = Reflections.requireClass(
		"org.apache.cassandra.cql3.Constants$NullLiteral");

	// CQL3Type.Raw - the parsed type of a column, UDT field or collection element.
	static final FieldBinding<CollectionType.Kind> COLLECTION_KIND = FieldBinding.of(RAW_COLLECTION,
		"kind", CollectionType.Kind.class);
	static final FieldBinding<CQL3Type.Raw> COLLECTION_KEYS = FieldBinding.of(RAW_COLLECTION, "keys",
		CQL3Type.Raw.class);
	static final FieldBinding<CQL3Type.Raw> COLLECTION_VALUES = FieldBinding.of(RAW_COLLECTION,
		"values", CQL3Type.Raw.class);
	static final FieldBinding<CQL3Type> NATIVE_TYPE = FieldBinding.of(RAW_TYPE, "type",
		CQL3Type.class);
	static final FieldBinding<List<CQL3Type.Raw>> TUPLE_TYPES = FieldBinding.ofList(RAW_TUPLE,
		"types");
	static final FieldBinding<CQL3Type.Raw> VECTOR_ELEMENT = FieldBinding.of(RAW_VECTOR, "element",
		CQL3Type.Raw.class);
	static final FieldBinding<Integer> VECTOR_DIMENSION = FieldBinding.of(RAW_VECTOR, "dimension",
		Integer.class);
	static final FieldBinding<UTName> USER_TYPE_NAME = FieldBinding.of(RAW_UT, "name", UTName.class);

	// Terms and bind markers. A map literal publishes its entries; the other literals do not.
	static final FieldBinding<Integer> MARKER_BIND_INDEX = FieldBinding.of(AbstractMarker.Raw.class,
		"bindIndex", Integer.class);
	static final FieldBinding<Term.Raw> SET_VALUE = FieldBinding.of(Operation.SetValue.class, "value",
		Term.Raw.class);
	/**
	 * {@code [1, 2]} parses as an {@code ArrayLiteral} rather than a {@code Lists.Literal}: since
	 * vectors were added the receiving type decides which of the two a bracket literal means.
	 */
	static final FieldBinding<List<Term.Raw>> ARRAY_ELEMENTS = FieldBinding.ofList(
		ArrayLiteral.class, "elements");
	static final FieldBinding<List<Term.Raw>> SET_ELEMENTS = FieldBinding.ofList(Sets.Literal.class,
		"elements");
	static final FieldBinding<List<Term.Raw>> TUPLE_ELEMENTS = FieldBinding.ofList(
		Tuples.Literal.class, "elements");
	static final FieldBinding<CQL3Type.Raw> TYPE_CAST_TYPE = FieldBinding.of(TypeCast.class, "type",
		CQL3Type.Raw.class);
	static final FieldBinding<Term.Raw> TYPE_CAST_TERM = FieldBinding.of(TypeCast.class, "term",
		Term.Raw.class);
	static final FieldBinding<FunctionName> FUNCTION_NAME = FieldBinding.of(FunctionCall.Raw.class,
		"name", FunctionName.class);
	static final FieldBinding<List<Term.Raw>> FUNCTION_TERMS = FieldBinding.ofList(
		FunctionCall.Raw.class, "terms");

	/**
	 * SELECT ORDER BY. {@code SelectStatement.Parameters#orderings} is public, but the elements it
	 * holds publish nothing: {@code Ordering.Raw} and its {@code SingleColumn} expression keep the
	 * column and the direction package-private, and {@code bind} - the only public reader - resolves
	 * them against a {@code TableMetadata} SeaStar does not have.
	 */
	static final FieldBinding<Ordering.Raw.Expression> ORDERING_EXPRESSION = FieldBinding.of(
		Ordering.Raw.class, "expression", Ordering.Raw.Expression.class);
	static final FieldBinding<Ordering.Direction> ORDERING_DIRECTION = FieldBinding.of(
		Ordering.Raw.class, "direction", Ordering.Direction.class);
	static final FieldBinding<ColumnIdentifier> ORDERING_COLUMN = FieldBinding.of(
		Ordering.Raw.SingleColumn.class, "column", ColumnIdentifier.class);

	// Lightweight-transaction IF conditions. The condition's value has a public getValue().
	static final FieldBinding<Operator> CONDITION_OPERATOR = FieldBinding.of(
		ColumnCondition.Raw.class, "operator", Operator.class);
	static final FieldBinding<Term.Raw> CONDITION_COLLECTION_ELEMENT = FieldBinding.of(
		ColumnCondition.Raw.class, "collectionElement", Term.Raw.class);
	static final FieldBinding<FieldIdentifier> CONDITION_UDT_FIELD = FieldBinding.of(
		ColumnCondition.Raw.class, "udtField", FieldIdentifier.class);
	static final FieldBinding<List<Term.Raw>> CONDITION_IN_VALUES = FieldBinding.ofList(
		ColumnCondition.Raw.class, "inValues");
	static final FieldBinding<AbstractMarker.INRaw> CONDITION_IN_MARKER = FieldBinding.of(
		ColumnCondition.Raw.class, "inMarker", AbstractMarker.INRaw.class);

	// INSERT / UPDATE / DELETE.
	static final FieldBinding<Boolean> MODIFICATION_IF_NOT_EXISTS = FieldBinding.of(
		ModificationStatement.Parsed.class, "ifNotExists", Boolean.class);
	static final FieldBinding<Boolean> MODIFICATION_IF_EXISTS = FieldBinding.of(
		ModificationStatement.Parsed.class, "ifExists", Boolean.class);
	static final FieldBinding<List<ColumnIdentifier>> INSERT_COLUMN_NAMES = FieldBinding.ofList(
		UpdateStatement.ParsedInsert.class, "columnNames");
	static final FieldBinding<List<Term.Raw>> INSERT_COLUMN_VALUES = FieldBinding.ofList(
		UpdateStatement.ParsedInsert.class, "columnValues");
	static final FieldBinding<List<Pair<ColumnIdentifier, Operation.RawUpdate>>> UPDATE_UPDATES =
		FieldBinding.ofList(UpdateStatement.ParsedUpdate.class, "updates");
	static final FieldBinding<WhereClause> UPDATE_WHERE_CLAUSE = FieldBinding.of(
		UpdateStatement.ParsedUpdate.class, "whereClause", WhereClause.class);
	static final FieldBinding<List<Operation.RawDeletion>> DELETE_DELETIONS = FieldBinding.ofList(
		DeleteStatement.Parsed.class, "deletions");
	static final FieldBinding<WhereClause> DELETE_WHERE_CLAUSE = FieldBinding.of(
		DeleteStatement.Parsed.class, "whereClause", WhereClause.class);
	static final FieldBinding<List<ModificationStatement.Parsed>> BATCH_STATEMENTS =
		FieldBinding.ofList(BatchStatement.Parsed.class, "parsedStatements");

	// Schema statements.
	static final FieldBinding<Boolean> CREATE_KEYSPACE_IF_NOT_EXISTS = FieldBinding.of(
		CreateKeyspaceStatement.Raw.class, "ifNotExists", Boolean.class);
	static final FieldBinding<KeyspaceAttributes> CREATE_KEYSPACE_ATTRIBUTES = FieldBinding.of(
		CreateKeyspaceStatement.Raw.class, "attrs", KeyspaceAttributes.class);
	/**
	 * {@code PropertyDefinitions#properties} is {@code protected} and its only public readers are
	 * typed accessors ({@code getBoolean}, {@code getInt}); the replication map is reachable only
	 * through the private {@code getAllReplicationOptions}, so the raw map is read directly.
	 */
	static final FieldBinding<Map<String, Object>> PROPERTY_DEFINITIONS_PROPERTIES =
		FieldBinding.ofMap(PropertyDefinitions.class, "properties");
	static final FieldBinding<String> DROP_KEYSPACE_NAME = FieldBinding.of(
		DropKeyspaceStatement.Raw.class, "keyspaceName", String.class);
	static final FieldBinding<Boolean> DROP_KEYSPACE_IF_EXISTS = FieldBinding.of(
		DropKeyspaceStatement.Raw.class, "ifExists", Boolean.class);

	static final FieldBinding<Boolean> CREATE_TABLE_IF_NOT_EXISTS = FieldBinding.of(
		CreateTableStatement.Raw.class, "ifNotExists", Boolean.class);
	static final FieldBinding<Boolean> CREATE_TABLE_USE_COMPACT_STORAGE = FieldBinding.of(
		CreateTableStatement.Raw.class, "useCompactStorage", Boolean.class);
	static final FieldBinding<Map<ColumnIdentifier, Object>> CREATE_TABLE_RAW_COLUMNS =
		FieldBinding.ofMap(CreateTableStatement.Raw.class, "rawColumns");
	static final FieldBinding<List<ColumnIdentifier>> CREATE_TABLE_PARTITION_KEY_COLUMNS =
		FieldBinding.ofList(CreateTableStatement.Raw.class, "partitionKeyColumns");
	static final FieldBinding<List<ColumnIdentifier>> CREATE_TABLE_CLUSTERING_COLUMNS =
		FieldBinding.ofList(CreateTableStatement.Raw.class, "clusteringColumns");
	static final FieldBinding<Map<ColumnIdentifier, Boolean>> CREATE_TABLE_CLUSTERING_ORDER =
		FieldBinding.ofMap(CreateTableStatement.Raw.class, "clusteringOrder");
	static final FieldBinding<Set<ColumnIdentifier>> CREATE_TABLE_STATIC_COLUMNS = FieldBinding.ofSet(
		CreateTableStatement.Raw.class, "staticColumns");
	static final FieldBinding<CQL3Type.Raw> COLUMN_RAW_TYPE = FieldBinding.of(COLUMN_PROPERTIES_RAW,
		"rawType", CQL3Type.Raw.class);
	static final FieldBinding<ColumnMask.Raw> COLUMN_RAW_MASK = FieldBinding.of(
		COLUMN_PROPERTIES_RAW, "rawMask", ColumnMask.Raw.class);
	static final FieldBinding<QualifiedName> DROP_TABLE_NAME = FieldBinding.of(
		DropTableStatement.Raw.class, "name", QualifiedName.class);
	static final FieldBinding<Boolean> DROP_TABLE_IF_EXISTS = FieldBinding.of(
		DropTableStatement.Raw.class, "ifExists", Boolean.class);

	static final FieldBinding<UTName> CREATE_TYPE_NAME = FieldBinding.of(
		CreateTypeStatement.Raw.class, "name", UTName.class);
	static final FieldBinding<Boolean> CREATE_TYPE_IF_NOT_EXISTS = FieldBinding.of(
		CreateTypeStatement.Raw.class, "ifNotExists", Boolean.class);
	static final FieldBinding<List<FieldIdentifier>> CREATE_TYPE_FIELD_NAMES = FieldBinding.ofList(
		CreateTypeStatement.Raw.class, "fieldNames");
	static final FieldBinding<List<CQL3Type.Raw>> CREATE_TYPE_RAW_FIELD_TYPES = FieldBinding.ofList(
		CreateTypeStatement.Raw.class, "rawFieldTypes");

	static final FieldBinding<UTName> ALTER_TYPE_NAME = FieldBinding.of(AlterTypeStatement.Raw.class,
		"name", UTName.class);
	static final FieldBinding<Boolean> ALTER_TYPE_IF_EXISTS = FieldBinding.of(
		AlterTypeStatement.Raw.class, "ifExists", Boolean.class);
	static final FieldBinding<Enum<?>> ALTER_TYPE_KIND = FieldBinding.ofEnum(
		AlterTypeStatement.Raw.class, "kind");
	static final FieldBinding<FieldIdentifier> ALTER_TYPE_NEW_FIELD_NAME = FieldBinding.of(
		AlterTypeStatement.Raw.class, "newFieldName", FieldIdentifier.class);
	static final FieldBinding<CQL3Type.Raw> ALTER_TYPE_NEW_FIELD_TYPE = FieldBinding.of(
		AlterTypeStatement.Raw.class, "newFieldType", CQL3Type.Raw.class);
	static final FieldBinding<Boolean> ALTER_TYPE_IF_FIELD_NOT_EXISTS = FieldBinding.of(
		AlterTypeStatement.Raw.class, "ifFieldNotExists", Boolean.class);
	static final FieldBinding<Map<FieldIdentifier, FieldIdentifier>> ALTER_TYPE_RENAMED_FIELDS =
		FieldBinding.ofMap(AlterTypeStatement.Raw.class, "renamedFields");
	static final FieldBinding<Boolean> ALTER_TYPE_IF_FIELD_EXISTS = FieldBinding.of(
		AlterTypeStatement.Raw.class, "ifFieldExists", Boolean.class);

	static final FieldBinding<QualifiedName> CREATE_INDEX_TABLE_NAME = FieldBinding.of(
		CreateIndexStatement.Raw.class, "tableName", QualifiedName.class);
	static final FieldBinding<QualifiedName> CREATE_INDEX_INDEX_NAME = FieldBinding.of(
		CreateIndexStatement.Raw.class, "indexName", QualifiedName.class);
	static final FieldBinding<List<IndexTarget.Raw>> CREATE_INDEX_RAW_TARGETS = FieldBinding.ofList(
		CreateIndexStatement.Raw.class, "rawIndexTargets");
	static final FieldBinding<Boolean> CREATE_INDEX_IF_NOT_EXISTS = FieldBinding.of(
		CreateIndexStatement.Raw.class, "ifNotExists", Boolean.class);
	static final FieldBinding<ColumnIdentifier> INDEX_TARGET_COLUMN = FieldBinding.of(
		IndexTarget.Raw.class, "column", ColumnIdentifier.class);

	private FieldBindings() {
		// hidden constructor
	}

}
