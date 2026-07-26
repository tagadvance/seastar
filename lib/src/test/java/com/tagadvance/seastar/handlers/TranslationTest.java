package com.tagadvance.seastar.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.tagadvance.seastar.SeaStarCqlSession;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.SelectStatement.RawStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsert;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedUpdate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The translation layer answers for a parsed statement without one being executed, which is the
 * point of keeping it separate from the handlers: a WHERE clause can be read, and the reading
 * checked, without a session, a result set or a row.
 */
class TranslationTest {

	private static final Supplier<Optional<CqlIdentifier>> NO_SESSION_KEYSPACE = Optional::empty;
	private static final Supplier<Optional<CqlIdentifier>> SESSION_KEYSPACE =
		() -> Optional.of(CqlIdentifier.fromInternal("ks"));

	private SeaStarCqlSession session;
	private SeaStarDriverContext context;
	private Node node;
	private SeaStarTable people;
	private SeaStarTable events;

	@BeforeEach
	void beforeEach() {
		session = SeaStarCqlSession.builder().build();
		context = session.getContext();
		node = context.getNode();

		final var keyspace = context.newSeaStarKeyspace("ks");

		people = keyspace.newSeaStarTable("people");
		people.addColumn("id", DataTypes.INT);
		people.addColumn("name", DataTypes.TEXT);
		people.addColumn("age", DataTypes.INT);
		people.addColumn("tags", DataTypes.listOf(DataTypes.TEXT));
		people.markPartitionKey(CqlIdentifier.fromInternal("id"));

		events = keyspace.newSeaStarTable("events");
		events.addColumn("pk", DataTypes.INT);
		events.addColumn("ck", DataTypes.INT);
		events.addColumn("note", DataTypes.TEXT);
		events.markPartitionKey(CqlIdentifier.fromInternal("pk"));
		events.markClustering(CqlIdentifier.fromInternal("ck"), ClusteringOrder.ASC);
	}

	@AfterEach
	void afterEach() {
		session.close();
	}

	private static CQLStatement.Raw parse(final String cql) {
		return QueryProcessor.parseStatement(cql);
	}

	private static CqlIdentifier id(final String name) {
		return CqlIdentifier.fromInternal(name);
	}

	private static Restriction.Column column(final SeaStarTable table, final String name) {
		final var identifier = id(name);
		final var index = table.firstIndexOf(identifier);

		return new Restriction.Column(index, identifier, table.get(index).getType());
	}

	@Nested
	@DisplayName("Targets")
	class TargetsTest {

		@Test
		@DisplayName("an unqualified statement resolves against the session keyspace")
		void unqualified() {
			final var raw = (RawStatement) parse("SELECT * FROM people");

			final var target = Targets.require(context, SESSION_KEYSPACE, raw, node);

			assertSame(people, target.table());
		}

		@Test
		@DisplayName("a qualified statement resolves without a session keyspace")
		void qualified() {
			final var raw = (RawStatement) parse("SELECT * FROM ks.people");

			final var target = Targets.require(context, NO_SESSION_KEYSPACE, raw, node);

			assertSame(people, target.table());
		}

		@Test
		@DisplayName("an unqualified statement with no session keyspace names the missing USE")
		void noKeyspace() {
			final var raw = (RawStatement) parse("SELECT * FROM people");

			final var error = assertThrows(InvalidQueryException.class,
				() -> Targets.require(context, NO_SESSION_KEYSPACE, raw, node));

			assertEquals("No keyspace has been specified. USE a keyspace, or explicitly specify "
				+ "keyspace.tablename", error.getMessage());
		}

		@Test
		@DisplayName("an unknown keyspace is reported before the table is looked up")
		void unknownKeyspace() {
			final var raw = (RawStatement) parse("SELECT * FROM nope.people");

			final var error = assertThrows(InvalidQueryException.class,
				() -> Targets.require(context, SESSION_KEYSPACE, raw, node));

			assertTrue(error.getMessage().contains("nope"), error.getMessage());
		}

		@Test
		@DisplayName("an unknown table is reported by name")
		void unknownTable() {
			final var raw = (RawStatement) parse("SELECT * FROM ks.nope");

			final var error = assertThrows(InvalidQueryException.class,
				() -> Targets.require(context, SESSION_KEYSPACE, raw, node));

			assertTrue(error.getMessage().contains("nope"), error.getMessage());
		}

		@Test
		@DisplayName("primary key names are the partition key then the clustering columns, in order")
		void primaryKeyNames() {
			final var raw = (RawStatement) parse("SELECT * FROM ks.events");

			final var target = Targets.require(context, NO_SESSION_KEYSPACE, raw, node);

			assertSame(events, target.table());
			assertEquals(List.of(id("pk"), id("ck")), List.copyOf(target.primaryKeyNames()));
			assertEquals(List.of(id("pk")), List.copyOf(target.partitionKeyNames()));
		}

	}

	@Nested
	@DisplayName("Restrictions")
	class RestrictionsTest {

		private List<Restriction> restrictionsOf(final String cql, final Object... bindings) {
			final var raw = (RawStatement) parse(cql);

			return Restrictions.translate(people, raw.whereClause, context.getCodecRegistry(), node,
				bindings);
		}

		@Test
		@DisplayName("an equality restriction carries the column, the operator and one value")
		void equality() {
			final var restrictions = restrictionsOf("SELECT * FROM ks.people WHERE id = 1");

			assertEquals(List.of(new Restriction(List.of(column(people, "id")), CqlOperator.EQ,
				List.of(List.of(1)))), restrictions);
		}

		@Test
		@DisplayName("an IN restriction carries every value in the order it was written")
		void in() {
			final var restrictions = restrictionsOf("SELECT * FROM ks.people WHERE id IN (2, 1)");

			assertEquals(List.of(new Restriction(List.of(column(people, "id")), CqlOperator.IN,
				List.of(List.of(2), List.of(1)))), restrictions);
		}

		@Test
		@DisplayName("a multi-column relation carries every column and one value each")
		void multiColumn() {
			final var raw = (RawStatement) parse(
				"SELECT * FROM ks.events WHERE pk = 1 AND (ck, note) > (2, 'x')");
			final var restrictions = Restrictions.translate(events, raw.whereClause,
				context.getCodecRegistry(), node);

			assertEquals(new Restriction(List.of(column(events, "ck"), column(events, "note")),
				CqlOperator.GT, List.of(List.of(2, "x"))), restrictions.get(1));
		}

		@Test
		@DisplayName("a bind marker takes its value from the bound values")
		void marker() {
			final var restrictions = restrictionsOf("SELECT * FROM ks.people WHERE id = ?", 7);

			assertEquals(List.of(List.of(7)), restrictions.get(0).values());
		}

		@Test
		@DisplayName("a restriction on an undefined column throws InvalidQueryException")
		void undefinedColumn() {
			final var error = assertThrows(InvalidQueryException.class,
				() -> restrictionsOf("SELECT * FROM ks.people WHERE nope = 1"));

			assertEquals("Undefined column name nope", error.getMessage());
		}

		@Test
		@DisplayName("a statement with no WHERE clause restricts nothing")
		void noWhereClause() {
			assertEquals(List.of(), restrictionsOf("SELECT * FROM ks.people"));
		}

		@Test
		@DisplayName("an operator no statement accepts is translated, then rejected as a predicate")
		void unsupportedOperator() {
			final var restrictions = restrictionsOf(
				"SELECT * FROM ks.people WHERE name LIKE 'A%' ALLOW FILTERING");

			assertEquals(CqlOperator.LIKE, restrictions.get(0).operator());
			final var error = assertThrows(UnsupportedOperationException.class,
				() -> restrictions.get(0).toPredicate());
			assertEquals("Unsupported operator LIKE in WHERE", error.getMessage());
		}

		@Test
		@DisplayName("an equality predicate matches only the rows holding that value")
		void equalityPredicate() {
			addPerson(1, "Ann");
			addPerson(2, "Bob");
			final var predicate = restrictionsOf("SELECT * FROM ks.people WHERE id = 1").get(0)
				.toPredicate();

			assertEquals(List.of("Ann"), names(predicate));
		}

		@Test
		@DisplayName("an IN predicate matches every row holding one of the values")
		void inPredicate() {
			addPerson(1, "Ann");
			addPerson(2, "Bob");
			addPerson(3, "Cid");
			final var predicate = restrictionsOf("SELECT * FROM ks.people WHERE id IN (1, 3)").get(0)
				.toPredicate();

			assertEquals(List.of("Ann", "Cid"), names(predicate));
		}

		static Stream<Arguments> ranges() {
			return Stream.of(arguments("age > 30", List.of("Cid")),
				arguments("age >= 30", List.of("Bob", "Cid")),
				arguments("age < 30", List.of("Ann")),
				arguments("age <= 30", List.of("Ann", "Bob")));
		}

		@ParameterizedTest(name = "{0} matches {1}")
		@MethodSource("ranges")
		@DisplayName("A range predicate matches the rows inside the bound it names")
		void rangePredicate(final String relation, final List<String> expected) {
			addPerson(1, "Ann", 20);
			addPerson(2, "Bob", 30);
			addPerson(3, "Cid", 40);
			final var predicate = restrictionsOf(
				"SELECT * FROM ks.people WHERE %s ALLOW FILTERING".formatted(relation)).get(0)
				.toPredicate();

			assertEquals(expected, names(predicate));
		}

		@Test
		@DisplayName("a range predicate never matches a row whose column is null")
		void rangePredicateSkipsNulls() {
			people.addRow(Arrays.asList(1, "Ann", null, null));
			final var predicate = restrictionsOf(
				"SELECT * FROM ks.people WHERE age > 0 ALLOW FILTERING").get(0).toPredicate();

			assertEquals(List.of(), names(predicate));
		}

		@Test
		@DisplayName("a multi-column range predicate compares the columns lexicographically")
		void multiColumnRangePredicate() {
			events.addRow(Arrays.asList(1, 1, "a"));
			events.addRow(Arrays.asList(1, 2, "b"));
			events.addRow(Arrays.asList(2, 1, "c"));
			final var raw = (RawStatement) parse(
				"SELECT * FROM ks.events WHERE (pk, ck) > (1, 1)");
			final var predicate = Restrictions.translate(events, raw.whereClause,
				context.getCodecRegistry(), node).get(0).toPredicate();

			assertEquals(List.of("b", "c"),
				events.rows().filter(predicate).map(row -> row.getString(2)).toList());
		}

		@Test
		@DisplayName("CONTAINS matches a collection holding the value, CONTAINS KEY a map's key")
		void containsPredicate() {
			people.addRow(Arrays.asList(1, "Ann", 30, List.of("x", "y")));
			people.addRow(Arrays.asList(2, "Bob", 30, List.of("z")));
			final var predicate = restrictionsOf(
				"SELECT * FROM ks.people WHERE tags CONTAINS 'x' ALLOW FILTERING").get(0)
				.toPredicate();

			assertEquals(List.of("Ann"), names(predicate));
		}

		private void addPerson(final int id, final String name) {
			addPerson(id, name, 30);
		}

		private void addPerson(final int id, final String name, final Integer age) {
			// The tags column is left unset, which List.of cannot hold.
			people.addRow(Arrays.asList(id, name, age, null));
		}

		private List<String> names(final Predicate<SeaStarRow> predicate) {
			return people.rows()
				.filter(predicate)
				.map(row -> row.getString(1))
				.toList();
		}

	}

	@Nested
	@DisplayName("Modifications")
	class ModificationsTest {

		@Test
		@DisplayName("INSERT translates its columns to assignments and carries IF NOT EXISTS")
		void insert() {
			final var raw = (ParsedInsert) parse(
				"INSERT INTO ks.people (id, name) VALUES (1, 'Ann') IF NOT EXISTS");

			final var insert = Modifications.insert(context, NO_SESSION_KEYSPACE, raw, node);

			assertEquals(List.of(new Assignment(0, id("id"), 1), new Assignment(1, id("name"), "Ann")),
				insert.assignments());
			assertEquals(List.of(), insert.restrictions());
			assertTrue(insert.ifNotExists());
			assertFalse(insert.ifExists());
		}

		@Test
		@DisplayName("INSERT naming an undefined column throws InvalidQueryException")
		void insertUndefinedColumn() {
			final var raw = (ParsedInsert) parse(
				"INSERT INTO ks.people (id, nope) VALUES (1, 'Ann')");

			final var error = assertThrows(InvalidQueryException.class,
				() -> Modifications.insert(context, NO_SESSION_KEYSPACE, raw, node));

			assertEquals("Undefined column name nope", error.getMessage());
		}

		@Test
		@DisplayName("UPDATE separates its SET items, its WHERE clause and its IF conditions")
		void update() {
			final var raw = (ParsedUpdate) parse(
				"UPDATE ks.people SET name = 'Ann' WHERE id = 1 IF age = 30");

			final var update = Modifications.update(context, NO_SESSION_KEYSPACE, raw, node);

			assertEquals(List.of(new Assignment(1, id("name"), "Ann")), update.assignments());
			assertEquals(List.of(new Restriction(List.of(column(people, "id")), CqlOperator.EQ,
				List.of(List.of(1)))), update.restrictions());
			assertEquals(List.of(new Condition(2, id("age"), CqlOperator.EQ, 30)),
				update.conditions());
		}

		@Test
		@DisplayName("UPDATE IF EXISTS is carried as a flag rather than a condition")
		void updateIfExists() {
			final var raw = (ParsedUpdate) parse(
				"UPDATE ks.people SET name = 'Ann' WHERE id = 1 IF EXISTS");

			final var update = Modifications.update(context, NO_SESSION_KEYSPACE, raw, node);

			assertTrue(update.ifExists());
			assertEquals(List.of(), update.conditions());
		}

		@Test
		@DisplayName("an UPDATE that does not simply set a column is rejected")
		void updateUnsupportedAssignment() {
			final var raw = (ParsedUpdate) parse(
				"UPDATE ks.people SET tags = tags + ['x'] WHERE id = 1");

			assertThrows(UnsupportedOperationException.class,
				() -> Modifications.update(context, NO_SESSION_KEYSPACE, raw, node));
		}

		@Test
		@DisplayName("DELETE of named columns translates to assignments of null")
		void deleteColumns() {
			final var raw = (DeleteStatement.Parsed) parse(
				"DELETE name FROM ks.people WHERE id = 1");

			final var delete = Modifications.delete(context, NO_SESSION_KEYSPACE, raw, node);

			assertEquals(List.of(new Assignment(1, id("name"), null)), delete.assignments());
		}

		@Test
		@DisplayName("DELETE of whole rows assigns nothing")
		void deleteRows() {
			final var raw = (DeleteStatement.Parsed) parse("DELETE FROM ks.people WHERE id = 1");

			final var delete = Modifications.delete(context, NO_SESSION_KEYSPACE, raw, node);

			assertEquals(List.of(), delete.assignments());
			assertEquals(List.of(new Restriction(List.of(column(people, "id")), CqlOperator.EQ,
				List.of(List.of(1)))), delete.restrictions());
		}

		@Test
		@DisplayName("an IF condition on a primary key column throws InvalidQueryException")
		void conditionOnPrimaryKey() {
			final var raw = (ParsedUpdate) parse(
				"UPDATE ks.people SET name = 'Ann' WHERE id = 1 IF id = 2");

			final var error = assertThrows(InvalidQueryException.class,
				() -> Modifications.update(context, NO_SESSION_KEYSPACE, raw, node));

			assertEquals("PRIMARY KEY column 'id' cannot have IF conditions", error.getMessage());
		}

	}

	@Nested
	@DisplayName("Terms")
	class TermsTest {

		/**
		 * A term needs no statement of its own, so every case is written as the second INSERT value
		 * and then resolved against whichever type the case is about. That keeps the table of cases
		 * to (literal, type, value) rather than a table of schemas.
		 */
		private Object resolve(final String literal, final DataType dataType,
			final Object... bindings) {
			final var raw = (ParsedInsert) parse(
				"INSERT INTO ks.people (id, name) VALUES (1, %s)".formatted(literal));
			final var term = FieldBindings.INSERT_COLUMN_VALUES.require(raw).get(1);

			return Terms.resolve(term, dataType, context.getCodecRegistry(), node, bindings);
		}

		static Stream<Arguments> valid() {
			return Stream.of(arguments("a null literal", "null", DataTypes.TEXT, null),
				arguments("an integer literal", "1", DataTypes.INT, 1),
				arguments("a string literal", "'x'", DataTypes.TEXT, "x"),
				arguments("a list literal", "[1, 2]", DataTypes.listOf(DataTypes.INT), List.of(1, 2)),
				arguments("a set literal", "{2, 1}", DataTypes.setOf(DataTypes.INT), Set.of(1, 2)),
				arguments("a map literal", "{'a': 1}", DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT),
					Map.of("a", 1)),
				arguments("a nested collection literal", "{'a': [1]}",
					DataTypes.mapOf(DataTypes.TEXT, DataTypes.listOf(DataTypes.INT, true), false),
					Map.of("a", List.of(1))),
				arguments("an empty list literal", "[]", DataTypes.listOf(DataTypes.INT), null),
				arguments("an empty frozen list literal", "[]", DataTypes.listOf(DataTypes.INT, true),
					List.of()),
				arguments("an empty brace literal for a set", "{}", DataTypes.setOf(DataTypes.INT), null),
				arguments("an empty brace literal for a frozen set", "{}",
					DataTypes.setOf(DataTypes.INT, true), Set.of()),
				arguments("an empty brace literal for a map", "{}",
					DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT), null),
				arguments("an empty brace literal for a frozen map", "{}",
					DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT, true), Map.of()),
				arguments("a cast to the column's own type", "(int) 3", DataTypes.INT, 3),
				arguments("a null literal for a collection", "null", DataTypes.listOf(DataTypes.INT),
					null));
		}

		@ParameterizedTest(name = "{0} resolves to the value it names")
		@MethodSource("valid")
		@DisplayName("A term resolves to the Java value its type gives it")
		void valid(final String description, final String literal, final DataType dataType,
			final Object expected) {
			assertEquals(expected, resolve(literal, dataType));
		}

		static Stream<Arguments> invalid() {
			return Stream.of(
				arguments("an empty brace literal for a list", "{}", DataTypes.listOf(DataTypes.INT),
					"Invalid set literal for a column of type list<int>"),
				arguments("a list literal for a set", "[1]", DataTypes.setOf(DataTypes.INT),
					"Unexpected receiver type 'set<int>'; only list and vector are expected"),
				arguments("a map literal for a set", "{'a': 1}", DataTypes.setOf(DataTypes.INT),
					"Invalid map literal for a column of type set<int>"),
				arguments("a cast to another type", "(bigint) 3", DataTypes.INT,
					"Cannot assign value (bigint)3 to a column of type int"),
				arguments("an unknown function", "wat()", DataTypes.TEXT, "Unknown function wat called"),
				arguments("a function whose result the column cannot hold", "now()", DataTypes.TEXT,
					"Type error: cannot assign result of function now to a column of type text"),
				arguments("a vector literal of the wrong length", "[1.5]",
					DataTypes.vectorOf(DataTypes.FLOAT, 2),
					"Invalid vector literal: type vector<float, 2> expects 2 elements but got 1"),
				arguments("a tuple literal with too many components", "(1, 'a', 2)",
					DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT),
					"Invalid tuple literal: too many elements. Type frozen<tuple<int, text>> expects 2 but got 3"));
		}

		@ParameterizedTest(name = "{0} is rejected")
		@MethodSource("invalid")
		@DisplayName("A term the column's type cannot take throws InvalidQueryException")
		void invalid(final String description, final String literal, final DataType dataType,
			final String message) {
			final var error = assertThrows(InvalidQueryException.class,
				() -> resolve(literal, dataType));

			assertEquals(message, error.getMessage());
		}

		@Test
		@DisplayName("a bind marker resolves to the value bound at its index")
		void marker() {
			assertEquals("Ann", resolve("?", DataTypes.TEXT, "Ann"));
		}

		@Test
		@DisplayName("a tuple literal fills the components it names and leaves the rest null")
		void tuple() {
			final var type = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT);

			assertEquals(type.newValue(6, "x"), resolve("(6, 'x')", type));
			assertEquals(type.newValue(6, null), resolve("(6)", type));
		}

		@Test
		@DisplayName("a list literal for a vector column resolves to a vector")
		void vector() {
			assertEquals(CqlVector.newInstance(1.5f, 2.5f),
				resolve("[1.5, 2.5]", DataTypes.vectorOf(DataTypes.FLOAT, 2)));
		}

		@Test
		@DisplayName("now(), uuid() and currentTimestamp() are evaluated when the term is resolved")
		void functions() {
			assertEquals(1, ((UUID) resolve("now()", DataTypes.TIMEUUID)).version());
			assertEquals(4, ((UUID) resolve("uuid()", DataTypes.UUID)).version());
			assertInstanceOf(Instant.class, resolve("currentTimestamp()", DataTypes.TIMESTAMP));
		}

		@Test
		@DisplayName("a function called with arguments SeaStar cannot pass is rejected")
		void functionArguments() {
			final var error = assertThrows(InvalidQueryException.class,
				() -> resolve("now(1)", DataTypes.TIMEUUID));

			assertEquals("Invalid number of arguments in call to function now: 0 required but found 1",
				error.getMessage());
		}

	}

	@Nested
	@DisplayName("Queries")
	class QueriesTest {

		private Query translate(final String cql, final Object... bindings) {
			return Queries.translate(context, NO_SESSION_KEYSPACE, (RawStatement) parse(cql), node,
				bindings);
		}

		@Test
		@DisplayName("a select list translates to the positions of the selected columns")
		void projection() {
			assertEquals(List.of(2, 1), translate("SELECT age, name FROM ks.people").projection());
		}

		@Test
		@DisplayName("SELECT * projects nothing, leaving the table's own columns")
		void selectAll() {
			assertEquals(List.of(), translate("SELECT * FROM ks.people").projection());
		}

		@Test
		@DisplayName("selecting an undefined column throws InvalidQueryException")
		void undefinedColumn() {
			final var error = assertThrows(InvalidQueryException.class,
				() -> translate("SELECT nope FROM ks.people"));

			assertEquals("Undefined column name nope", error.getMessage());
		}

		@Test
		@DisplayName("DISTINCT and ALLOW FILTERING are carried as written")
		void parameters() {
			final var query = translate(
				"SELECT DISTINCT id FROM ks.people WHERE name = 'Ann' ALLOW FILTERING");

			assertTrue(query.distinct());
			assertTrue(query.allowFiltering());

			final var plain = translate("SELECT id FROM ks.people");
			assertFalse(plain.distinct());
			assertFalse(plain.allowFiltering());
		}

		@Test
		@DisplayName("a LIMIT is resolved to a number, and absent when unwritten")
		void limit() {
			assertEquals(3, translate("SELECT * FROM ks.people LIMIT 3").limit());
			assertEquals(5, translate("SELECT * FROM ks.people LIMIT ?", 5).limit());
			assertNull(translate("SELECT * FROM ks.people").limit());
		}

		@Test
		@DisplayName("a LIMIT of zero or less throws InvalidQueryException")
		void nonPositiveLimit() {
			final var error = assertThrows(InvalidQueryException.class,
				() -> translate("SELECT * FROM ks.people LIMIT 0"));

			assertEquals("LIMIT must be strictly positive", error.getMessage());
		}

	}

}
