package gov.nasa.ziggy.crud;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.hibernate.query.criteria.HibernateCriteriaBuilder;

import com.google.common.collect.Lists;

import gov.nasa.ziggy.module.PipelineException;
import jakarta.persistence.criteria.AbstractQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.criteria.Selection;
import jakarta.persistence.criteria.Subquery;
import jakarta.persistence.metamodel.ListAttribute;
import jakarta.persistence.metamodel.SetAttribute;
import jakarta.persistence.metamodel.SingularAttribute;

/**
 * Abstraction of the JPA {@link CriteriaQuery}, the JPA {@link Root}, and the Hibernate
 * {@link HibernateCriteriaBuilder} that allows construction and manipulation of multiple related
 * query artifacts in a single class.
 * <p>
 * The JPA Criteria API is extremely verbose and typically requires 3 separate objects to construct
 * and perform queries: the {@link CriteriaQuery} or {@link Subquery}, which is defined in terms of
 * the class that the query returns; the {@link Root}, which is defined in terms of the database
 * table that is the target of the query; and the {@link CriteriaBuilder}, which provides the
 * options that configure the query actions (sorting, filtering, projecting, etc.). Most of Ziggy's
 * query requirements can be satisfied using a small fraction of the JPA system's capabilities. Thus
 * the JPA API is abstracted into the {@link ZiggyQuery}, which automatically constructs those
 * queries and the necessary objects and hides all the underlying verbosity and complexity from the
 * user, who generally could care less.
 * <p>
 * The JPA API requires that queries include a component with a type parameter that corresponds to
 * the class of object returned by the query and a component that is parameterized based on the
 * class that is the target of the query. For this reason, the {@link AbstractCrud} class has two
 * methods that return instances of {@link ZiggyQuery}. The simpler method returns an object for
 * which the query return class and query target class are the same, hence this method takes one
 * class argument; the other returns an object for which the query return class and query target
 * class are different, hence this method takes two arguments. The latter method returns a
 * {@link ZiggyQuery} that is suitable for projection / selection (i.e., returning individual fields
 * of the object instead of the entire object).
 * <p>
 * The {@link ZiggyQuery} uses a fluent ("builder") pattern to the extent possible. As such, most
 * methods return the {@link ZiggyQuery} object, which makes it easier to chain together elements of
 * the query in a single line.
 *
 * @author PT
 * @author Bill Wohler
 */

public class ZiggyQuery<T, R> {

    private HibernateCriteriaBuilder builder;

    private Root<T> root;

    private SingularAttribute<T, ?> attribute;
    private SetAttribute<T, ?> set;
    private ListAttribute<T, ?> list;
    private String columnName;

    private List<Predicate> predicates = new ArrayList<>();
    private List<Selection<?>> selections = new ArrayList<>();

    private List<ZiggyQuery<?, ?>> subqueries = new ArrayList<>();

    // AbstractQuery allows this to be either CriteriaQuery or Subquery, as needed.
    private AbstractQuery<R> jpaQuery;

    /** For testing only. */
    private List<List<Object>> queryChunks = new ArrayList<>();

    /** Constructor for {@link CriteriaQuery} instances. */
    ZiggyQuery(Class<T> databaseClass, Class<R> returnClass, AbstractCrud<?> crud) {
        builder = crud.createCriteriaBuilder();
        jpaQuery = builder.createQuery(returnClass);
        root = jpaQuery.from(databaseClass);
    }

    /** Constructor for {@link Subquery} classes. */
    ZiggyQuery(Class<T> databaseClass, Class<R> returnClass, HibernateCriteriaBuilder builder,
        AbstractQuery<?> parentQuery) {
        this.builder = builder;
        jpaQuery = parentQuery.subquery(returnClass);
        root = jpaQuery.from(databaseClass);
    }

    /**
     * Defines a column for later use in a query operation. The column is either a scalar or
     * collection.
     * <p>
     * N.B. This call is not type-safe. Use {@link #column(SingularAttribute)} or
     * {@link #column(SetAttribute)} if at all possible.
     */
    public ZiggyQuery<T, R> column(String columnName) {
        attribute = null;
        set = null;
        this.columnName = columnName;
        return this;
    }

    /**
     * Use when a method can take either a scalar or collection attribute.
     */
    private boolean hasAttribute() {
        return hasScalarAttribute() || set != null || list != null;
    }

    /**
     * Use when a method can take either a scalar or collection attribute.
     */
    private boolean hasScalarAttribute() {
        return attribute != null || columnName != null;
    }

    /**
     * Defines a column for later use in a query operation. The column is a scalar.
     * <p>
     * Queries typically need to restrict the query results based on column values, return column
     * values rather than the entire object that's queried, etc. This is accomplished using this
     * method, which accepts the column to be used in all subsequent operations.
     */
    public <Y> ZiggyQuery<T, R> column(SingularAttribute<T, Y> attribute) {
        this.attribute = attribute;
        set = null;
        columnName = null;
        return this;
    }

    /**
     * Defines a column for later use in a query operation. The column is a set.
     * <p>
     * Queries typically need to restrict the query results based on column values, return column
     * values rather than the entire object that's queried, etc. This is accomplished using this
     * method, which accepts the column to be used in all subsequent operations.
     */
    public <Y> ZiggyQuery<T, R> column(SetAttribute<T, Y> set) {
        attribute = null;
        this.set = set;
        columnName = null;
        return this;
    }

    public <Y> ZiggyQuery<T, R> column(ListAttribute<T, Y> list) {
        attribute = null;
        set = null;
        columnName = null;
        this.list = list;
        return this;
    }

    /**
     * Looks for a given value in a column that is a set or a list.
     * <p>
     * {@link #contains(Object)} allows the user to retrieve objects for which a list or set field
     * contains a specified value. For example, imagine that class Foo has a List<Bar> field, bars.
     * The user wants to find the instances of Foo for which one of the values of the bars field is
     * baz. This can be accomplished thusly:
     *
     * <pre>
     * ZiggyQuery&lt;Foo, Foo&gt; query = createQuery(Foo.class);
     * query.column(Foo_.bars).contains(baz);
     * return list(query);
     * </pre>
     * <p>
     * Note that, at present, this method cannot take a {@link ZiggyQuery} (or more specifically a
     * subquery) as argument.
     */
    public <Y> ZiggyQuery<T, R> contains(Y value) {

        // TODO extend to allow value to be a subquery.
        if (value instanceof ZiggyQuery) {
            throw new IllegalArgumentException("Argument to contains cannot be a ZiggyQuery");
        }
        Join<T, ?> joinInstance = join();
        predicates.add(getBuilder().in(joinInstance, value));
        return this;
    }

    /**
     * Returns the collection of objects that contain, in a list or set field, any of the values
     * provided in the argument.
     */
    public <Y> ZiggyQuery<T, R> containsAny(Collection<Y> values) {

        // If the values argument is empty or null, we need the query to return null/empty.
        // To do this, we need to create a predicate that is always false. So:
        if (CollectionUtils.isEmpty(values)) {
            predicates.add(getBuilder().or());
            return this;
        }
        // TODO extend to allow value to be a subquery.
        Join<T, ?> joinInstance = join();
        Predicate predicate = null;
        for (Y value : values) {
            Predicate newPredicate = getBuilder().in(joinInstance, value);
            predicate = predicate == null ? newPredicate : getBuilder().or(predicate, newPredicate);
        }
        predicates.add(predicate);
        return this;
    }

    /**
     * Applies a query constraint that the value of a column must contain the specified value.
     */
    @SuppressWarnings("unchecked")
    public <Y> ZiggyQuery<T, R> in(Y value) {
        checkState(hasScalarAttribute(), "a column has not been defined");
        if (value instanceof ZiggyQuery) {
            Subquery<?> subquery = (Subquery<?>) ((ZiggyQuery<?, ?>) value).jpaQuery;
            predicates.add(attribute != null
                ? builder.in((Path<? extends Y>) root.get(attribute), Set.of(subquery))
                : builder.in(root.get(columnName), Set.of(subquery)));
            return this;
        }
        predicates.add(
            attribute != null ? builder.in((Path<? extends Y>) root.get(attribute), Set.of(value))
                : builder.in(root.get(columnName), Set.of(value)));
        return this;
    }

    /**
     * Applies a query constraint that the value of a column must be one of a collection of
     * specified values.
     */
    @SuppressWarnings("unchecked")
    public <Y> ZiggyQuery<T, R> in(Collection<Y> values) {
        checkState(hasScalarAttribute(), "a column has not been defined");
        predicates
            .add(attribute != null ? builder.in((Path<? extends Y>) root.get(attribute), values)
                : builder.in(root.get(columnName), values));
        return this;
    }

    /**
     * Performs the action of the {@link #in(Collection)} method, but performs the query in chunks.
     * This allows queries in which the collection of values is too large for a single query. The
     * number of values in a chunk is set by the {@link AbstractCrud#MAX_EXPRESSIONS} constant.
     */
    @SuppressWarnings("unchecked")
    public <Y> ZiggyQuery<T, R> chunkedIn(Collection<Y> values) {
        checkState(hasScalarAttribute(), "a column has not been defined");
        List<Y> valuesList = new ArrayList<>(values);
        Predicate criterion = builder.disjunction();
        for (List<Y> valuesSubset : Lists.partition(valuesList, maxExpressions())) {
            queryChunks.add((List<Object>) valuesSubset);
            criterion = builder.or(criterion,
                attribute != null
                    ? builder.in((Path<? extends Y>) root.get(attribute), valuesSubset)
                    : builder.in(root.get(columnName), valuesSubset));
        }
        predicates.add(criterion);
        return this;
    }

    public <Y> CriteriaBuilder.In<Y> in(Expression<? extends Y> expression, Collection<Y> values) {
        return builder.in(expression, values);
    }

    public <Y> CriteriaBuilder.In<Y> in(Expression<? extends Y> expression, Y value) {
        return builder.in(expression, Set.of(value));
    }

    /**
     * Applies a query constraint that the value of a column must be between a specified minimum
     * value and a specified maximum value, inclusive.
     */
    @SuppressWarnings("unchecked")
    public <Y extends Comparable<? super Y>> ZiggyQuery<T, R> between(Y minValue, Y maxValue) {
        checkState(hasScalarAttribute(), "a column has not been defined");
        predicates.add(attribute != null
            ? builder.between((Expression<? extends Y>) root.get(attribute), minValue, maxValue)
            : builder.between(root.get(columnName), minValue, maxValue));
        return this;
    }

    /**
     * Selects a column of the query target class for projection.
     */
    public ZiggyQuery<T, R> select() {
        checkState(hasAttribute(), "a column has not been defined");
        if (attribute != null) {
            select(attribute);
        } else if (set != null) {
            select(set);
        } else if (list != null) {
            select(list);
        } else {
            select(columnName);
        }
        return this;
    }

    /**
     * Performs projection on a specified field of the query target class.
     */
    public <Y> ZiggyQuery<T, R> select(SingularAttribute<T, Y> attribute) {
        selections.add(root.get(attribute));
        return this;
    }

    /**
     * Performs projection on a specified field of the query target class.
     */
    public <Y> ZiggyQuery<T, R> select(SetAttribute<T, Y> set) {
        selections.add(root.get(set));
        return this;
    }

    /**
     * Performs projection on a specified field of the query target class.
     */
    public <Y> ZiggyQuery<T, R> select(ListAttribute<T, Y> set) {
        selections.add(root.get(set));
        return this;
    }

    /**
     * Performs projection on a specified field of the query target class.
     * <p>
     * The {@link #select(Selection)} method supports queries that return values that are fields of
     * fields. Example: assume you have a class, Foo, that has a field, bar, of class Bar; Bar, in
     * turn, has a field, baz, of class Baz. You can select the baz field in a given instance of Foo
     * as follows:
     *
     * <pre>
     * ZiggyQuery&lt;Foo, Baz&gt; query = createZiggyQuery(Foo.class, Baz.class);
     * query.select(query.getRoot().get(Foo_.bar).get(Bar_.baz));
     * </pre>
     */
    public ZiggyQuery<T, R> select(Selection<? extends R> selection) {
        if (selection instanceof ZiggyQuery) {
            selections.add((Selection<?>) ((ZiggyQuery<?, ?>) selection).jpaQuery);
        }
        selections.add(selection);
        return this;
    }

    private void select(String columnName) {
        selections.add(root.get(columnName));
    }

    @SuppressWarnings("unchecked")
    public <Y extends Comparable<? super Y>> ZiggyQuery<T, R> min() {
        checkState(hasScalarAttribute(), "a column has not been defined");
        selections.add(attribute != null ? builder.least((Expression<Y>) root.get(attribute))
            : builder.least(root.<Y> get(columnName)));
        return this;
    }

    @SuppressWarnings("unchecked")
    public <Y extends Comparable<? super Y>> ZiggyQuery<T, R> max() {
        checkState(hasScalarAttribute(), "a column has not been defined");
        selections.add(attribute != null ? builder.greatest((Expression<Y>) root.get(attribute))
            : builder.max(root.get(columnName)));
        return this;
    }

    /**
     * Applies a query constraint that projects the minimum and maximum value of a column.
     * <p>
     * In order to use the {@link #minMax()} constraint, the user must specify a {@link ZiggyQuery}
     * that returns Object[]. The minimum value will be the first element of the array, the maximum
     * value will be the second value.
     */
    public ZiggyQuery<T, R> minMax() {
        min();
        max();
        return this;
    }

    /**
     * Selects the sum of a specified column.
     */
    @SuppressWarnings("unchecked")
    public <N extends Number> ZiggyQuery<T, R> sum() {
        checkState(hasScalarAttribute(), "a column has not been defined");
        selections.add(attribute != null ? builder.sum((Expression<N>) root.get(attribute))
            : builder.sum(root.get(columnName)));
        return this;
    }

    /**
     * Selects a count of results.
     */
    public ZiggyQuery<T, R> count() {
        selections.add(builder.count(root));
        return this;
    }

    /**
     * Allows the user to specify an arbitrary {@link Predicate}. This allows the predicate to be
     * stored along with the ones generated internally by the {@link ZiggyQuery}, so that the
     * user-supplied predicate can be combined with internally generated ones prior to evaluating
     * the query.
     */
    public ZiggyQuery<T, R> where(Predicate predicate) {
        predicates.add(predicate);
        return this;
    }

    /**
     * Combines multiple stored constraints into a {@link CriteriaQuery#where(Predicate...)} clause.
     * <p>
     * Ziggy requires that constraints be specified one at a time via the {@link #in(Object)},
     * {@link #between(Comparable, Comparable)}, etc., methods. Meanwhile, the {@link CriteriaQuery}
     * API accepts only one {@link CriteriaQuery#where(Predicate...)} statement. This mismatch is
     * addressed by capturing all the {@link Predicate} instances created in the query and then
     * using {@link #constructWhereClause()} to apply them to the {@link CriteriaQuery}.
     * <p>
     * The {@link #constructWhereClause()} must be called before the query is executed.
     */
    public ZiggyQuery<T, R> constructWhereClause() {
        for (ZiggyQuery<?, ?> subquery : subqueries) {
            subquery.constructSelectClause().constructWhereClause();
        }
        if (predicates.isEmpty()) {
            return this;
        }
        Predicate[] predicatesArray = predicates.toArray(new Predicate[0]);
        jpaQuery = jpaQuery.where(predicatesArray);
        return this;
    }

    /**
     * Combines multiple stored selections into a {@link CriteriaQuery#multiselect(List)} clause.
     * <p>
     * Ziggy requires that selections be specified one at a time via the {@link #select()},
     * {@link #min()}, etc., methods. Meanwhile, the {@link CriteriaQuery} API accepts only one
     * {@link CriteriaQuery#multiselect(List)} statement. This mismatch is addressed by capturing
     * all the {@link Selection} instances created in the query and then using
     * {@link #constructSelectClause()} to apply them to the {@link CriteriaQuery}.
     * <p>
     * The {@link #constructSelectClause()} must be called before the query is executed.
     */
    public ZiggyQuery<T, R> constructSelectClause() {
        for (ZiggyQuery<?, ?> subquery : subqueries) {
            subquery.constructSelectClause().constructWhereClause();
        }
        if (selections.isEmpty()) {
            return this;
        }

        // Insane as this may sound, the one method that CriteriaQuery has, and Subquery has, but
        // Abstract query DOES NOT have, is select. Also, the Subquery form of where requires an
        // additional cast from Selection to Expression.
        jpaQuery = querySelect(jpaQuery, selections);
        return this;
    }

    private AbstractQuery<R> querySelect(AbstractQuery<R> query, List<Selection<?>> selections) {
        if (query instanceof Subquery) {
            return subquerySelect((Subquery<R>) query, selections);
        }
        return criteriaQuerySelect((CriteriaQuery<R>) query, selections);
    }

    private Subquery<R> subquerySelect(Subquery<R> query, List<Selection<?>> selections) {
        if (selections.size() > 1) {
            throw new PipelineException("Subquery does not support multiselect");
        }
        @SuppressWarnings("unchecked")
        Expression<R> selection = (Expression<R>) selections.get(0);
        return query.select(selection);
    }

    private CriteriaQuery<R> criteriaQuerySelect(CriteriaQuery<R> query,
        List<Selection<?>> selections) {

        // NB: multiselect produces an (undocumented) IllegalStateException
        // if the size of its argument is 1, which is why there needs to be
        // a block for the single Selection case that uses select and one for
        // the multiple Selection case that uses multiselect.
        if (selections.size() == 1) {
            Selection<?> selection = selections.get(0);
            @SuppressWarnings("unchecked")
            Selection<R> selectionR = (Selection<R>) selection;
            return query.select(selectionR);
        }
        return query.multiselect(selections);
    }

    /**
     * Applies a query constraint that specifies whether all values are returned, or whether the
     * returned values are filtered to eliminate duplicates.
     */
    public ZiggyQuery<T, R> distinct(boolean distinct) {
        jpaQuery = jpaQuery.distinct(distinct);
        return this;
    }

    /** Instructs the query to return results in descending order. */
    public ZiggyQuery<T, R> ascendingOrder() {
        if (jpaQuery instanceof Subquery) {
            return this;
        }
        checkState(hasScalarAttribute(), "a column has not been defined");
        ((CriteriaQuery<R>) jpaQuery).orderBy(attribute != null ? builder.asc(root.get(attribute))
            : builder.asc(root.get(columnName)));
        return this;
    }

    /** Instructs the query to return results in descending order. */
    public ZiggyQuery<T, R> descendingOrder() {
        if (jpaQuery instanceof Subquery) {
            return this;
        }
        checkState(hasScalarAttribute(), "a column has not been defined");
        ((CriteriaQuery<R>) jpaQuery).orderBy(attribute != null ? builder.desc(root.get(attribute))
            : builder.desc(root.get(columnName)));
        return this;
    }

    /** Generates a subquery to the current query. */
    public <U> ZiggyQuery<U, U> ziggySubquery(Class<U> subqueryClass) {
        return ziggySubquery(subqueryClass, subqueryClass);
    }

    /** Generates a subquery to the current query. */
    public <U, V> ZiggyQuery<U, V> ziggySubquery(Class<U> databaseClass, Class<V> returnClass) {
        ZiggyQuery<U, V> subquery = new ZiggyQuery<>(databaseClass, returnClass, builder, jpaQuery);
        subqueries.add(subquery);
        return subquery;
    }

    /**
     * Creates a Join between the table and a field selected by the column() method. The selected
     * column must be either a list field or a set field.
     */
    @SuppressWarnings("unchecked")
    public <Y> Join<T, Y> join() {
        if (list == null && set == null) {
            throw new IllegalArgumentException("Column selected for a join must be a set or list");
        }
        if (list != null) {
            return getRoot().join((ListAttribute<T, Y>) list);
        }
        return getRoot().join((SetAttribute<T, Y>) set);
    }

    /**
     * Returns the {@link CriteriaBuilder} instance in the {@link ZiggyQuery}. This allows users to
     * build queries that aren't directly supported by {@link ZiggyQuery} and instead require more
     * direct use of the JPA API.
     */
    public HibernateCriteriaBuilder getBuilder() {
        return builder;
    }

    /**
     * Returns the {@link Root} instance in the {@link ZiggyQuery}. This allows users to build
     * queries that aren't directly supported by {@link ZiggyQuery} and instead require more direct
     * use of the JPA API.
     */
    public Root<T> getRoot() {
        return root;
    }

    public <Y> Path<Y> get(SingularAttribute<? super T, Y> attribute) {
        return root.get(attribute);
    }

    /** Returns the {@link AbstractQuery} cast to {@link CriteriaQuery}. */
    public CriteriaQuery<R> getCriteriaQuery() {
        if (jpaQuery instanceof Subquery) {
            throw new PipelineException("Subquery cannot be cast to CriteriaQuery");
        }
        return (CriteriaQuery<R>) jpaQuery;
    }

    /**
     * Maximum expressions allowed in each chunk of {@link #chunkedIn(Collection)}. Broken out into
     * a package-private method so that tests can reduce this value to something small enough to
     * exercise in test.
     */
    int maxExpressions() {
        return AbstractCrud.MAX_EXPRESSIONS;
    }

    /** For testing only. */
    List<List<Object>> queryChunks() {
        return queryChunks;
    }
}
