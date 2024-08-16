/**
 * Provides classes and interfaces for working with the Ziggy database. The Ziggy database
 * architecture is in the form of an onion with the following three layers:
 * <ol>
 * <li>The CRUD methods, which use {@link gov.nasa.ziggy.crud.ZiggyQuery}s to access the database
 * with Hibernate, an object–relational mapping tool for the Java programming language.</li>
 * <li>The operations classes, which will be discussed in more detail shortly.</li>
 * <li>All other classes, which can only access the database through the operations classes.</li>
 * <p>
 * The goal of this architecture is to avoid the question, am I in a transaction? This is achieved
 * with the simple rule: The transaction boundary shall be confined to a single operations class.
 * </p>
 * <p>
 * The Ziggy operations classes are all subclasses of
 * {@link gov.nasa.ziggy.services.database.DatabaseOperations} and use
 * {@link gov.nasa.ziggy.services.database.DatabaseOperations#performTransaction(DatabaseTransaction)},
 * which does exactly what it sounds like.
 * </p>
 * <p>
 * Calls to the CRUD methods should only occur within a lambda that is passed to
 * {@code performTransaction()}.
 * </p>
 * <p>
 * And again, all other code should not set up any transactions of their own, rather the code should
 * call an appropriate method in an operations class.
 * </p>
 *
 * @author Bill Wohler
 * @author PT
 */

package gov.nasa.ziggy.services.database;
