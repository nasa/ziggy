package gov.nasa.ziggy.pipeline.definition.crud;

import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * @author Miles Cote
 */
public interface HibernateClassCrudTest<T> {
    DatabaseService getDatabaseService();

    T createHibernateObject();
}
