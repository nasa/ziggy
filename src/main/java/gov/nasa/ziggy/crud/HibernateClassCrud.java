package gov.nasa.ziggy.crud;

/**
 * @author Miles Cote
 */
public interface HibernateClassCrud<T> extends AbstractCrudInterface {

    String getHibernateClassName();

}
