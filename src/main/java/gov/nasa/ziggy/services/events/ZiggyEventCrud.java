package gov.nasa.ziggy.services.events;

import java.util.List;

import org.hibernate.Criteria;

import gov.nasa.ziggy.crud.AbstractCrud;

/**
 * CRUD (Create, Retrieve, Update, Delete) class for {@link ZiggyEventHandler} and
 * {@link ZiggyEvent} classes.
 *
 * @author PT
 */
public class ZiggyEventCrud extends AbstractCrud {

    /**
     * Returns all events. Used by the GUI event display.
     */
    public List<ZiggyEvent> retrieveAllEvents() {
        Criteria criteria = createCriteria(ZiggyEvent.class);
        return list(criteria);
    }

    /**
     * Returns all event handlers. Used by the worker during initialization.
     */
    public List<ZiggyEventHandler> retrieveAllEventHandlers() {
        Criteria criteria = createCriteria(ZiggyEventHandler.class);
        return list(criteria);
    }
}
