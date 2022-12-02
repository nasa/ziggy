package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;

/**
 * Filter used by the {@link PipelineInstanceCrud#retrieve(PipelineInstanceFilter)} method.
 *
 * @author Todd Klaus
 */
public class PipelineInstanceFilter {
    private static final String FROM_CLAUSE = "from PipelineInstance pi";
    private static final String ORDER_BY_CLAUSE = "order by pi.id asc";
    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;

    /**
     * Pass if PipelineInstance.name contains the specified String. If empty or null, name is not
     * included in the where clause.
     */
    private String nameContains = "";

    /**
     * Pass if PipelineInstance.state is contained in this Set. If null, state is not included in
     * the where clause. Note that List is used rather than Set in order to make the order
     * deterministic, which simplifies testing
     */
    private List<PipelineInstance.State> states = null;

    /**
     * Pass if PipelineInstance.startProcessingTime is within ageDays days of the time the query is
     * ran. If 0, startProcessingTime is not included in the where clause.
     */
    private int ageDays = 10;

    public PipelineInstanceFilter() {
    }

    public PipelineInstanceFilter(String nameContains, List<State> states, int ageDays) {
        this.nameContains = nameContains;
        this.states = states;
        this.ageDays = ageDays;
    }

    /**
     * Create the Hibernate Query object that implements this filter. Called only by
     * PipelineInstanceCrud.
     *
     * @param session
     * @return
     */
    Query query(Session session) {
        StringBuilder result = new StringBuilder();
        List<Date> startTimestamps = new ArrayList<>();

        if (nameContains != null && nameContains.length() > 0) {
            result.append("pi.name like '%" + nameContains + "%'");
        }

        if (states != null) {
            if (result.length() > 0) {
                result.append(" and ");
            }

            if (states.size() > 0) {
                int statesAdded = 0;
                result.append("pi.state in (");
                for (PipelineInstance.State state : states) {
                    if (statesAdded > 0) {
                        result.append(",");
                    }
                    result.append(state.ordinal());
                    statesAdded++;
                }
                result.append(")");
            } else {
                // empty, no matches
                result.append("pi.state = -1");
            }
        }

        if (ageDays > 0) {
            if (result.length() > 0) {
                result.append(" and ");
            }
            result.append("pi.startProcessingTime >= :startProcessingTime");
            long t = System.currentTimeMillis() - ageDays * MILLIS_PER_DAY;
            Date d = new Date(t);
            startTimestamps.add(d);
        }

        Query q;

        if (result.length() > 0) {
            q = session
                .createQuery(FROM_CLAUSE + " where " + result.toString() + " " + ORDER_BY_CLAUSE);
            for (Date obj : startTimestamps) {
                q.setParameter("startProcessingTime", obj);
            }
        } else {
            // no filters
            q = session.createQuery(FROM_CLAUSE + " " + ORDER_BY_CLAUSE);
        }

        return q;
    }

    public String getNameContains() {
        return nameContains;
    }

    public void setNameContains(String nameContains) {
        this.nameContains = nameContains;
    }

    public List<PipelineInstance.State> getStates() {
        return states;
    }

    public void setStates(List<PipelineInstance.State> states) {
        this.states = states;
    }

    public int getAgeDays() {
        return ageDays;
    }

    public void setAgeDays(int ageDays) {
        this.ageDays = ageDays;
    }
}
