package gov.nasa.ziggy.worker;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;

public class WorkerResourcesCrud extends AbstractCrud<WorkerResources> {

    public WorkerResources retrieveDefaultResources() {
        ZiggyQuery<WorkerResources, WorkerResources> query = createZiggyQuery(
            WorkerResources.class);
        query.column(WorkerResources_.defaultInstance).in(true);
        return uniqueResult(query);
    }

    @Override
    public Class<WorkerResources> componentClass() {
        return WorkerResources.class;
    }
}
