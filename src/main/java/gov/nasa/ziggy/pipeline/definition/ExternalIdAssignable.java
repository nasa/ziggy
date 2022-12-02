package gov.nasa.ziggy.pipeline.definition;

/**
 * Defines an interface for classes where the pipeline needs to support both importation and
 * generation of the model/table in question. In the case of importation, the model/table will have
 * an external ID provided by whomever or whatever generated the model/table. In the case of
 * generation, the model/table requires a generated ID that takes into account both the existing IDs
 * in the table and the minimum ID permitted for generated tables.
 *
 * @author PT
 */
public interface ExternalIdAssignable extends HasExternalId {

    /**
     * Method that returns the minimum allowed ID value in cases where an ID must be assigned (i.e.,
     * models/tables that are generated and not imported). In these cases, values will start with
     * the minAllowedAssignedIdNumber and increase monotonically for subsequent generated
     * models/tables. Models and tables may override this to return their own minimum allowed
     * assigned ID values
     *
     * @return the minimum ID number that the assignment process may use.
     */
    int minAllowedAssignedIdNumber();
}
