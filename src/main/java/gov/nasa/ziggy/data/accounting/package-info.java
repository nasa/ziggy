/**
 * Provides classes and interfaces for reporting on data accountability. Data accountability is the
 * traceability between inputs and outputs, such that for any given output it is possible to work
 * backwards and find all data, models, and parameters that were inputs to the production of those
 * outputs. Ziggy implements this by maintaining a database table of all data files in the
 * datastore. Each file in the table has the ID of its producer (pipeline task that produced it)
 * recorded, along with the IDs of all consumers of that file (pipeline tasks that used that file as
 * an input).
 * </p>
 * <p>
 * Data accountability for models uses a different mechanism, see the ModelRegistry class in
 * gov.nasa.ziggy.models. Similarly, parameter set accountability uses a system implemented in
 * gov.nasa.ziggy.parameters.
 * </p>
 *
 * @author Bill Wohler
 * @author PT
 */

package gov.nasa.ziggy.data.accounting;
