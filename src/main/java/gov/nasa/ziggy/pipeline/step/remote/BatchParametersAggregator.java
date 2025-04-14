package gov.nasa.ziggy.pipeline.step.remote;

import java.util.Collection;

/**
 * Aggregates multiple instances of {@link BatchParameters} to determine resource needs for a
 * collection of pipeline tasks.
 *
 * @author PT
 */
public interface BatchParametersAggregator<T extends BatchParameters> {

    T aggregate(Collection<BatchParameters> parametersInstances);
}
