package gov.nasa.ziggy.parameters;

import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;

/**
 * Interface for all parameter bean classes.
 * <p>
 * This interface is a simple marker used by the console to display all possible classes found on
 * the classpath when configuring a new {@link PipelineDefinition}.
 * <p>
 * Implementing classes should conform to the JavaBeans specification.
 * <p>
 * This class is used to parameterize {@link BeanWrapper} in {@link PipelineDefinition} and
 * {@link PipelineInstance}
 *
 * @author Todd Klaus
 */
public interface Parameters extends Persistable {

    default void validate() {
        // Do nothing, by default.
    }

}
