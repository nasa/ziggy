package gov.nasa.ziggy.uow;

import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.services.config.PropertyName;

/**
 * Abstract superclass for classes that map {@link PipelineModule} subclasses to
 * {@link UnitOfWorkGenerator} subclasses. This allows different pipeline modules to use different
 * concrete classes to generate their own units of work. When implementing this class, provide a
 * no-argument constructor. Specify the fully-qualified name of your subclass in the property
 * {@link PropertyName#PIPELINE_DEFAULT_UOW_IDENTIFIER_CLASS}.
 * <p>
 * The concrete class that supports this functionality for Ziggy pipeline modules does not yet
 * exist. In the meantime, use the method {@link UnitOfWorkGenerator#ziggyDefaultUowGenerators()}.
 *
 * @author PT
 */
public abstract class DefaultUnitOfWorkIdentifier {

    /**
     * Determines the default {@link UnitOfWorkGenerator} subclass for a given subclass of
     * {@link PipelineModule}.
     */
    public abstract Class<? extends UnitOfWorkGenerator> defaultUnitOfWorkGeneratorForClass(
        Class<? extends PipelineModule> module);
}
