package gov.nasa.ziggy.uow;

import gov.nasa.ziggy.pipeline.definition.PipelineModule;

/**
 * Abstract superclass for classes that map {@link PipelineModule} subclasses to
 * {@link UnitOfWorkGenerator} subclasses. This allows different pipeline modules to use different
 * concrete classes to handle their default units of work, while still allowing the
 * {@link DefaultUnitOfWork} to be the specified default for all pipeline modules.
 * <p>
 * The concrete class that supports this functionality for Ziggy pipeline modules is
 * {@link UnitOfWorkGenerator.ZiggyDefaultUnitOfWorkIdentifier}. For additional pipeline modules
 * that are defined for an actual pipeline, the users can specify the fully-qualified name of a
 * class that provides this functionality in those cases, as long as the class is a subclass of
 * {@link DefaultUnitOfWorkIdentifier} and has a no-argument constructor.
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
