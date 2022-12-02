package gov.nasa.ziggy.services;

import gov.nasa.ziggy.module.PipelineException;

/**
 * @author Todd Klaus
 */
public interface AbstractService {
    void initialize() throws PipelineException;

    boolean isInitialized() throws PipelineException;
}
