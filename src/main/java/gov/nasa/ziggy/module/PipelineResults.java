package gov.nasa.ziggy.module;

import gov.nasa.ziggy.crud.HasOriginator;
import gov.nasa.ziggy.module.io.Persistable;

/**
 * Superclass for all pipeline results. Pipeline results files are files that are stored in the
 * datastore. They need to implement Persistable (so that the HDF5 reader and writer can work with
 * them), and they need to have an originator (to support data accountability).
 *
 * @author PT
 */
public abstract class PipelineResults implements Persistable, HasOriginator {

    // Note: it is not necessary to manually set the originator in a PipelineResults
    // subclass instance as long as the instance is serialized by the saveResultsToTaskDir()
    // method in PipelineOutputs; that method automatically sets the originator before
    // serializing.
    private long originator;

    @Override
    public long getOriginator() {
        return originator;
    }

    @Override
    public void setOriginator(long originator) {
        this.originator = originator;
    }
}
