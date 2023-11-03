package gov.nasa.ziggy.module.remote.nas;

import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.RemoteExecutor;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.module.remote.SupportedRemoteClusters;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Manages execution of a pipeline algorithm on the Pleiades cluster.
 *
 * @author PT
 */
public class NasExecutor extends RemoteExecutor {

    public NasExecutor(PipelineTask pipelineTask) {
        super(pipelineTask);
    }

    /**
     * Implements the generatePbsParameters abstract method. For NAS / Pleiades, the architecture
     * must be selected and the resource parameters can then be determined without any further ado.
     */
    @Override
    public PbsParameters generatePbsParameters(RemoteParameters remoteParameters,
        int totalSubtasks) {

        PbsParameters pbsParameters = remoteParameters.pbsParametersInstance();
        pbsParameters.populateArchitecture(remoteParameters, totalSubtasks,
            SupportedRemoteClusters.NAS);

        // Pleiades doesn't actually make use of the cores or gigs per node specifications,
        // but the resource parameter method (below) does, so we need to set them to the
        // values for the architecture
        pbsParameters.setMinCoresPerNode(pbsParameters.getArchitecture().getMaxCores());
        pbsParameters.setMinGigsPerNode(pbsParameters.getArchitecture().getMaxGigs());

        pbsParameters.populateResourceParameters(remoteParameters, totalSubtasks);

        return pbsParameters;
    }

    @Override
    protected void submitForExecution(StateFile stateFile) {
        submitToPbsInternal(stateFile, pipelineTask, algorithmLogDir(), taskDataDir());
    }
}
