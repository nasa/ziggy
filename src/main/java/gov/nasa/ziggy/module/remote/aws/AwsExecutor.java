package gov.nasa.ziggy.module.remote.aws;

import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.RemoteExecutor;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.module.remote.SupportedRemoteClusters;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Manages execution of a {@link PipelineTask} on the AWS cluster.
 *
 * @author PT
 */
public class AwsExecutor extends RemoteExecutor {

    protected AwsExecutor(PipelineTask pipelineTask) {
        super(pipelineTask);
    }

    /**
     * Implements the {@link generatePbsParameters} method for this class. In this case, the
     * architecture must first be selected; then the min cores and min gigs per node must be
     * selected (since each architecture supports multiple configurations of cores and RAM, but all
     * these configurations have the same ratio of RAM per core); and finally the resource
     * parameters can be determined.
     */
    @Override
    public PbsParameters generatePbsParameters(RemoteParameters remoteParameters,
        int totalSubtasks) {

        PbsParameters pbsParameters = remoteParameters.pbsParametersInstance();
        pbsParameters.populateArchitecture(remoteParameters, totalSubtasks,
            SupportedRemoteClusters.AWS);

        // We need to make sure that we request a minimum of cores and gigs that is sufficient
        // to run at least 1 subtask per node
        double gigsPerSubtask = remoteParameters.getGigsPerSubtask();
        int minCoresPerNode = (int) Math
            .ceil(gigsPerSubtask / pbsParameters.getArchitecture().getGigsPerCore());
        int minGigsPerNode = (int) Math.ceil(remoteParameters.getGigsPerSubtask());

        // We also need to make sure we don't ask for less than the minimum available for
        // the specified architecture
        minCoresPerNode = Math.max(minCoresPerNode, pbsParameters.getArchitecture().getMinCores());
        minGigsPerNode = (int) Math.max(minGigsPerNode,
            minCoresPerNode * pbsParameters.getArchitecture().getGigsPerCore());

        // Compute the gigs and cores as though the user hasn't set any overrides
        int defaultCoresPerNode = pbsParameters.getArchitecture().getMaxCores() / 3;
        int requestedMinCoresPerNode = Math.max(minCoresPerNode, defaultCoresPerNode);
        int defaultGigsPerNode = pbsParameters.getArchitecture().getMaxGigs() / 3;
        int requestedMinGigsPerNode = Math.max(minGigsPerNode, defaultGigsPerNode);

        // if the user overrode the min gigs per node, make sure they asked for enough
        // to run at least one subtask per node
        if (pbsParameters.getMinGigsPerNode() != 0
            && pbsParameters.getMinGigsPerNode() < gigsPerSubtask) {
            pbsParameters.setMinGigsPerNode((int) Math.ceil(gigsPerSubtask));
        }

        // Set either of the node parameters that are not overridden
        if (pbsParameters.getMinCoresPerNode() == 0) {
            pbsParameters.setMinCoresPerNode(requestedMinCoresPerNode);
        }
        if (pbsParameters.getMinGigsPerNode() == 0) {
            pbsParameters.setMinGigsPerNode(requestedMinGigsPerNode);
        }

        pbsParameters.populateResourceParameters(remoteParameters, totalSubtasks);

        return pbsParameters;
    }

    @Override
    protected void submitForExecution(StateFile stateFile) {
        submitToPbsInternal(stateFile, pipelineTask, algorithmLogDir(), taskDataDir());
    }
}
