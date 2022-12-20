package gov.nasa.ziggy.module.remote;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.exec.CommandLine;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.remote.aws.AwsExecutor;
import gov.nasa.ziggy.module.remote.nas.NasExecutor;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.process.ExternalProcess;

/**
 * Provides the list of supported clusters for remote task execution. This class also provides
 * static members and methods that allow all threads in a process to know which supported cluster
 * flavor is available. Each enum can also provide the class of the corresponding
 * {@link RemoteExecutor} subclass for that cluster.
 *
 * @author PT
 */
public enum SupportedRemoteClusters {
    NAS(NasExecutor.class), AWS(AwsExecutor.class);

    private static SupportedRemoteClusters remoteCluster;

    private static SupportedRemoteClusters defaultCluster = NAS;
    private Class<? extends RemoteExecutor> remoteExecutorClass;

    SupportedRemoteClusters(Class<? extends RemoteExecutor> remoteExecutorClass) {
        this.remoteExecutorClass = remoteExecutorClass;
    }

    public Class<? extends RemoteExecutor> getRemoteExecutorClass() {
        return remoteExecutorClass;
    }

    public static SupportedRemoteClusters remoteCluster() {
        if (remoteCluster == null) {
            setRemoteCluster();
        }
        return remoteCluster;
    }

    private static void setRemoteCluster() {

        // If there's no /etc/os-release file at all, then we're not on a system with access to
        // any cluster, and need to check the properties file for a configuration we should use
        // (generally this means we are emulating a remote cluster for test purposes).
        File osReleaseFile = new File("/etc/os-release");
        if (!osReleaseFile.exists()) {
            Configuration config = ZiggyConfiguration.getInstance();
            String clusterName = config.getString(PropertyNames.CLUSTER_PROPERTY_NAME,
                defaultCluster.name());
            remoteCluster = SupportedRemoteClusters.valueOf(clusterName.toUpperCase());
        } else {

            // See if the /etc/os-release file has the string "Amazon" in it
            ExternalProcess p = ExternalProcess
                .simpleExternalProcess(CommandLine.parse("/usr/bin/grep Amazon /etc/os-release"));
            try {
                p.run(true, 0);
                String grepResult = p.getStdoutString();
                if (grepResult.isEmpty()) {
                    remoteCluster = SupportedRemoteClusters.NAS;
                } else {
                    remoteCluster = SupportedRemoteClusters.AWS;
                }
            } catch (Exception e) {
                throw new PipelineException("Unable to set remote cluster", e);
            }

        }
    }
}
