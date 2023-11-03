package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.parameters.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.parameters.ParameterSetDescriptor;
import gov.nasa.ziggy.parameters.ParametersOperations;
import gov.nasa.ziggy.services.security.Privilege;

/**
 * GUI Proxy class for {@link ParametersOperations}
 *
 * @author Todd Klaus
 */
public class ParametersOperationsProxy {

    public ParametersOperationsProxy() {
    }

    public List<ParameterSetDescriptor> importParameterLibrary(final String sourcePath,
        final List<String> excludeList, final boolean dryRun) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParametersOperations paramsOps = new ParametersOperations();
            return paramsOps.importParameterLibrary(sourcePath, excludeList,
                dryRun ? ParamIoMode.DRYRUN : ParamIoMode.STANDARD);
        });
    }

    public void exportParameterLibrary(final String destinationPath, final List<String> excludeList,
        final boolean dryRun) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_CONFIG);
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParametersOperations paramsOps = new ParametersOperations();
            paramsOps.exportParameterLibrary(destinationPath, excludeList,
                dryRun ? ParamIoMode.DRYRUN : ParamIoMode.STANDARD);
            return null;
        });
    }
}
