package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.parameters.ParameterSetDescriptor;
import gov.nasa.ziggy.parameters.ParametersOperations;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * GUI Proxy class for {@link ParametersOperations}
 *
 * @author Todd Klaus
 */
public class ParametersOperationsProxy extends CrudProxy {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ParametersOperationsProxy.class);

    public ParametersOperationsProxy() {
    }

    public List<ParameterSetDescriptor> importParameterLibrary(final String sourcePath,
        final List<String> excludeList, final boolean dryRun) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        List<ParameterSetDescriptor> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                ParametersOperations paramsOps = new ParametersOperations();
                List<ParameterSetDescriptor> results = paramsOps.importParameterLibrary(sourcePath,
                    excludeList, dryRun ? ParamIoMode.DRYRUN : ParamIoMode.STANDARD);
                return results;
            });
        return result;
    }

    public void exportParameterLibrary(final String destinationPath, final List<String> excludeList,
        final boolean dryRun) {
        verifyPrivileges(Privilege.PIPELINE_CONFIG);
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParametersOperations paramsOps = new ParametersOperations();
            paramsOps.exportParameterLibrary(destinationPath, excludeList,
                dryRun ? ParamIoMode.DRYRUN : ParamIoMode.STANDARD);
            return null;
        });
    }
}
