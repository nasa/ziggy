package gov.nasa.ziggy.uow;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.events.ZiggyEventLabels;

/**
 * Subclass of {@link DirectoryUnitOfWorkGenerator} that selects units of work based on the
 * directory tree below the data receipt directory.
 *
 * @author PT
 */
public class DataReceiptUnitOfWorkGenerator extends DirectoryUnitOfWorkGenerator {

    @Override
    protected Path rootDirectory() {
        return Paths.get(
            ZiggyConfiguration.getInstance().getString(PropertyNames.DATA_RECEIPT_DIR_PROP_NAME));
    }

    /**
     * Extends the superclass generateTasks() method by filtering out any DR unit of work that has a
     * directory of ".manifests".
     */
    @Override
    public List<UnitOfWork> generateTasks(Map<Class<? extends Parameters>, Parameters> parameters) {
        List<UnitOfWork> unitsOfWork = super.generateTasks(parameters);

        // If the pipeline that's going to execute data receipt was launched by an event handler,
        // we need to restrict the UOWs to the ones that are specified by the event handler.
        ZiggyEventLabels eventLabels = (ZiggyEventLabels) parameters.get(ZiggyEventLabels.class);
        if (eventLabels != null) {

            // Handle the special case in which the user wants to trigger data receipt to operate
            // at the top-level DR directory (i.e., not any subdirectories of same).
            if (eventLabels.getEventLabels().length == 0 && unitsOfWork.size() == 1
                && unitsOfWork.get(0).getParameter(DIRECTORY_PROPERTY_NAME).getString().isEmpty()) {
                return unitsOfWork;
            }
            Set<String> eventLabelSet = Sets.newHashSet(eventLabels.getEventLabels());
            unitsOfWork = unitsOfWork.stream()
                .filter(s -> eventLabelSet
                    .contains(s.getParameter(DIRECTORY_PROPERTY_NAME).getString()))
                .collect(Collectors.toList());
        }
        return unitsOfWork;

    }

}
