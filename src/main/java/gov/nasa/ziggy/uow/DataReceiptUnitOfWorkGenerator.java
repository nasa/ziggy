package gov.nasa.ziggy.uow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.data.management.Manifest;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Subclass of {@link DirectoryUnitOfWorkGenerator} that selects units of work based on the
 * directory tree below the data receipt directory.
 *
 * @author PT
 */
public class DataReceiptUnitOfWorkGenerator extends DirectoryUnitOfWorkGenerator {

    @Override
    protected Path rootDirectory() {
        return Paths
            .get(ZiggyConfiguration.getInstance()
                .getString(PropertyName.DATA_RECEIPT_DIR.property()))
            .toAbsolutePath();
    }

    @Override
    public List<UnitOfWork> generateUnitsOfWork(PipelineInstanceNode pipelineInstanceNode) {
        return generateUnitsOfWork(pipelineInstanceNode, null);
    }

    /**
     * Generates units of work by looking for a manifest in the data receipt directory (in which
     * case the data receipt directory is the only UOW), if none is found then searching the
     * top-level subdirectories of the data receipt directory for manifests (each directory that has
     * one becomes a UOW). The resulting UOWs are filtered by the event labels argument so that, if
     * there are any event labels, only units of work that match the event labels will be processed.
     */
    @Override
    public List<UnitOfWork> generateUnitsOfWork(PipelineInstanceNode pipelineInstanceNode,
        Set<String> eventLabels) {
        List<UnitOfWork> unitsOfWork = new ArrayList<>();

        // If the root directory for this UOW generator contains a manifest, then the root directory
        // will be the only unit of work.
        if (directoryContainsManifest(rootDirectory())) {
            UnitOfWork uow = new UnitOfWork();
            uow.addParameter(new Parameter(DIRECTORY_PARAMETER_NAME, rootDirectory().toString(),
                ZiggyDataType.ZIGGY_STRING));
            unitsOfWork.add(uow);
        } else {

            // Check for subdirectories that have manifests
            Set<Path> subdirs = subdirsWithManifests();
            for (Path subdir : subdirs) {
                UnitOfWork uow = new UnitOfWork();
                uow.addParameter(new Parameter(DIRECTORY_PARAMETER_NAME, subdir.toString(),
                    ZiggyDataType.ZIGGY_STRING));
                unitsOfWork.add(uow);
            }
        }

        // Handle two special cases: the event labels Set is null (DR not triggered by an
        // event); the event labels Set is non-null but empty (DR triggered by an event but
        // the event wants DR to run in the main DR directory). In either case, there can
        // be only one UOW.
        if (eventLabels == null || eventLabels.size() == 0 && unitsOfWork.size() == 1
            && unitsOfWork.get(0)
                .getParameter(DIRECTORY_PARAMETER_NAME)
                .getString()
                .equals(rootDirectory().toString())) {
            return unitsOfWork;
        }

        // Otherwise, filter against the event labels.
        return unitsOfWork.stream()
            .filter(s -> eventLabels.contains(uowDirectoryFileName(s)))
            .collect(Collectors.toList());
    }

    private String uowDirectoryFileName(UnitOfWork uow) {
        Path uowDirectory = Paths.get(uow.getParameter(DIRECTORY_PARAMETER_NAME).getString());
        return uowDirectory.getFileName().toString();
    }

    @Override
    public void setBriefState(UnitOfWork uow, PipelineInstanceNode pipelineInstanceNode) {
        Path directory = Paths.get(uow.getParameter(DIRECTORY_PARAMETER_NAME).getString());
        uow.setBriefState(directory.getFileName().toString());
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private boolean directoryContainsManifest(Path directory) {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory,
            file -> file.getFileName().toString().endsWith(Manifest.FILENAME_SUFFIX))) {
            for (@SuppressWarnings("unused")
            Path manifestFile : dirStream) {
                return true;
            }
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Set<Path> subdirsWithManifests() {
        Set<Path> subdirsWithManifests = new HashSet<>();
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(rootDirectory(),
            Files::isDirectory)) {
            for (Path subdirPath : dirStream) {
                if (directoryContainsManifest(subdirPath)) {
                    subdirsWithManifests.add(subdirPath);
                }
            }
            return subdirsWithManifests;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
