package gov.nasa.ziggy.pipeline.definition.importer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import gov.nasa.ziggy.pipeline.step.remote.Architecture;
import gov.nasa.ziggy.pipeline.step.remote.BatchQueue;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironment;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironmentOperations;
import gov.nasa.ziggy.util.PipelineException;

/**
 * Prepares {@link RemoteEnvironment} definitions from XML for persistence in the database.
 * Specifically:
 * <ol>
 * <li>Rejection of update imports if the update flag is not set.
 * <li>Performing update imports if the update flag is set.
 * <li>Checks for uniqueness of {@link RemoteEnvironment} names.
 * <li>Checks for uniqueness of {@link Architecture} names within a remote environment.
 * <li>Checks for uniqueness of {@link BatchQueue} names within a remote environment.
 * </ol>
 *
 * @author PT
 */

public class RemoteEnvironmentImportConditioner {

    private RemoteEnvironmentOperations remoteEnvironmentOperations = new RemoteEnvironmentOperations();

    private List<RemoteEnvironment> remoteEnvironments;
    private boolean update;

    public RemoteEnvironmentImportConditioner(List<RemoteEnvironment> remoteEnvironments,
        boolean updateFlag) {
        this.remoteEnvironments = remoteEnvironments;
        for (RemoteEnvironment remoteEnvironment : remoteEnvironments) {
            if (CollectionUtils.isEmpty(remoteEnvironment.getArchitectures())) {
                remoteEnvironment.populateDatabaseFields();
            }
        }
        update = updateFlag;
    }

    public List<RemoteEnvironment> remoteEnvironmentsToPersist() {
        checkImportedEnvironmentUniquenessRequirements();
        Map<String, RemoteEnvironment> databaseRemoteEnvironmentByName = remoteEnvironmentOperations()
            .remoteEnvironmentByName();
        List<RemoteEnvironment> remoteEnvironmentsToPersist = new ArrayList<>();
        for (RemoteEnvironment remoteEnvironment : remoteEnvironments) {

            // Genuinely new environments: always persist.
            if (!databaseRemoteEnvironmentByName.containsKey(remoteEnvironment.getName())) {
                remoteEnvironmentsToPersist.add(remoteEnvironment);
            } else if (update) {

                // Update environments that are present in the import and in the database.
                remoteEnvironmentsToPersist.add(updatedRemoteEnvironment(remoteEnvironment,
                    databaseRemoteEnvironmentByName.get(remoteEnvironment.getName())));
            }
        }
        return remoteEnvironmentsToPersist;
    }

    // Checks that all imported environments have unique names, and that within an environment
    // all architectures and all batch queues have unique names.
    private void checkImportedEnvironmentUniquenessRequirements() {
        List<String> duplicateEnvironmentNames = duplicates(
            remoteEnvironments.stream().map(RemoteEnvironment::getName).toList());
        if (!CollectionUtils.isEmpty(duplicateEnvironmentNames)) {
            throw new PipelineException(
                "Multiple remote environments with names: " + duplicateEnvironmentNames.toString());
        }
        for (RemoteEnvironment environment : remoteEnvironments) {
            List<String> architectureNames = environment.getArchitectures()
                .stream()
                .map(Architecture::getName)
                .toList();
            List<String> duplicateArchitectureNames = duplicates(architectureNames);
            if (!CollectionUtils.isEmpty(duplicateArchitectureNames)) {
                throw new PipelineException("Environment " + environment.getName()
                    + " has multiple architectures with names: "
                    + duplicateArchitectureNames.toString());
            }
            List<String> duplicateQueueNames = duplicates(
                environment.getQueues().stream().map(BatchQueue::getName).toList());
            if (!CollectionUtils.isEmpty(duplicateQueueNames)) {
                throw new PipelineException("Environment " + environment.getName()
                    + " has multiple queues with names: " + duplicateQueueNames.toString());
            }
        }
    }

    private List<String> duplicates(List<String> names) {
        return names.stream().filter(s -> Collections.frequency(names, s) > 1).toList();
    }

    // Takes the database version of an environment and replaces all its values with the ones
    // from the imported version of the environment.
    private RemoteEnvironment updatedRemoteEnvironment(RemoteEnvironment importedRemoteEnvironment,
        RemoteEnvironment databaseRemoteEnvironment) {
        List<Architecture> updatedArchitectures = updatedArchitectures(
            importedRemoteEnvironment.getArchitectures(),
            databaseRemoteEnvironment.getArchitectures());
        List<BatchQueue> updatedBatchQueues = updatedBatchQueues(
            importedRemoteEnvironment.getQueues(), databaseRemoteEnvironment.getQueues());
        databaseRemoteEnvironment.updateFrom(importedRemoteEnvironment, updatedArchitectures,
            updatedBatchQueues);
        return databaseRemoteEnvironment;
    }

    // Identifies architectures that are updates to the database and puts the new values into
    // the database instance; persists all genuinely new architectures.
    private List<Architecture> updatedArchitectures(List<Architecture> importedArchitectures,
        List<Architecture> databaseArchitectures) {
        List<Architecture> updatedArchitectures = new ArrayList<>();
        Map<String, Architecture> databaseArchitectureByName = architectureByName(
            databaseArchitectures);
        for (Architecture importedArchitecture : importedArchitectures) {
            if (!databaseArchitectureByName.containsKey(importedArchitecture.getName())) {
                updatedArchitectures.add(remoteEnvironmentOperations().merge(importedArchitecture));
            } else {
                Architecture databaseArchitecture = databaseArchitectureByName
                    .get(importedArchitecture.getName());
                databaseArchitecture.updateFrom(importedArchitecture);
                updatedArchitectures.add(databaseArchitecture);
            }
        }
        return updatedArchitectures;
    }

    Map<String, Architecture> architectureByName(List<Architecture> architectures) {
        Map<String, Architecture> architectureByName = new HashMap<>();
        for (Architecture architecture : architectures) {
            architectureByName.put(architecture.getName(), architecture);
        }
        return architectureByName;
    }

    // Identifies batch queues that are updates to the database and puts the new values into
    // the database instance; persists all genuinely new batch queues.
    private List<BatchQueue> updatedBatchQueues(List<BatchQueue> importedBatchQueues,
        List<BatchQueue> databaseBatchQueues) {
        List<BatchQueue> updatedBatchQueues = new ArrayList<>();
        Map<String, BatchQueue> batchQueueByName = batchQueueByName(databaseBatchQueues);
        for (BatchQueue importedBatchQueue : importedBatchQueues) {
            if (!batchQueueByName.containsKey(importedBatchQueue.getName())) {
                updatedBatchQueues.add(remoteEnvironmentOperations().merge(importedBatchQueue));
            } else {
                BatchQueue databaseBatchQueue = batchQueueByName.get(importedBatchQueue.getName());
                databaseBatchQueue.updateFrom(importedBatchQueue);
                updatedBatchQueues.add(databaseBatchQueue);
            }
        }
        return updatedBatchQueues;
    }

    Map<String, BatchQueue> batchQueueByName(List<BatchQueue> batchQueues) {
        Map<String, BatchQueue> batchQueueByName = new HashMap<>();
        for (BatchQueue batchQueue : batchQueues) {
            batchQueueByName.put(batchQueue.getName(), batchQueue);
        }
        return batchQueueByName;
    }

    RemoteEnvironmentOperations remoteEnvironmentOperations() {
        return remoteEnvironmentOperations;
    }
}
