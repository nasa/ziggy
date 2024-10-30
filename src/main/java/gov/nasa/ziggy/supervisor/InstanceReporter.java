package gov.nasa.ziggy.supervisor;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDisplayDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.dispmod.InstancesDisplayModel;
import gov.nasa.ziggy.util.dispmod.PipelineStatsDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskSummaryDisplayModel;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * Creates a report for a {@link PipelineInstance}.
 *
 * @author Miles Cote
 */
public class InstanceReporter {

    private final PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private final PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public File report(PipelineInstance instance, File outputDir) {
        checkNotNull(instance, "instance cannot be null.");
        checkNotNull(outputDir, "outputDir cannot be null.");

        outputDir.mkdirs();

        File reportFile = new File(outputDir, "instance-" + instance.getId() + "-report.txt");
        reportFile.delete();

        try (PrintStream printStream = new PrintStream(reportFile,
            ZiggyFileUtils.ZIGGY_CHARSET_NAME)) {
            printStream.print("state: " + instance.getState() + "\n\n");

            InstancesDisplayModel instancesDisplayModel = new InstancesDisplayModel(instance);
            instancesDisplayModel.print(printStream, "Instance Summary");
            printStream.println();

            List<PipelineTaskDisplayData> tasks = pipelineTaskDisplayDataOperations()
                .pipelineTaskDisplayData(instance);

            TaskSummaryDisplayModel taskSummaryDisplayModel = new TaskSummaryDisplayModel(
                new TaskCounts(tasks));
            taskSummaryDisplayModel.print(printStream, "Instance Task Summary");

            TaskCounts taskCounts = taskSummaryDisplayModel.getTaskCounts();
            List<String> orderedModuleNames = taskCounts.getModuleNames();

            PipelineStatsDisplayModel pipelineStatsDisplayModel = new PipelineStatsDisplayModel(
                tasks, orderedModuleNames);
            pipelineStatsDisplayModel.print(printStream, "Processing Time Statistics");

            TaskMetricsDisplayModel taskMetricsDisplayModel = new TaskMetricsDisplayModel(tasks,
                orderedModuleNames);
            taskMetricsDisplayModel.print(printStream,
                "Processing Time Breakdown (completed tasks only)");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            // This can never occur. A supported character set is used, and the file is guaranteed
            // to be legitimate.
            throw new AssertionError(e);
        }

        return reportFile;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }
}
