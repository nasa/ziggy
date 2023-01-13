package gov.nasa.ziggy.worker;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.util.TasksStates;
import gov.nasa.ziggy.util.dispmod.InstancesDisplayModel;
import gov.nasa.ziggy.util.dispmod.PipelineStatsDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskSummaryDisplayModel;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Creates a report for a {@link PipelineInstance}.
 *
 * @author Miles Cote
 */
public class InstanceReporter {
    public File report(PipelineInstance instance, File outputDir)
        throws FileNotFoundException, UnsupportedEncodingException {
        checkNotNull(instance, "instance cannot be null.");
        checkNotNull(outputDir, "outputDir cannot be null.");

        outputDir.mkdirs();

        File reportFile = new File(outputDir, "instance-" + instance.getId() + "-report.txt");
        reportFile.delete();

        try (PrintStream printStream = new PrintStream(reportFile, FileUtil.ZIGGY_CHARSET_NAME)) {
            printStream.print("state: " + instance.getState() + "\n\n");

            InstancesDisplayModel instancesDisplayModel = new InstancesDisplayModel(instance);
            instancesDisplayModel.print(printStream, "Instance Summary");
            printStream.println();

            PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
            List<PipelineTask> tasks = pipelineTaskCrud.retrieveTasksForInstance(instance);

            Map<Long, ProcessingSummary> taskAttrs = new ProcessingSummaryOperations()
                .processingSummaries(tasks);

            TaskSummaryDisplayModel taskSummaryDisplayModel = new TaskSummaryDisplayModel(
                new TasksStates(tasks, taskAttrs));
            taskSummaryDisplayModel.print(printStream, "Instance Task Summary");

            TasksStates tasksStates = taskSummaryDisplayModel.getTaskStates();
            List<String> orderedModuleNames = tasksStates.getModuleNames();

            PipelineStatsDisplayModel pipelineStatsDisplayModel = new PipelineStatsDisplayModel(
                tasks, orderedModuleNames);
            pipelineStatsDisplayModel.print(printStream, "Processing Time Statistics");

            TaskMetricsDisplayModel taskMetricsDisplayModel = new TaskMetricsDisplayModel(tasks,
                orderedModuleNames);
            taskMetricsDisplayModel.print(printStream,
                "Processing Time Breakdown (completed tasks only)");
        }

        return reportFile;
    }
}
