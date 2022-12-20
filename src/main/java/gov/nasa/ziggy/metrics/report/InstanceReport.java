package gov.nasa.ziggy.metrics.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lowagie.text.pdf.PdfPTable;

import gov.nasa.ziggy.module.PipelineCategories;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.util.TasksStates;
import gov.nasa.ziggy.util.dispmod.InstancesDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskSummaryDisplayModel;

public class InstanceReport extends Report {
    private static final Logger log = LoggerFactory.getLogger(InstanceReport.class);

    public InstanceReport(PdfRenderer pdfRenderer) {
        super(pdfRenderer);
    }

    public void generateReport(PipelineInstance instance, List<PipelineInstanceNode> nodes)
        throws Exception {
        PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();

        String instanceName = instance.getId() + ":" + instance.getName();

        pdfRenderer.printText("Performance Report for " + instanceName, PdfRenderer.titleFont);

        pdfRenderer.println();
        pdfRenderer.println();
        pdfRenderer.println();

        pdfRenderer.printText("Pipeline Instance Summary", PdfRenderer.h1Font);

        pdfRenderer.println();

        PdfPTable timeTable = new PdfPTable(3);
        timeTable.setWidthPercentage(100);

        addCell(timeTable, "Start", true);
        addCell(timeTable, "End", true);
        addCell(timeTable, "Total", true);

        addCell(timeTable, dateToDateString(instance.getStartProcessingTime()), false);
        addCell(timeTable, dateToDateString(instance.getEndProcessingTime()), false);
        String elapsedTime = instance.elapsedTime();
        addCell(timeTable, elapsedTime, false);

        pdfRenderer.add(timeTable);

        pdfRenderer.println();

        // Instance Summary
        InstancesDisplayModel instancesDisplayModel = new InstancesDisplayModel(instance);
        printDisplayModel("", instancesDisplayModel);

        pdfRenderer.println();

        // Task Summary
        List<PipelineTask> tasks = new ArrayList<>();

        for (PipelineInstanceNode node : nodes) {
            List<PipelineTask> nodeTasks = pipelineTaskCrud.retrieveAll(node);
            tasks.addAll(nodeTasks);
        }

        Map<Long, ProcessingSummary> taskAttrs = new ProcessingSummaryOperations()
            .processingSummaries(tasks);

        TaskSummaryDisplayModel tasksDisplayModel = new TaskSummaryDisplayModel(
            new TasksStates(tasks, taskAttrs));
        printDisplayModel("Pipeline Task Summary", tasksDisplayModel);

        pdfRenderer.println();

        pdfRenderer.printText("File Sizes and Transfer Rates", PdfRenderer.h1Font);
        pdfRenderer.println();

        generateTransferStats(nodes);
    }

    private String dateToDateString(java.util.Date date) {
        if (date.getTime() == 0) {
            return "--";
        }
        return date.toString();
    }

    private void generateTransferStats(List<PipelineInstanceNode> nodes) throws Exception {
        PdfPTable transfersTable = new PdfPTable(4);

        transfersTable.setWidthPercentage(100);

        addCell(transfersTable, "Node", true);
        addCell(transfersTable, "Transfer Type", true);
        addCell(transfersTable, "Size", true);
        addCell(transfersTable, "Transfer Rate", true);

        for (PipelineInstanceNode node : nodes) {
            transfersForNode(transfersTable, node, "Inputs (Ziggy->Compute)",
                PipelineCategories.TF_INPUTS_SIZE_CATEGORY,
                PipelineCategories.SEND_INPUTS_CATEGORY);

            transfersForNode(transfersTable, node, "Outputs (Compute->Ziggy)",
                PipelineCategories.TF_PFE_OUTPUTS_SIZE_CATEGORY,
                PipelineCategories.RECEIVE_OUTPUTS_CATEGORY);

            transfersForNode(transfersTable, node, "Archive (Ziggy->File System)",
                PipelineCategories.TF_ARCHIVE_SIZE_CATEGORY,
                PipelineCategories.COPY_TASK_FILES_CATEGORY);
        }
        pdfRenderer.add(transfersTable);
    }

    private void transfersForNode(PdfPTable transfersTable, PipelineInstanceNode node, String label,
        String sizeCategory, String timeCategory) {
        BytesFormat bytesFormatter = new BytesFormat();
        BytesPerSecondFormat rateFormatter = new BytesPerSecondFormat();

        PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();

        DescriptiveStatistics sizeStats = new DescriptiveStatistics();
        DescriptiveStatistics timeStats = new DescriptiveStatistics();

        List<PipelineTask> tasks = pipelineTaskCrud.retrieveAll(node);

        for (PipelineTask task : tasks) {
            List<PipelineTaskMetrics> taskMetrics = task.getSummaryMetrics();

            for (PipelineTaskMetrics taskMetric : taskMetrics) {
                if (taskMetric.getCategory().equals(sizeCategory)) {
                    sizeStats.addValue(taskMetric.getValue());
                } else if (taskMetric.getCategory().equals(timeCategory)) {
                    timeStats.addValue(taskMetric.getValue());
                }
            }
        }

        double bytesForNode = sizeStats.getSum();
        double millisForNode = timeStats.getSum();

        double bytesPerSecondForNode = bytesForNode / (millisForNode / 1000);

        log.info("bytesForNode = " + bytesForNode);
        log.info("millisForNode = " + millisForNode);
        log.info("bytesPerSecondForNode = " + bytesPerSecondForNode);

        addCell(transfersTable, node.getPipelineDefinitionNode().getModuleName().getName());
        addCell(transfersTable, label);
        addCell(transfersTable, bytesFormatter.format(bytesForNode));
        addCell(transfersTable, rateFormatter.format(bytesPerSecondForNode));
    }
}
