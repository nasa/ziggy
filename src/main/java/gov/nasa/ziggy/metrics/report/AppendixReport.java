package gov.nasa.ziggy.metrics.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.util.dispmod.TasksDisplayModel;

public class AppendixReport extends Report {
    public AppendixReport(PdfRenderer pdfRenderer) {
        super(pdfRenderer);
    }

    public void generateReport(PipelineInstance instance, List<PipelineInstanceNode> nodes) {
        // instance full (all tasks for specified nodes)
        PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
        List<PipelineTask> tasks = new ArrayList<>();

        Map<Long, ProcessingSummary> taskAttrs = new ProcessingSummaryOperations()
            .processingSummaries(tasks);

        for (PipelineInstanceNode node : nodes) {
            List<PipelineTask> nodeTasks = pipelineTaskCrud.retrieveAll(node);
            tasks.addAll(nodeTasks);
        }

        TasksDisplayModel tasksDisplayModel = new TasksDisplayModel(tasks, taskAttrs);
        float[] colsWidth = { 0.5f, 0.5f, 2f, 1f, 1f, 0.5f, 1.5f };

        printDisplayModel("Appendix A: All Tasks", tasksDisplayModel, colsWidth);

        // instance report (params)
        pdfRenderer.newPage();
        pdfRenderer.printText("Appendix B: Parameters and Data Model Registry Report",
            PdfRenderer.h1Font);
        PipelineOperations ops = new PipelineOperations();
        String report = ops.generatePedigreeReport(instance);
        pdfRenderer.printText(report, PdfRenderer.bodyMonoFont);
    }
}
