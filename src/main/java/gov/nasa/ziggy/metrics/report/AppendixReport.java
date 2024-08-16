package gov.nasa.ziggy.metrics.report;

import java.util.List;

import gov.nasa.ziggy.pipeline.PipelineReportGenerator;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.util.dispmod.TasksDisplayModel;

public class AppendixReport extends Report {

    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();

    public AppendixReport(PdfRenderer pdfRenderer) {
        super(pdfRenderer);
    }

    public void generateReport(PipelineInstance instance, List<PipelineInstanceNode> nodes) {

        List<PipelineTask> tasks = pipelineInstanceNodeOperations().pipelineTasks(nodes);

        TasksDisplayModel tasksDisplayModel = new TasksDisplayModel(tasks);
        float[] colsWidth = { 0.5f, 0.5f, 2f, 1f, 1f, 0.5f, 1.5f };

        printDisplayModel("Appendix A: All Tasks", tasksDisplayModel, colsWidth);

        // instance report (params)
        pdfRenderer.newPage();
        pdfRenderer.printText("Appendix B: Parameters and Data Model Registry Report",
            PdfRenderer.h1Font);
        String report = new PipelineReportGenerator().generatePedigreeReport(instance);
        pdfRenderer.printText(report, PdfRenderer.bodyMonoFont);
    }

    PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }
}
