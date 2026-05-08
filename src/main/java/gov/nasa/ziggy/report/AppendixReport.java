package gov.nasa.ziggy.report;

import java.util.List;

import gov.nasa.ziggy.pipeline.PipelineReportGenerator;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDisplayDataOperations;
import gov.nasa.ziggy.util.dispmod.TasksDisplayModel;

public class AppendixReport extends Report {

    private static final String APPENDIX_A_HEADING = "Appendix A: All Tasks";
    private static final String APPENDIX_B_HEADING = "Appendix B: Parameters and Data Model Registry Report";
    public static final List<String> HEADINGS = List.of(APPENDIX_A_HEADING, APPENDIX_B_HEADING);

    private final PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();
    private final PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();

    public AppendixReport(PdfRenderer pdfRenderer) {
        super(pdfRenderer);
    }

    public void generateReport(PipelineInstance instance, List<PipelineInstanceNode> nodes) {

        List<PipelineTask> tasks = pipelineInstanceNodeOperations().pipelineTasks(nodes);
        List<PipelineTaskDisplayData> taskData = pipelineTaskDisplayDataOperations()
            .pipelineTaskDisplayData(tasks);

        float[] colsWidth = { 1.0f, 3.0f, 3.0f, 2.0f, 2.0f, 2.0f, 1.5f };
        printDisplayModel(APPENDIX_A_HEADING, new TasksDisplayModel(taskData), colsWidth);

        pdfRenderer.newPage();
        pdfRenderer.printText(APPENDIX_B_HEADING, PdfRenderer.h1Font);
        String report = new PipelineReportGenerator().generatePedigreeReport(instance);
        pdfRenderer.printText(report, PdfRenderer.bodyMonoFont);
    }

    PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }

    PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }
}
