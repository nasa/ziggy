package gov.nasa.ziggy.report;

import java.util.List;

import com.lowagie.text.Element;
import com.lowagie.text.pdf.PdfPTable;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDisplayDataOperations;
import gov.nasa.ziggy.util.ZiggyStringUtils;
import gov.nasa.ziggy.util.dispmod.InstancesDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskSummaryDisplayModel;

public class InstanceReport extends Report {
    private static final int PIPELINE_INSTANCE_SUMMARY = 0;
    private static final int PIPELINE_TASK_SUMMARY = 1;
    private static final int PROCESSING_TIMES_AND_SIZES = 2;

    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();

    public InstanceReport(PdfRenderer pdfRenderer) {
        super(pdfRenderer);
    }

    public List<String> headings() {
        return List.of("Pipeline Instance Summary", "Pipeline Task Summary",
            "Processing Times and Sizes");
    }

    public void generateReport(PipelineInstance instance, List<PipelineInstanceNode> nodes) {

        printDisplayModel(headings().get(PIPELINE_INSTANCE_SUMMARY),
            new InstancesDisplayModel(instance));

        pdfRenderer.newPage();
        printDisplayModel(headings().get(PIPELINE_TASK_SUMMARY),
            new TaskSummaryDisplayModel(pipelineTaskDisplayDataOperations().taskCounts(nodes)));

        pdfRenderer.newPage();
        printProcessingTimesAndSizes(headings().get(PROCESSING_TIMES_AND_SIZES), nodes);
    }

    private void printProcessingTimesAndSizes(String heading, List<PipelineInstanceNode> nodes) {

        pdfRenderer.printText(heading, PdfRenderer.h1Font);
        pdfRenderer.println();

        List<PipelineTaskDisplayData> tasks = pipelineTaskDisplayDataOperations()
            .pipelineTaskDisplayDataForNodes(nodes);
        TaskMetricsDisplayModel taskMetricsDisplayModel = new TaskMetricsDisplayModel(tasks,
            PipelineTaskDisplayDataOperations.orderedPipelineStepNames(tasks), true);

        PdfPTable table = new PdfPTable(taskMetricsDisplayModel.getColumnCount());
        table.setWidthPercentage(100);

        for (int i = 0; i < taskMetricsDisplayModel.getColumnCount(); i++) {
            table.addCell(createCell(taskMetricsDisplayModel.getColumnName(i), true));
        }

        for (int row = 0; row < taskMetricsDisplayModel.getRowCount(); row++) {
            for (int column = 0; column < taskMetricsDisplayModel.getColumnCount(); column++) {
                String value = taskMetricsDisplayModel.getValueAt(row, column).toString();
                int alignment = ZiggyStringUtils.NO_DATA.equals(value) ? Element.ALIGN_CENTER
                    : taskMetricsDisplayModel.getAlignment(column);
                table.addCell(createCell(value, alignment, isOddNumberedTableRow(table)));
            }
        }

        pdfRenderer.add(table);
    }

    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }
}
