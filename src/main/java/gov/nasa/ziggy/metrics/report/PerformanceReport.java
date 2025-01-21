/*
 * Copyright (C) 2022-2025 United States Government as represented by the Administrator of the
 * National Aeronautics and Space Administration. All Rights Reserved.
 *
 * NASA acknowledges the SETI Institute's primary role in authoring and producing Ziggy, a Pipeline
 * Management System for Data Analysis Pipelines, under Cooperative Agreement Nos. NNX14AH97A,
 * 80NSSC18M0068 & 80NSSC21M0079.
 *
 * This file is available under the terms of the NASA Open Source Agreement (NOSA). You should have
 * received a copy of this agreement with the Ziggy source code; see the file LICENSE.pdf.
 *
 * Disclaimers
 *
 * No Warranty: THE SUBJECT SOFTWARE IS PROVIDED "AS IS" WITHOUT ANY WARRANTY OF ANY KIND, EITHER
 * EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED TO, ANY WARRANTY THAT THE SUBJECT
 * SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR FREEDOM FROM INFRINGEMENT, ANY WARRANTY THAT THE SUBJECT SOFTWARE WILL BE
 * ERROR FREE, OR ANY WARRANTY THAT DOCUMENTATION, IF PROVIDED, WILL CONFORM TO THE SUBJECT
 * SOFTWARE. THIS AGREEMENT DOES NOT, IN ANY MANNER, CONSTITUTE AN ENDORSEMENT BY GOVERNMENT AGENCY
 * OR ANY PRIOR RECIPIENT OF ANY RESULTS, RESULTING DESIGNS, HARDWARE, SOFTWARE PRODUCTS OR ANY
 * OTHER APPLICATIONS RESULTING FROM USE OF THE SUBJECT SOFTWARE. FURTHER, GOVERNMENT AGENCY
 * DISCLAIMS ALL WARRANTIES AND LIABILITIES REGARDING THIRD-PARTY SOFTWARE, IF PRESENT IN THE
 * ORIGINAL SOFTWARE, AND DISTRIBUTES IT "AS IS."
 *
 * Waiver and Indemnity: RECIPIENT AGREES TO WAIVE ANY AND ALL CLAIMS AGAINST THE UNITED STATES
 * GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT. IF RECIPIENT'S
 * USE OF THE SUBJECT SOFTWARE RESULTS IN ANY LIABILITIES, DEMANDS, DAMAGES, EXPENSES OR LOSSES
 * ARISING FROM SUCH USE, INCLUDING ANY DAMAGES FROM PRODUCTS BASED ON, OR RESULTING FROM,
 * RECIPIENT'S USE OF THE SUBJECT SOFTWARE, RECIPIENT SHALL INDEMNIFY AND HOLD HARMLESS THE UNITED
 * STATES GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT, TO THE
 * EXTENT PERMITTED BY LAW. RECIPIENT'S SOLE REMEDY FOR ANY SUCH MATTER SHALL BE THE IMMEDIATE,
 * UNILATERAL TERMINATION OF THIS AGREEMENT.
 */

package gov.nasa.ziggy.metrics.report;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * @author Todd Klaus
 */
public class PerformanceReport {
    private static final Logger log = LoggerFactory.getLogger(PerformanceReport.class);

    private static String INSTANCE_ID_OPT = "id";
    private static String TASK_FILES_OPT = "taskdir";
    private static String NODE_IDS_OPT = "nodes";
    private static String FORCE_OPT = "force";

    private final long instanceId;
    private final File taskFilesDir;
    private final NodeIndexRange nodes;
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();

    public PerformanceReport(long instanceId, File taskFilesDir, NodeIndexRange nodes) {
        this.instanceId = instanceId;
        this.taskFilesDir = taskFilesDir;
        this.nodes = nodes;
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public Path generateReport() {
        log.info("Generating performance report");

        PipelineInstance instance = pipelineInstanceOperations().pipelineInstance(instanceId);

        if (instance == null) {
            System.err.println("No instance found with ID = " + instanceId);
            System.exit(-1);
        }

        Path outputPath = ReportFilePaths.performanceReportPath(instanceId);
        try {
            Files.createDirectories(outputPath.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to create directory " + outputPath.getParent().toString(), e);
        }

        log.info("Writing report to {}", outputPath.toString());

        PdfRenderer pdfRenderer = new PdfRenderer(outputPath.toFile(), false);

        List<PipelineInstanceNode> instanceNodes = pipelineInstanceOperations()
            .instanceNodes(instance);
        List<PipelineInstanceNode> nodesToProcess = nodes == null ? instanceNodes
            : selectNodes(instanceNodes);

        pdfRenderer.printText(
            "Nodes included in report: " + (nodes == null ? "All" : nodes.toString()),
            PdfRenderer.h1Font);
        pdfRenderer.println();

        new InstanceReport(pdfRenderer).generateReport(instance, nodesToProcess);

        if (nodesToProcess.isEmpty()) {
            System.err.println("No instance nodes found for instance = " + instanceId);
            System.exit(-1);
        } else {
            pdfRenderer.newPage();

            for (PipelineInstanceNode node : nodesToProcess) {
                generateNodeReport(pdfRenderer, node);
            }
        }

        pdfRenderer.newPage();

        new AppendixReport(pdfRenderer).generateReport(instance, nodesToProcess);

        pdfRenderer.close();

        log.info("Writing report to {}...done", outputPath.toString());

        return outputPath;
    }

    private List<PipelineInstanceNode> selectNodes(List<PipelineInstanceNode> instanceNodes) {
        int startNode = nodes.getStartNodeIndex();
        int endNode = nodes.getEndNodeIndex();

        if (startNode < 0 || startNode > instanceNodes.size() - 1 || endNode < 0
            || endNode > instanceNodes.size() - 1 || startNode > endNode) {
            throw new PipelineException("Invalid node range: " + nodes);
        }

        log.info("Processing nodes {} to {}", startNode, endNode);

        return instanceNodes.subList(startNode, endNode + 1);
    }

    private void generateNodeReport(PdfRenderer pdfRenderer, PipelineInstanceNode node) {
        String moduleName = node.getModuleName();

        NodeReport nodeReport = new NodeReport(pdfRenderer);
        nodeReport.generateReport(node);

        pdfRenderer.newPage();

        if (taskFilesDir != null) {
            // generate matlab report
            MatlabReport matlabReport = new MatlabReport(pdfRenderer, taskFilesDir, moduleName,
                instanceId);
            matlabReport.generateReport();
        } else {
            pdfRenderer.printText(
                "No per-process statistics available: Task files directory not specified",
                PdfRenderer.h1Font);
        }

        pdfRenderer.newPage();

        log.info("Category report");

        List<String> orderedCategoryNames = nodeReport.getOrderedCategoryNames();
        Map<String, DescriptiveStatistics> categoryStats = nodeReport.getCategoryStats();
        Map<String, TopNList> topTen = nodeReport.getCategoryTopTen();

        for (String category : orderedCategoryNames) {
            log.info("Processing category {}", category);

            boolean isTime = nodeReport.categoryIsTime(category);
            CategoryReport categoryReport = new CategoryReport(category, pdfRenderer, isTime);
            categoryReport.generateReport(moduleName, categoryStats.get(category),
                topTen.get(category));
        }
    }

    private static void usageAndExit(String msg, Options options) {
        System.err.println(msg);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("perf-report", options);
        System.exit(-1);
    }

    @AcceptableCatchBlock(rationale = Rationale.USAGE)
    @AcceptableCatchBlock(rationale = Rationale.USAGE)
    private static NodeIndexRange parseNodesArg(String nodesArg, Options options) {
        String[] parts = nodesArg.split(":");

        if (parts.length != 2) {
            usageAndExit(
                "Node indices must be specified in START:END format. You entered: " + nodesArg,
                options);
        }

        int startNodeIndex = -1;
        int endNodeIndex = -1;

        try {
            startNodeIndex = Integer.parseInt(parts[0]);
        } catch (NumberFormatException e) {
            usageAndExit("Invalid start node index: " + parts[0], options);
        }

        try {
            endNodeIndex = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            usageAndExit("Invalid end node index: " + parts[1], options);
        }

        return new NodeIndexRange(startNodeIndex, endNodeIndex);
    }

    @AcceptableCatchBlock(rationale = Rationale.USAGE)
    @AcceptableCatchBlock(rationale = Rationale.USAGE)
    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(INSTANCE_ID_OPT, true, "Pipeline instance ID");
        options.addOption(TASK_FILES_OPT, true, "Top-level task file for instance");
        options.addOption(NODE_IDS_OPT, true,
            "Start and end node indices in START:END format. Default is all nodes");
        options.addOption(FORCE_OPT, false,
            "Force generation of report without specifying task file directory. "
                + "If the task dir is not specified, CPU and memory stats will not be included in report");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            usageAndExit("Illegal argument: " + e.getMessage(), options);
        }

        String instanceIdArg = cmdLine.getOptionValue(INSTANCE_ID_OPT);

        long instanceId = -1;
        try {
            instanceId = Long.parseLong(instanceIdArg);
        } catch (NumberFormatException e) {
            usageAndExit("Invalid instanceId: " + instanceIdArg, options);
        }

        String taskDirArg = cmdLine.getOptionValue(TASK_FILES_OPT);

        if (taskDirArg == null && !cmdLine.hasOption(FORCE_OPT)) {
            usageAndExit("Task file dir not specified.  "
                + "If the task dir is not specified, CPU and memory stats will not be included in report. "
                + "To force generation of the report without CPU & memory stats, use the -force option."
                + instanceIdArg, options);
        }

        NodeIndexRange nodes = null;
        if (cmdLine.hasOption(NODE_IDS_OPT)) {
            String nodesArg = cmdLine.getOptionValue(NODE_IDS_OPT);
            nodes = parseNodesArg(nodesArg, options);
        }

        File taskDir = null;
        if (taskDirArg != null) {
            taskDir = new File(taskDirArg);
        }
        PerformanceReport report = new PerformanceReport(instanceId, taskDir, nodes);
        report.generateReport();
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }
}
