/*
 * Copyright (C) 2022-2026 United States Government as represented by the Administrator of the
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

package gov.nasa.ziggy.report;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.PipelineException;

/**
 * @author Todd Klaus
 */
public class PerformanceReport {
    private static final Logger log = LoggerFactory.getLogger(PerformanceReport.class);

    private static final String HELP_OPTION = "help";
    private static final String INSTANCE_OPTION = "instance";
    private static final String NODES_OPTION = "nodes";

    private static final String COMMAND_HELP = """

        Options:""";
    private static final int HELP_WIDTH = 100;

    private final long instanceId;
    private final NodeIndexRange nodes;
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();

    public PerformanceReport(Long instanceId) {
        this(instanceId, null);
    }

    public PerformanceReport(long instanceId, NodeIndexRange nodes) {
        this.instanceId = instanceId;
        this.nodes = nodes;
    }

    @AcceptableCatchBlock(rationale = Rationale.USAGE)
    @AcceptableCatchBlock(rationale = Rationale.USAGE)
    public static void main(String[] args) {
        Options options = new Options()
            .addOption(Option.builder("i")
                .longOpt(INSTANCE_OPTION)
                .hasArg()
                .type(Long.class) // if only this did the type checking for us
                .required()
                .desc("Instance ID")
                .build())
            .addOption(Option.builder("h").longOpt(HELP_OPTION).desc("Show this help").build())
            .addOption(Option.builder("t")
                .longOpt(NODES_OPTION)
                .hasArgs()
                .desc("Start and end node 0-based indices in START:END format (default: all nodes)")
                .build());

        org.apache.commons.cli.CommandLine cmdLine = null;
        try {
            cmdLine = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            usageAndExit(options, e.getMessage());
        }

        String instanceOption = cmdLine.getOptionValue(INSTANCE_OPTION);
        long instanceId = -1;
        try {
            instanceId = Long.parseLong(instanceOption);
        } catch (NumberFormatException e) {
            usageAndExit(options,
                "Could not parse instance ID " + instanceOption + ": " + e.getMessage());
        }

        NodeIndexRange nodes = null;
        try {
            if (cmdLine.hasOption(NODES_OPTION)) {
                nodes = parseNodes(cmdLine.getOptionValue(NODES_OPTION));
            }
        } catch (Exception e) {
            usageAndExit(options, e.getMessage());
        }

        Path report = new PerformanceReport(instanceId, nodes).generateReport();
        System.out.println("Wrote report to " + report);
    }

    private static void usageAndExit(Options options, String message) {
        if (options != null) {
            if (message != null) {
                System.err.println(message);
            }
            new HelpFormatter().printHelp(HELP_WIDTH, "PerformanceReport [options]", COMMAND_HELP,
                options, null);
        }
        System.exit(-1);
    }

    @AcceptableCatchBlock(rationale = Rationale.USAGE)
    @AcceptableCatchBlock(rationale = Rationale.USAGE)
    private static NodeIndexRange parseNodes(String nodesOption) {
        String[] nodes = nodesOption.split(":");

        if (nodes.length != 2) {
            throw new IllegalArgumentException(
                "Node indices " + nodesOption + " were not specified in START:END format");
        }

        return new NodeIndexRange(parseNode(nodes[0]), parseNode(nodes[1]));
    }

    private static int parseNode(String node) {
        try {
            return Integer.parseInt(node);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid node index " + node);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public Path generateReport() {
        log.info("Generating performance report");

        PipelineInstance instance = pipelineInstanceOperations().pipelineInstance(instanceId);

        if (instance == null) {
            System.err.println("No instance found with ID = " + instanceId);
            System.exit(-1);
        }

        Path outputPath = ReportFilePaths.performanceReportPath(instance.getPipeline().getName(),
            instanceId);
        try {
            Files.createDirectories(outputPath.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to create directory " + outputPath.getParent().toString(), e);
        }

        log.info("Writing report to {}...", outputPath.toString());

        PdfRenderer pdfRenderer = new PdfRenderer(outputPath.toFile(), false);

        pdfRenderer.printText("Performance Report for " + instanceName(instance),
            PdfRenderer.titleFont);
        pdfRenderer.println();
        pdfRenderer.println();
        pdfRenderer.println();

        InstanceReport instanceReport = new InstanceReport(pdfRenderer);

        List<PipelineInstanceNode> instanceNodes = pipelineInstanceOperations()
            .instanceNodes(instance);
        List<PipelineInstanceNode> nodesToProcess = nodes == null ? instanceNodes
            : selectNodes(instanceNodes);

        AppendixReport appendixReport = new AppendixReport(pdfRenderer);

        addTableOfContents(pdfRenderer, instanceReport, nodesToProcess, appendixReport);

        instanceReport.generateReport(instance, nodesToProcess);

        if (nodesToProcess.isEmpty()) {
            System.err.println("No instance nodes found for instance = " + instanceId);
            System.exit(-1);
        } else {
            pdfRenderer.newPage();

            for (PipelineInstanceNode node : nodesToProcess) {
                new NodeReport(pdfRenderer).generateReport(node);
            }
        }
        pdfRenderer.newPage();

        appendixReport.generateReport(instance, nodesToProcess);

        pdfRenderer.close();

        log.info("Writing report to {}...done", outputPath.toString());

        return outputPath;
    }

    private String instanceName(PipelineInstance instance) {
        return instance.getPipeline().getName()
            + (StringUtils.isBlank(instance.getName()) ? "" : ": " + instance.getName()) + " ("
            + instance.getId() + ")";
    }

    private void addTableOfContents(PdfRenderer pdfRenderer, InstanceReport instanceReport,
        List<PipelineInstanceNode> nodesToProcess, AppendixReport appendixReport) {

        pdfRenderer.printText("Table of Contents", PdfRenderer.h1Font);
        pdfRenderer.println();

        String prefix = "    ";
        for (String heading : instanceReport.headings()) {
            pdfRenderer.printText(prefix + heading, PdfRenderer.bodyFont);
        }
        pdfRenderer.printText(prefix + "Pipeline Steps", PdfRenderer.bodyFont);
        for (PipelineInstanceNode node : nodesToProcess) {
            pdfRenderer.printText(prefix + prefix + node.getPipelineStepName(),
                PdfRenderer.bodyFont);
        }
        for (String heading : AppendixReport.HEADINGS) {
            pdfRenderer.printText(prefix + heading, PdfRenderer.bodyFont);
        }

        pdfRenderer.println();
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

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }
}
