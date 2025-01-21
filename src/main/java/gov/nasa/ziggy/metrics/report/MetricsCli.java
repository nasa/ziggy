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
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricsOperations;
import gov.nasa.ziggy.util.TimeRange;

/**
 * Command-line interface for metrics dumping and reporting
 *
 * @author Todd Klaus
 */
public class MetricsCli {

    private MetricsOperations metricsOperations = new MetricsOperations();

    public MetricsCli() {
    }

    private void processCommand(String[] args) {
        String command = args[0];
        if (command.equals("available") || command.equals("a")) {
            dumpAvailableTypes();
        } else if (command.equals("dump") || command.equals("d")) {
            dumpMetricsFileCommand(args);
        } else if (command.equals("report") || command.equals("r")) {
            generateMetricsReportCommand(args);
        } else {
            handleError("Unknown command: " + printCommandLine(args));
        }
        System.exit(0);
    }

    private void generateMetricsReportCommand(String[] args) {
        if (args.length == 2) {
            File file = new File(args[1]);
            if (!file.exists() || !file.isDirectory()) {
                handleError("Specified file is not a directory or does not exist: " + file);
            }

            InstanceMetricsReport report = new InstanceMetricsReport(file);
            report.generateReport();
        } else {
            handleError("FILE must be specified");
        }
    }

    private void dumpAvailableTypes() {

        List<MetricType> availableTypes = metricsOperations().metricTypes();
        System.out.println("Found " + availableTypes.size() + " metric types.");

        for (MetricType type : availableTypes) {
            TimeRange dateRange = metricsOperations().timestampRange(type);
            System.out.println(type + ": [" + dateRange.getStartTimestamp() + " through "
                + dateRange.getEndTimestamp() + "]");
        }
    }

    private void dumpMetricsFileCommand(String[] args) {
        if (args.length == 2) {
            File file = new File(args[1]);
            if (!file.exists() || !file.isFile()) {
                handleError("Specified file is not a regular file or does not exist: " + file);
            }

            Map<String, Metric> metrics = Metric.loadMetricsFromSerializedFile(file);

            System.out.println(file + " contains " + metrics.size() + " metrics.");

            for (Metric metric : metrics.values()) {
                System.out.println(metric.getName() + ": " + metric.toString());
            }
        } else {
            handleError("FILE must be specified");
        }
    }

    private void handleError(String message) {
        System.err.println(message);
        usage();
        System.exit(-1);
    }

    private String printCommandLine(String[] args) {
        StringBuilder sb = new StringBuilder();
        for (String arg : args) {
            sb.append(arg + " ");
        }
        return sb.toString();
    }

    private static void usage() {
        System.out.println("metrics COMMAND ARGS");
        System.out.println("  Examples:");
        System.out
            .println("    a[vailable] : display available types and time ranges for each type.");
        System.out.println(
            "    d[ump] FILE : Dump the contents of a serialized metrics file (metrics.ser)");
        System.out.println(
            "    r[eport] DIR : Generate an aggregate report (PDF) from a tree of metrics.ser files");
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            usage();
            System.exit(-1);
        }

        MetricsCli cli = new MetricsCli();
        cli.processCommand(args);
    }

    MetricsOperations metricsOperations() {
        return metricsOperations;
    }
}
