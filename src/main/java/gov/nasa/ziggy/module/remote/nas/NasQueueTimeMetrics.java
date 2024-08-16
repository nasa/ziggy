/*
 * Copyright (C) 2022-2024 United States Government as represented by the Administrator of the
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

package gov.nasa.ziggy.module.remote.nas;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.remote.RemoteNodeDescriptor;
import gov.nasa.ziggy.module.remote.SupportedRemoteClusters;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * Manages the queue time metrics ("runout time" and "expansion") that Pleiades produces, and
 * associates same with their respective architectures. This is done by running a NAS command ("qs
 * -r") and processing the resulting output with grep and with text processing strings. Because the
 * qs command takes many seconds and changes only slowly, the qs command is only run if the time
 * since the last run exceeds a tolerance. Similarly, since all pipeline tasks can use the same
 * instance of {@link NasQueueTimeMetrics}, a singleton instance is provided.
 *
 * @author PT
 */
public class NasQueueTimeMetrics {

    private static final Logger log = LoggerFactory.getLogger(NasQueueTimeMetrics.class);

    private static NasQueueTimeMetrics instance = new NasQueueTimeMetrics();

    private Map<String, RemoteNodeDescriptor> remoteNodeNameMap = new HashMap<>();

    private Map<RemoteNodeDescriptor, Double> queueDepthMap = new HashMap<>();
    private Map<RemoteNodeDescriptor, Double> queueTimeMap = new HashMap<>();
    private long timeOfLastUpdate = 0;
    private String directorate;

    private final static long UPDATE_INTERVAL_MILLIS = 60 * 60 * 1000; // 1 hour
    private final static String DEFAULT_DIRECTORATE = "SMD";
    private final static String QS_RESULTS_DEFAULT_DESTINATION = "qs.csv";

    public void populate() {
        populate(fullDestination(QS_RESULTS_DEFAULT_DESTINATION));
    }

    private String fullDestination(String qsResultsDestination) {
        File tmpDir = new File("/tmp", System.getenv("USER"));
        tmpDir.mkdirs();
        return new File(tmpDir, qsResultsDestination).getAbsolutePath();
    }

    // This method has to stay public so that the unit tests will run correctly.
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void populate(String qsResultsDestination) {

        if (remoteNodeNameMap.isEmpty()) {
            List<RemoteNodeDescriptor> descriptors = RemoteNodeDescriptor
                .descriptorsSortedByCost(SupportedRemoteClusters.NAS);
            for (RemoteNodeDescriptor descriptor : descriptors) {
                remoteNodeNameMap.put(descriptor.getNodeName().substring(0, 3), descriptor);
            }
        }
        if (StringUtils.isBlank(directorate)) {
            directorate = ZiggyConfiguration.getInstance()
                .getString(PropertyName.REMOTE_NASA_DIRECTORATE.property(), DEFAULT_DIRECTORATE)
                .toUpperCase();
        }
        Long currentTime = System.currentTimeMillis();
        if (currentTime - timeOfLastUpdate > UPDATE_INTERVAL_MILLIS) {

            String qsCmd = "qs --onlyRunout --runout " + qsResultsDestination;
            ExternalProcess p = externalProcess(qsCmd);
            p.logStdErr(true);
            p.logStdOut(true);
            p.writeStdErr(true);
            p.writeStdOut(true);
            try {
                p.run(true, 0);
            } catch (Exception e) {
                // The qs command is potentially unreliable and prone to
                // intermittent failures that appear here as runtime exceptions.
                // In this case, we don't want execution to stop because we can
                // use the most recent prior run of qs to perform the calculations.
                log.error("Error when attempting to run qs command", e);
            }
            Set<ArchitectureQueueTimeMetrics> allMetrics = parseQsCsv(qsResultsDestination,
                directorate);
            for (ArchitectureQueueTimeMetrics metrics : allMetrics) {
                log.info("Architecture string: " + metrics.archName);
                RemoteNodeDescriptor descriptor = remoteNodeNameMap.get(metrics.archName);
                if (descriptor != null) {
                    queueDepthMap.put(descriptor, metrics.runoutTime);
                    queueTimeMap.put(descriptor, metrics.expansion);
                }
            }
            timeOfLastUpdate = currentTime;
        }
    }

    // Package-scoped external process method for unit testing
    ExternalProcess externalProcess(String command) {
        return ExternalProcess.simpleExternalProcess(command);
    }

    // Package-scoped method to replace the singleton instance with one for testing
    static void setSingletonInstance(NasQueueTimeMetrics instance) {
        NasQueueTimeMetrics.instance = instance;
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private Set<ArchitectureQueueTimeMetrics> parseQsCsv(String file, String nasaDivision) {

        Set<ArchitectureQueueTimeMetrics> metricsAllArchitectures = new HashSet<>();
        CSVFormat format = CSVFormat.Builder.create(CSVFormat.EXCEL)
            .setRecordSeparator("\n")
            .build();
        try (CSVParser parser = format.parse(
            new InputStreamReader(new FileInputStream(new File(file)), ZiggyFileUtils.ZIGGY_CHARSET))) {
            List<CSVRecord> csvRecords = parser.getRecords();
            CSVRecord divisionsRecord = csvRecords.get(0);
            List<String> divisions = new ArrayList<>();
            int iMax = divisionsRecord.size();

            for (int i = 0; i < iMax; i++) {
                String recordEntry = divisionsRecord.get(i);
                if (!StringUtils.isBlank(recordEntry)) {
                    divisions.add(recordEntry);
                }
            }
            int divisionIndex = divisions.indexOf(nasaDivision);
            int runoutIndex = 3 + 4 * divisionIndex;
            int expansionIndex = 4 + 4 * divisionIndex;
            int archIndex = 0;
            for (int i = 2; i < csvRecords.size(); i++) {
                CSVRecord archRecord = csvRecords.get(i);
                metricsAllArchitectures
                    .add(new ArchitectureQueueTimeMetrics(archRecord.get(archIndex),
                        archRecord.get(runoutIndex), archRecord.get(expansionIndex)));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to parse file " + file.toString(), e);
        }
        return metricsAllArchitectures;
    }

    public double getQueueDepth(RemoteNodeDescriptor descriptor) {
        return queueDepthMap.get(descriptor) != null ? queueDepthMap.get(descriptor) : Double.NaN;
    }

    public double getQueueTime(RemoteNodeDescriptor descriptor) {
        return queueTimeMap.get(descriptor) != null ? queueTimeMap.get(descriptor) : Double.NaN;
    }

    public static synchronized double queueDepth(RemoteNodeDescriptor descriptor) {
        instance.populate();
        return instance.getQueueDepth(descriptor);
    }

    public static synchronized double queueTime(RemoteNodeDescriptor descriptor) {
        instance.populate();
        return instance.getQueueTime(descriptor);
    }

    // This allows us to place a mocked instance into the system for testing purposes.
    public static void setInstance(NasQueueTimeMetrics testInstance) {
        instance = testInstance;
    }

    /**
     * Convenience class for returning the name, runout, and expansion values for an architecture in
     * a single object.
     *
     * @author PT
     */
    private static class ArchitectureQueueTimeMetrics {

        private final String archName;
        private final double runoutTime;
        private final double expansion;

        public ArchitectureQueueTimeMetrics(String archString, String runoutString,
            String expansionString) {

            archName = archString;
            runoutTime = Double.parseDouble(runoutString);
            expansion = Double.parseDouble(expansionString);
        }

        @Override
        public int hashCode() {
            return Objects.hash(archName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ArchitectureQueueTimeMetrics other = (ArchitectureQueueTimeMetrics) obj;
            if (!Objects.equals(archName, other.archName)) {
                return false;
            }
            return true;
        }
    }

    // The main() method is mainly for testing, to ensure that the system to issue the
    // command and parse the outputs works correctly, though it may prove to be useful in
    // operations as well.
    public static void main(String[] args) {

        System.out.println("Retrieving queue time metrics...");
        NasQueueTimeMetrics metrics = new NasQueueTimeMetrics();
        metrics.populate();
        System.out.println("Queue time metrics retrieved");

        System.out.println("ARCHNAME   RUNOUT   EXPANSION");
        for (Map.Entry<String, RemoteNodeDescriptor> entry : metrics.remoteNodeNameMap.entrySet()) {
            String archName = entry.getKey();
            RemoteNodeDescriptor descriptor = entry.getValue();
            Double runout = metrics.getQueueDepth(descriptor);
            Double expansion = metrics.getQueueTime(descriptor);
            System.out.println(archName + "  " + runout + "  " + expansion);
        }
    }
}
