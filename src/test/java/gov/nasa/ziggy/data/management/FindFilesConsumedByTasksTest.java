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

package gov.nasa.ziggy.data.management;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.data.datastore.DatastoreOperations;

/**
 * Performs an execution speed test on the findConsumedBy() method in
 * {@link DatastoreProducerConsumerCrud}.
 *
 * @author PT
 */
public class FindFilesConsumedByTasksTest {

    public static final long MAX_ALLOWED_EXECUTION_TIME_MILLIS = 60_000;

    public static class TestOperations extends DatastoreOperations {

        private List<String> findConsumedBy(Collection<Long> consumerIds) {
            return performTransaction(
                () -> new DatastoreProducerConsumerCrud().findConsumedBy(consumerIds));
        }

        private void createOrUpdateProducer(Collection<Path> files) {
            performTransaction(
                () -> new DatastoreProducerConsumerCrud().createOrUpdateProducer(null, files));
        }

        private void addConsumer(long consumer, Collection<Path> files) {
            performTransaction(() -> {
                DatastoreProducerConsumerCrud crud = new DatastoreProducerConsumerCrud();
                List<DatastoreProducerConsumer> records = new DatastoreProducerConsumerCrud()
                    .retrieveByFilename(files);
                records.stream().forEach(s -> s.addConsumer(consumer));
                records.stream().forEach(s -> crud.merge(s));
            });
        }
    }

    public static void main(String[] args) {
        int recordCount = Integer.parseInt(args[0]);
        List<Long> consumers = List.of(100L, 200L);
        Map<Long, List<Path>> filesByConsumer = new HashMap<>();
        for (long consumer : consumers) {
            filesByConsumer.put(consumer, new ArrayList<>());
        }
        System.out
            .println("Constructing " + recordCount + " records in producer-consumer table...");
        long startTime = System.currentTimeMillis();
        for (int recordIndex = 0; recordIndex < recordCount; recordIndex++) {
            int consumerIndex = recordIndex % consumers.size();
            String filename = "test-" + Integer.toString(recordIndex);
            Path file = Paths.get(filename);
            filesByConsumer.get(consumers.get(consumerIndex)).add(file);
        }
        TestOperations operations = new TestOperations();
        for (Map.Entry<Long, List<Path>> entry : filesByConsumer.entrySet()) {
            operations.createOrUpdateProducer(entry.getValue());
            operations.addConsumer(entry.getKey(), entry.getValue());
        }
        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out
            .println("Constructing " + recordCount + " records in producer-consumer table...done");
        System.out.println("Record construction took " + elapsedTime + " milliseconds");

        System.out.println("Retrieving records...");
        startTime = System.currentTimeMillis();
        List<String> retrievedFiles = operations.findConsumedBy(consumers);
        if (retrievedFiles.size() != recordCount) {
            System.out
                .println("Only " + retrievedFiles.size() + " out of " + recordCount + " returned");
            System.exit(1);
        }
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Retrieving records...done");
        System.out.println("Retrieval of " + retrievedFiles.size() + " records took " + elapsedTime
            + " milliseconds");
        String message = elapsedTime <= MAX_ALLOWED_EXECUTION_TIME_MILLIS ? "pass" : "fail";
        System.out.println(message);
    }
}
