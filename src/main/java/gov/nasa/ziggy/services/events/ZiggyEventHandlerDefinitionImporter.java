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

package gov.nasa.ziggy.services.events;

import java.io.File;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Extremely simple class that imports {@link ZiggyEventHandler} definitions into the database. The
 * importer will verify that each handler fires a defined pipeline and that handlers that define a
 * reset node have selected a node that's defined for their pipeline.
 *
 * @author PT
 */
public class ZiggyEventHandlerDefinitionImporter {

    private static final Logger log = LoggerFactory
        .getLogger(ZiggyEventHandlerDefinitionImporter.class);

    private ValidatingXmlManager<ZiggyEventHandlerFile> xmlManager;
    private File[] files;
    private ZiggyEventOperations ziggyEventOperations = new ZiggyEventOperations();
    private PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();

    public ZiggyEventHandlerDefinitionImporter(String[] filenames) {
        files = new File[filenames.length];
        for (int i = 0; i < filenames.length; i++) {
            files[i] = new File(filenames[i]);
        }
        xmlManager = new ValidatingXmlManager<>(ZiggyEventHandlerFile.class);
    }

    public ZiggyEventHandlerDefinitionImporter(File[] files) {
        this.files = files;
        xmlManager = new ValidatingXmlManager<>(ZiggyEventHandlerFile.class);
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void importFromFiles() {

        List<String> pipelineDefinitionNames = pipelineDefinitionOperations()
            .pipelineDefinitionNames();

        for (File file : files) {
            ZiggyEventHandlerFile handlerFile = null;
            try {
                log.info("Unmarshaling file {}", file.getName());
                handlerFile = xmlManager.unmarshal(file);
            } catch (Exception e) {
                log.error("Unable to unmarshal file {}", file.getName(), e);
                continue;
            }
            log.info("Unmarshaling file {}...done", file.getName());
            for (ZiggyEventHandler handler : handlerFile.getZiggyEventHandlers()) {
                if (!pipelineDefinitionNames.contains(handler.getPipelineName())) {
                    log.error("Handler {} fires pipeline {}, which is not defined",
                        handler.getName(), handler.getPipelineName());
                    log.error("Not persisting handler {}", handler.getName());
                    continue;
                }
                try {
                    log.info("Persisting handler {}", handler.getName());
                    ziggyEventOperations().mergeEventHandler(handler);
                } catch (Exception e) {
                    log.error("Unable to persist handler {}", handler.getName(), e);
                    continue;
                }
                log.info("Persisting handler {}...done", handler.getName());
            }
        }
        log.info("Done importing Ziggy event handlers");
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            throw new IllegalArgumentException(
                "No files listed for import as Ziggy event handlers");
        }
        new ZiggyEventHandlerDefinitionImporter(args).importFromFiles();
    }

    ZiggyEventOperations ziggyEventOperations() {
        return ziggyEventOperations;
    }

    private PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }
}
