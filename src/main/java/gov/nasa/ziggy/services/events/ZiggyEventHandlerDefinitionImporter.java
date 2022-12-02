/*
 * Copyright © 2022 United States Government as represented by the Administrator of the National
 * Aeronautics and Space Administration. All Rights Reserved.
 *
 * NASA acknowledges the SETI Institute’s primary role in authoring and producing Ziggy, a Pipeline
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

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

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

    public ZiggyEventHandlerDefinitionImporter(String[] filenames)
        throws InstantiationException, IllegalAccessException, SAXException, JAXBException {
        files = new File[filenames.length];
        for (int i = 0; i < filenames.length; i++) {
            files[i] = new File(filenames[i]);
        }
        xmlManager = new ValidatingXmlManager<>(ZiggyEventHandlerFile.class);
    }

    public ZiggyEventHandlerDefinitionImporter(File[] files)
        throws InstantiationException, IllegalAccessException, SAXException, JAXBException {
        this.files = files;
        xmlManager = new ValidatingXmlManager<>(ZiggyEventHandlerFile.class);
    }

    public void importFromFiles() {

        List<String> pipelineDefinitionNames = new PipelineDefinitionCrud()
            .retrievePipelineDefinitionNames();

        ZiggyEventCrud eventCrud = new ZiggyEventCrud();
        for (File file : files) {
            ZiggyEventHandlerFile handlerFile = null;
            try {
                log.info("Unmarshaling file " + file.getName() + "...");
                handlerFile = xmlManager.unmarshal(file);
            } catch (Exception e) {
                log.error("Unable to unmarshal file " + file.getName(), e);
                continue;
            }
            log.info("Unmarshaling file " + file.getName() + "...done");
            for (ZiggyEventHandler handler : handlerFile.getZiggyEventHandlers()) {
                if (!pipelineDefinitionNames.contains(handler.getPipelineName().getName())) {
                    log.error("Handler " + handler.getName() + " fires pipeline "
                        + handler.getPipelineName().getName() + ", which is not defined");
                    log.error("Not persisting handler " + handler.getName());
                    continue;
                }
                try {
                    log.info("Persisting handler " + handler.getName() + "...");
                    eventCrud.createOrUpdate(handler);
                } catch (Exception e) {
                    log.error("Unable to persist handler " + handler.getName(), e);
                    continue;
                }
                log.info("Persisting handler " + handler.getName() + "...done");
            }
        }
        log.info("Done importing Ziggy event handlers");
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            throw new IllegalArgumentException(
                "No files listed for import as Ziggy event handlers");
        }
        ZiggyEventHandlerDefinitionImporter importer = null;
        try {
            importer = new ZiggyEventHandlerDefinitionImporter(args);
        } catch (InstantiationException | IllegalAccessException | SAXException | JAXBException e) {
            throw new PipelineException("Unable to construct event handler importer ", e);
        }
        final ZiggyEventHandlerDefinitionImporter finalImporter = importer;
        DatabaseTransactionFactory.performTransaction(() -> {
            finalImporter.importFromFiles();
            return null;
        });
    }
}
