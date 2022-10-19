/*
 * Copyright (C) 2022 United States Government as represented by the Administrator of the National
 * Aeronautics and Space Administration. All Rights Reserved.
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

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.crud.DataFileTypeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import jakarta.xml.bind.JAXBException;

/**
 * Performs import of DataFileType and ModelType instances to the database.
 *
 * @author PT
 */
public class DataFileTypeImporter {

    private static final Logger log = LoggerFactory.getLogger(DataFileTypeImporter.class);

    private List<String> filenames;
    private boolean dryrun;
    private DataFileTypeCrud dataFileTypeCrud;
    private ModelCrud modelCrud;
    private int dataFileImportedCount;
    private int modelFileImportedCount;
    private ValidatingXmlManager<DatastoreConfigurationFile> xmlManager;

    // The following are instantiated so that unit tests that rely on them don't fail
    private static List<String> databaseDataFileTypeNames = new ArrayList<>();
    private static Set<String> databaseModelTypes = new HashSet<>();

    public DataFileTypeImporter(List<String> filenames, boolean dryrun) {
        this.filenames = filenames;
        this.dryrun = dryrun;
        try {
            xmlManager = new ValidatingXmlManager<>(DatastoreConfigurationFile.class);
		} catch (InstantiationException | IllegalAccessException | SAXException | JAXBException
				| IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new PipelineException(
                "Unable to construct ValidatingXmlManager for class DatastoreConfigurationFile", e);
        }
    }

    /**
     * Perform the import from all XML files. The importer will skip any file that fails to validate
     * or cannot be parsed, will skip any DataFileType instance that fails internal validation, and
     * will skip any DataFileType that has the name of a type that is already in the database; all
     * other DataFileTypes will be imported. If any duplicate names are present in the set of
     * DataFileType instances to be imported, none will be imported. The import also imports model
     * definitions.
     *
     * @throws JAXBException
     */
    public void importFromFiles() throws JAXBException {

        List<DataFileType> dataFileTypes = new ArrayList<>();
        List<ModelType> modelTypes = new ArrayList<>();
        for (String filename : filenames) {
            File file = new File(filename);
            if (!file.exists() || !file.isFile()) {
                log.warn("File " + filename + " is not a regular file");
                continue;
            }

            // open and read the XML file
            log.info("Reading from " + filename);
            DatastoreConfigurationFile configDoc = null;
            try {
                configDoc = xmlManager.unmarshal(file);
            } catch (Exception e) {
                log.warn("Unable to parse configuration file " + filename, e);
                continue;
            }

            log.info("Importing DataFileType definitions from " + filename);
            Set<DataFileType> dataFileTypesFromFile = configDoc.getDataFileTypes();
            List<DataFileType> dataFileTypesNotImported = new ArrayList<>();
            for (DataFileType typeXb : dataFileTypesFromFile) {
                try {
                    typeXb.validate();
                } catch (Exception e) {
                    log.warn("Unable to validate data file type definition " + typeXb.getName(), e);
                    dataFileTypesNotImported.add(typeXb);
                    continue;
                }
                if (databaseDataFileTypeNames.contains(typeXb.getName())) {
                    log.warn("Not importing data file type definition \"" + typeXb.getName()
                        + "\" due to presence of existing type with same name");
                    dataFileTypesNotImported.add(typeXb);
                    continue;
                }
            }
            dataFileTypesFromFile.removeAll(dataFileTypesNotImported);
            log.info("Imported " + dataFileTypesFromFile.size()
                + " DataFileType definitions from file " + filename);
            dataFileTypes.addAll(dataFileTypesFromFile);

            // Now for the models
            Set<ModelType> modelTypesFromFile = configDoc.getModelTypes();
            List<ModelType> modelTypesNotImported = new ArrayList<>();
            for (ModelType modelTypeXb : modelTypesFromFile) {
                try {
                    modelTypeXb.validate();
                } catch (Exception e) {
                    log.warn("Unable to validate model type definition " + modelTypeXb.getType(),
                        e);
                    modelTypesNotImported.add(modelTypeXb);
                    continue;
                }
                if (databaseModelTypes.contains(modelTypeXb.getType())) {
                    log.warn("Not importing model type definition \"" + modelTypeXb.getType()
                        + "\" due to presence of existing type with same name");
                    modelTypesNotImported.add(modelTypeXb);
                    continue;
                }
            }

            modelTypesFromFile.removeAll(modelTypesNotImported);
            log.info("Imported " + modelTypesFromFile.size() + " ModelType definitions from file "
                + filename);
            modelTypes.addAll(modelTypesFromFile);

        } // end loop over files

        List<String> dataFileTypeNames = dataFileTypes.stream()
            .map(DataFileType::getName)
            .collect(Collectors.toList());
        Set<String> uniqueDataFileTypeNames = new HashSet<>();
        uniqueDataFileTypeNames.addAll(dataFileTypeNames);
        if (dataFileTypeNames.size() != uniqueDataFileTypeNames.size()) {
            throw new IllegalStateException(
                "Unable to persist data file types due to duplicate names");
        }
        dataFileImportedCount = dataFileTypes.size();
        List<String> modelTypeNames = modelTypes.stream()
            .map(ModelType::getType)
            .collect(Collectors.toList());
        Set<String> uniqueModelTypeNames = new HashSet<>();
        uniqueModelTypeNames.addAll(modelTypeNames);
        if (modelTypeNames.size() != uniqueModelTypeNames.size()) {
            throw new IllegalStateException("Unable to persist model types due to duplicate names");
        }
        modelFileImportedCount = modelTypes.size();
        if (!dryrun) {
            log.info(
                "Persisting to datastore " + dataFileTypes.size() + " DataFileType definitions");
            dataFileTypeCrud().create(dataFileTypes);
            log.info("Persisting to datastore " + modelTypes.size() + " model definitions");
            modelCrud().create(modelTypes);
            log.info("Persist step complete");
        } else {
            log.info("Not persisting because of dryrun option");
        }

    }

    public static void main(String[] args) throws JAXBException {

        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("dryrun", false,
            "Parses and creates objects but does not persist to database");
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Illegal argument: " + e.getMessage());
        }
        String[] filenames = cmdLine.getArgs();
        boolean dryrun = cmdLine.hasOption("dryrun");
        DataFileTypeImporter importer = new DataFileTypeImporter(Arrays.asList(filenames), dryrun);

        DatabaseTransactionFactory.performTransaction(() -> {
            databaseDataFileTypeNames = new DataFileTypeCrud().retrieveAllNames();
            databaseModelTypes = new ModelCrud().retrieveModelTypeMap().keySet();
            return null;
        });
        if (!dryrun) {
            DatabaseTransactionFactory.performTransaction(() -> {
                importer.importFromFiles();
                return null;
            });
        } else {
            importer.importFromFiles();
        }
    }

// default scope for mocking in unit tests
    DataFileTypeCrud dataFileTypeCrud() {
        if (dataFileTypeCrud == null) {
            dataFileTypeCrud = new DataFileTypeCrud();
        }
        return dataFileTypeCrud;
    }

    ModelCrud modelCrud() {
        if (modelCrud == null) {
            modelCrud = new ModelCrud();
        }
        return modelCrud;
    }

    int getDataFileImportedCount() {
        return dataFileImportedCount;
    }

    int getModelFileImportedCount() {
        return modelFileImportedCount;
    }

}
