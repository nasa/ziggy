/*
 * Copyright (C) 2022-2023 United States Government as represented by the Administrator of the National
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

package gov.nasa.ziggy.services.config;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Merges the contents of two .properties files into one. Typically one contains the defaults and
 * the other contains overrides for a specific use.
 *
 * @author Todd Klaus
 */
public class ConfigMerge {
    private static final Logger log = LoggerFactory.getLogger(ConfigMerge.class);

    private final String basePath;
    private final String overridePath;
    private final String mergedPath;

    public ConfigMerge(String basePath, String overridePath, String mergedPath) {
        this.basePath = basePath;
        this.overridePath = overridePath;
        this.mergedPath = mergedPath;
    }

    public void merge() throws Exception {
        // Read base file
        File baseFile = new File(basePath);
        if (!baseFile.exists()) {
            throw new Exception("baseFile: " + baseFile + " does not exist");
        }

        log.info("reading base file: " + baseFile);
        PropertiesConfiguration baseConfig = new PropertiesConfiguration(baseFile);

        // Read the override file
        File overrideFile = new File(overridePath);
        if (!overrideFile.exists()) {
            throw new Exception("overrideFile: " + overrideFile + " does not exist");
        }

        log.info("reading override file: " + overrideFile);
        PropertiesConfiguration overrideConfig = new PropertiesConfiguration(overrideFile);

        // Copy the override properties to the base config
        ConfigurationUtils.copy(overrideConfig, baseConfig);

        // Write out the merged config .properties file
        File mergedFile = new File(mergedPath);
        log.info("writing merged file: " + mergedFile);
        BufferedWriter outputWriter = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(mergedFile), FileUtil.ZIGGY_CHARSET));
        baseConfig.save(outputWriter);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("USAGE: config-merge BASEPATH OVERRIDEPATH MERGEDPATH");
            System.err.println(
                "  example: config-merge skel/etc/p1.properties skel/etc/p2.properties build/etc/pe.properties");
            System.exit(-1);
        }

        try {
            String basePath = args[0];
            String overridePath = args[1];
            String mergedPath = args[2];

            ConfigMerge configMerge = new ConfigMerge(basePath, overridePath, mergedPath);
            configMerge.merge();
        } catch (Throwable e) {
            System.err.println("config-merge failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }
        System.exit(0);
    }
}
