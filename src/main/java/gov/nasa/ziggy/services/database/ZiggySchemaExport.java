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

package gov.nasa.ziggy.services.database;

import org.hibernate.cfg.Configuration;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a command-line interface to the Hibernate {@link SchemaExport} class which
 * uses {@link ZiggyHibernateConfiguration} as the configuration source.
 * <p>
 * Why didn't I just use the standard hbm2ddl ant task (which calls SchemaExport directly) to do
 * this instead of writing custom code to do it? Because the standard tools assume you have a
 * hibernate.cfg.xml file that explicitly lists all of your entity class. Rather than deal with that
 * maintenence headache, I wrote my own configurator {@link ZiggyHibernateConfiguration} which scans
 * the classpath for classes with Hibernate annotations and adds them to the configuration
 * dynamically. Hibernate does provide some similar code that scans the classpath, but it is
 * designed to work with JPA-style configuration files (persistence.xml) which has to be bundled in
 * a jar file, so it's difficult to support multiple database configurations.
 *
 * @author Todd Klaus
 */
public class ZiggySchemaExport {
    private static final Logger log = LoggerFactory.getLogger(ZiggySchemaExport.class);

    public ZiggySchemaExport() {
    }

    public static void main(String[] args) {
        try {
            boolean echoToStdOut = false;
            boolean drop = false;
            boolean create = false;
            boolean halt = true;
            boolean export = false;
            String outFile = null;
            boolean format = true;

            for (String arg : args) {
                if (arg.equals("--verbose")) {
                    echoToStdOut = true;
                } else if (arg.equals("--drop")) {
                    drop = true;
                } else if (arg.equals("--create")) {
                    create = true;
                } else if (arg.equals("--nohaltonerror")) {
                    halt = false;
                } else if (arg.startsWith("--output=")) {
                    outFile = arg.substring(9);
                } else if (arg.equals("--noformat")) {
                    format = false;
                } else {
                    System.err.println("unexpected arg: " + arg);
                    System.exit(-1);
                }
            }

            Configuration hibernateConfig = ZiggyHibernateConfiguration
                .buildHibernateConfiguration();

            SchemaExport se = new SchemaExport(hibernateConfig).setHaltOnError(halt)
                .setOutputFile(outFile)
                .setDelimiter(";");

            if (format) {
                se.setFormat(true);
            }

            se.execute(echoToStdOut, export, drop, create);
        } catch (Exception e) {
            log.error("Error creating schema ", e);
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
