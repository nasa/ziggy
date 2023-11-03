/*
 * Copyright (C) 2022-2023 United States Government as represented by the Administrator of the
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

package gov.nasa.ziggy.services.database;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.EnumSet;

import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.hbm2ddl.SchemaExport.Action;
import org.hibernate.tool.schema.TargetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * This class provides a command-line interface to the Hibernate {@link SchemaExport} class. Specify
 * either the {@code --create} or {@code --drop} option, a {@code --output=<filename>} option, and
 * define the Java property {@code hibernate.dialect}. For example:
 *
 * <pre>
 * java -Dhibernate.dialect=org.hibernate.dialect.HSQLDialect gov.nasa.ziggy.services.database.ZiggySchemaExport --create --output=build/schema/ddl.hsqldb-create.sql
 * java -Dhibernate.dialect=org.hibernate.dialect.PostgreSQLDialect gov.nasa.ziggy.services.database.ZiggySchemaExport --create --output=build/schema/ddl.postgresql-create.sql
 * </pre>
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
public class ZiggySchemaExport {
    private static final Logger log = LoggerFactory.getLogger(ZiggySchemaExport.class);

    public ZiggySchemaExport() {
    }

    @AcceptableCatchBlock(rationale = Rationale.SYSTEM_EXIT)
    public static void main(String[] args) {
        try {
            boolean drop = false;
            boolean create = false;
            boolean halt = true;
            String outFile = null;
            boolean format = true;

            for (String arg : args) {
                if (arg.equals("--drop")) {
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

            SchemaExport schemaExport = new SchemaExport().setHaltOnError(halt)
                .setOutputFile(outFile)
                .setDelimiter(";");

            if (format) {
                schemaExport.setFormat(true);
            }

            Action action = Action.NONE;
            if (create) {
                action = drop ? Action.BOTH : Action.CREATE;
            } else if (drop) {
                action = Action.DROP;
            }

            MetadataSources metadata = new MetadataSources(
                new StandardServiceRegistryBuilder().build());
            for (Class<?> clazz : ZiggyHibernateConfiguration.annotatedClasses()) {
                metadata.addAnnotatedClass(clazz);
            }

            Files.deleteIfExists(Paths.get(outFile));

            schemaExport.execute(EnumSet.of(TargetType.SCRIPT), action, metadata.buildMetadata());
        } catch (Exception e) {
            log.error("Error creating schema ", e);
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
