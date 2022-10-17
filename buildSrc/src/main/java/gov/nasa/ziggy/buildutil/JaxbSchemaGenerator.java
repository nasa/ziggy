/**
 * 
 */
package gov.nasa.ziggy.buildutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

/**
 * Generate an XML schema from the JAXB annotations.  Sun/Oracle used to
 * distribue a command line tool that would just do this.  I'm not sure how it's
 * superior to have everyone reinvent this.
 * 
 * @author Sean McCauliff
 *
 */
public final class JaxbSchemaGenerator {
        

    /**
     * JAXB requires this so that it knows how to write the schema file.
     *
     */
    public static final class FileSchemaOutputResolver extends SchemaOutputResolver {

        private String fname;
        private final File dir;
        
        /**
         * Specify the file name this is to be written.
         * @param fname This may be null.
         * @param dir The destination directory
         */
        public FileSchemaOutputResolver(String fname, File destDir) {
            this.fname = fname;
            this.dir = destDir;
        }
        
        public FileSchemaOutputResolver(File destDir) {
            this.dir = destDir;
            this.fname = null;
        }
        
        
        @Override
        public Result createOutput(String namespaceURI, String suggestedFileName) throws IOException {
            if (fname == null) {
                fname = suggestedFileName;
            }
            File file = new File(suggestedFileName);
            StreamResult result = new StreamResult(file);
            return result;
        }
        
        public File getDestinationFile() {
            return new File(dir, fname);
        }

    }
    
    public static String parseSchemaOutput(String schemaString) throws IOException {
        List<String> linesa = null;
        try (BufferedReader reader = new BufferedReader(new StringReader(schemaString))) {
            linesa = reader.lines().collect(Collectors.toList());
        }
        
        final List<String> lines = linesa;
        
        //Identify the schema header that we want.
        int[] schemaBlockToUse = null;
        try (IntStream goodSchemaBlocksStream = 
                IntStream.range(0, lines.size())
                         .parallel()
                         .filter( i -> lines.get(i).startsWith("<xs:schema ") &&
                                       lines.get(i).contains("targetNamespace"))) {
            schemaBlockToUse = goodSchemaBlocksStream.toArray();
            if (schemaBlockToUse.length != 1) {
                throw new IllegalStateException("Can't determine correct schema block. Number of good schema blocks found: " 
                + schemaBlockToUse.length + ".\n" 
                + lines.stream()
                      .filter(line -> line.contains("schema"))
                      .collect(Collectors.joining(", ")));
            }
        }
        String schemaStartBlock = lines.get(schemaBlockToUse[0]);
          
        //Clean all the lines that we don't want.  This searches in parallel for all the
        //lines that need to be removed.  This can't filter in parallel because
        //parallel filtering won't preserve ordering.
        IntStream.range(0, lines.size())
                 .parallel()
                 .filter(lineIndex -> lines.get(lineIndex).contains("<xs:schema") ||
                        lines.get(lineIndex).startsWith("</xs:schema>") ||
                        lines.get(lineIndex).contains("<xs:import") ||
                        lines.get(lineIndex).contains("<?xml version="))
                 .boxed()
                 .sorted(Comparator.reverseOrder())  //  Boxing!!!!!! !! ! !1
                 .forEachOrdered( lineIndex -> { lines.remove(lineIndex.intValue()); });
          
        //paste together all the bits.
        StringBuilder bldr = new StringBuilder(lines.size() * 40);
        bldr.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
        bldr.append(schemaStartBlock);
          
        lines.forEach(line -> bldr.append(line).append("\n"));
        bldr.append("</xs:schema>\n");
        return bldr.toString();
    }
    /**
     * Write schema to a StringWriter.
     *
     */
    public static final class StringSchemaOutputResolver extends SchemaOutputResolver {

        private final StringWriter stringWriter = new StringWriter();
        
        @Override
        public Result createOutput(String namespaceUri, String suggestedFileName)
                throws IOException {

            StreamResult result = new StreamResult(stringWriter);
            result.setSystemId(suggestedFileName);
            return result;
        }
    
        /**
         * The complexity behind this function is to allow for generating the schema in a single file.
         */
        public String getSchema() {
            try {
                return parseSchemaOutput(stringWriter.toString());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        
    }
    
    public JaxbSchemaGenerator() {
        
    }
    
    /**
     * 
     * @param packageName  e.g. "gov.nasa.kepler.hibernate.dv"  This package name should contain
     * classes with Jaxb annotations.  There either needs to be a JAXB ObjectFactor class or there
     * should be a jaxb.index file in the package.  This is just a plain text file that lists
     * the SimpleName of all the classes in the package with annotations.
     * @return
     * @throws IOException 
     * @throws JAXBException 
     */
    public String generateSchemaFromAnnotatedClasses(String packageName) 
            throws IOException, JAXBException {
        JAXBContext context = JAXBContext.newInstance(packageName);
        return generateSchema(context);
    }
    
    public String generateSchemaFromAnnotatedClasses(Class<?>... annotatedClasses) 
            throws IOException, JAXBException {
        JAXBContext context = JAXBContext.newInstance(annotatedClasses);
        return generateSchema(context);
    }
    
    private String generateSchema(JAXBContext context) throws IOException {
        StringSchemaOutputResolver outputResolver = new StringSchemaOutputResolver();
        context.generateSchema(outputResolver);
        return outputResolver.getSchema();
    }

    /**
     * @param packageName. 
     */
    public static void generateXmlSchema(String packageName, File destinationFile) throws IOException, JAXBException {
        JaxbSchemaGenerator schemaGenerator = new JaxbSchemaGenerator();
        String schema = schemaGenerator.generateSchemaFromAnnotatedClasses(packageName);
        try (FileWriter fout = new FileWriter(destinationFile)) {
            fout.write(schema);
        }
    }
    
    public static void main(String[] argv) throws Exception {
        System.out.println("Generating XML schema for JAXB annotated classes in package " + argv[0]);
        generateXmlSchema(argv[0], new File(argv[1]));
    }
}
