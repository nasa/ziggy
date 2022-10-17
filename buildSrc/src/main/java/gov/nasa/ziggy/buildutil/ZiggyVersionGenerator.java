package gov.nasa.ziggy.buildutil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.Internal;

import com.google.common.collect.ImmutableList;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates version info in the generated file <code>ZiggyVersion.java</code.
 * Note that several Git commands are used here, with command-line options to
 * set the number of characters of the commit hash to use. The number is set to
 * 10, because that is currently 1 more than necessary to distinguish all commit
 * hashes to date. One command to determine the maximum hash length in use is:
 *
 * <pre>$ git rev-list --all --abbrev=0 --abbrev-commit | awk '{print length()}' | sort -n | uniq -c
 * 1040 4
 * 7000 5
 * 1149 6
 *   68 7
 *    8 8
 *    1 9
 * </pre>
 *
 */
public class ZiggyVersionGenerator extends TessExecTask {

    private static final Logger log = LoggerFactory.getLogger(ZiggyVersionGenerator.class);
    private static final String MAC_OS_X_OS_NAME = "Mac OS X";

    public File outputFile;
    public final String dateFormat = "dd-MMM-yyyy HH:mm:ss";
    private String osType;

    public void generateFile(BufferedWriter out) throws IOException, InterruptedException {

        osType = System.getProperty("os.name");
        log.debug("OS Type: " + osType);
        Configuration config = new Configuration();
        config.setClassForTemplateLoading(this.getClass(), "/");
        config.setDefaultEncoding("UTF-8");
        config.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        VersionInfo versionInfo = new VersionInfo();
        versionInfo.setBuildDate(getBuildDate());
        versionInfo.setSoftwareVersion(getGitRelease());
        versionInfo.setBranch(getGitBranch());
        versionInfo.setRevision(getGitRevision());

        try {
            config.getTemplate("ZiggyVersion.java.ftlh").process(versionInfo, out);
        } catch (TemplateException e) {
            throw new IllegalStateException("Error processing template", e);
        }
    }

    public List<String> processOutput(List<String> command)
        throws IOException, InterruptedException {

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();
        List<String> lines = new ArrayList<>();

        BufferedReader bufferedReader = new BufferedReader(
            new InputStreamReader(process.getInputStream()));

        for (;;) {
            String line = bufferedReader.readLine();
            if (line == null) {
                break;
            }

            lines.add(line);
        }

        process.waitFor();
        return lines;
    }

    @Internal
    public String getGitRevision() throws IOException, InterruptedException {
        if (osType.equals(MAC_OS_X_OS_NAME)) {
            return "Not Supported";
        }
        List<String> cmd = ImmutableList.of("git", "rev-parse", "--short=10", "HEAD");
        List<String> output = processOutput(cmd);
        return output.get(output.size() - 1);
    }

    @Internal
    public String getGitBranch() throws IOException, InterruptedException {
        if (osType.equals(MAC_OS_X_OS_NAME)) {
            return "Not Supported";
        }
        List<String> cmd = ImmutableList.of("git", "rev-parse", "--abbrev-ref", "HEAD");
        List<String> output = processOutput(cmd);
        return output.get(output.size() - 1);
    }

    @Internal
    public String getGitRelease() throws IOException, InterruptedException {
        if (osType.equals(MAC_OS_X_OS_NAME)) {
            return "Not Supported";
        }
        List<String> cmd = ImmutableList.of("git", "describe", "--always", "--abbrev=10");
        List<String> output = processOutput(cmd);
        return output.get(output.size() - 1);
    }

    @Internal
    public String getBuildDate() {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(dateFormat);
        return dateFormatter.format(new Date());
    }

    @OutputFile
    public File getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(File output) {
        outputFile = output;
    }

    @TaskAction
    public void action() throws IOException, InterruptedException {
        try (BufferedWriter output = new BufferedWriter(new FileWriter(outputFile))) {
            generateFile(output);
        }
    }

    /**
     * Holds version information in a Java bean suitable for referencing from a template.
     */
    public static class VersionInfo {

        private String buildDate;
        private String softwareVersion;
        private String revision;
        private String branch;

        public String getBuildDate() {
            return buildDate;
        }

        public void setBuildDate(String dateStr) {
            buildDate = dateStr;
        }

        public String getSoftwareVersion() {
            return softwareVersion;
        }

        public void setSoftwareVersion(String versionStr) {
            softwareVersion = versionStr;
        }

        public String getRevision() {
            return revision;
        }

        public void setRevision(String revision) {
            this.revision = revision;
        }

        public String getBranch() {
            return branch;
        }

        public void setBranch(String branch) {
            this.branch = branch;
        }
    }
}
