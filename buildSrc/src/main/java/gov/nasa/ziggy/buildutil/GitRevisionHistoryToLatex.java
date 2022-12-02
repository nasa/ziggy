package gov.nasa.ziggy.buildutil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods for dealing with latex files.
 */
public class GitRevisionHistoryToLatex { 

    private static final int LINES_PER_PAGE = 50;
    private static final int CHARACTERS_PER_LINE = 100;
    private static final ThreadLocal<SimpleDateFormat> revisionHistoryDateFormat = ThreadLocal
        .withInitial(() -> new SimpleDateFormat("d MMM yyyy"));

    /**
     * Runs "git log" on the specified document file to generate the revision history in the same
     * directory as <orig file>-revision-history.tex
     *
     * @throws Exception
     */
    public static void gitLogToLatex(String documentFileName, File destDir) throws Exception {
        try {
            System.out.println(
                "Generating revision history for latex file \"" + documentFileName + "\".");
            gitLogToLatexInternal(documentFileName, destDir);
        } catch (Throwable t) {
            // This is here because gradle -stacktrace causes this stack trace
            // to disappear.
            StringWriter stringWriter = new StringWriter();
            PrintWriter pw = new PrintWriter(stringWriter);
            t.printStackTrace(pw);
            System.out.println(stringWriter.toString());
            throw t;
        }
    }

    /**
     * Escapes stuff like $ and _.
     */
    private static String escapeLatex(String raw) {
        StringBuilder bldr = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '\\':
                    bldr.append("\\\\");
                    break;
                case '$':
                    bldr.append("\\$");
                    break;
                case '_':
                    bldr.append("\\_");
                    break;
                case '{':
                    bldr.append("\\{");
                    break;
                case '}':
                    bldr.append("\\}");
                    break;
                case '~':
                    bldr.append("\\~");
                    break;
                case '&':
                    bldr.append("\\&");
                    break;
                // TODO: test me
                case '"':
                    bldr.append("\\verb|\"|");
                    break;
                default:
                    bldr.append(c);
            }
        }
        return bldr.toString();
    }

    private static void writeErrorToLatexFile(Throwable exception, File outputFile)
        throws IOException {
        StringWriter errMsgString = new StringWriter();
        PrintWriter exceptionOutput = new PrintWriter(errMsgString);
        exception.printStackTrace(exceptionOutput);
        exceptionOutput.close();
        BufferedWriter fileWriter = new BufferedWriter(new FileWriter(outputFile));
        try {
            fileWriter.write("Error &");
            fileWriter.write(escapeLatex(errMsgString.toString()));
            fileWriter.write("\\\\\n");
        } finally {
            fileWriter.close();
        }
    }

    private static final class HistoryEntry {
        private final Date entryDate;
        private final String entry;
        private final int pageNumber;

        public HistoryEntry(Date entryDate, String entry, int pageNumber) {
            this.entryDate = entryDate;
            this.entry = entry;
            this.pageNumber = pageNumber;
        }

        void writeTo(Appendable writer) throws IOException {
            writer.append(revisionHistoryDateFormat.get().format(entryDate));
            writer.append(" & ");
            writer.append(entry);
            writer.append("\\\\\n");
        }
    }

    private static void gitLogToLatexInternal(String documentFileName, File destDir)
        throws ParseException, IOException, InterruptedException {
        String[] command = new String[] { "git", "log", "--follow", documentFileName };

        File documentFile = new File(documentFileName);
        String fileNameWithoutExtension = stripFileExtension(documentFile.getName());
        String outputFName = fileNameWithoutExtension + "-revision-history.tex";
        File outputFile = new File(destDir, outputFName);

        List<String> lines = readProcessOutput(command, outputFile);

        // System.out.println("Completed reading git history.");
        SortedMap<Date, StringBuilder> dateToLog = collectHistoryEntries(lines);

        // Paginate
        List<List<HistoryEntry>> historyEntriesPerPage = paginate(dateToLog);

        // Write out top level history tex file
        System.out.println("Writing history file \"" + outputFile + "\".");
        try (BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputFile))) {
            for (int pagei = 0; pagei < historyEntriesPerPage.size(); pagei++) {
                outputWriter.write("\\input{build/" + fileNameWithoutExtension
                    + "-revision-history-" + pagei + ".tex}\n");
            }
        }

        // Write out each page's history.
        for (int pagei = 0; pagei < historyEntriesPerPage.size(); pagei++) {
            File pageFileName = new File(outputFile.getParent(),
                fileNameWithoutExtension + "-revision-history-" + pagei + ".tex");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(pageFileName))) {
                // Table header
                writer.write("\\begin{table}[H]\n");
                writer.write("\\centering\n");
                writer.write("\\begin{tabularx}{\\linewidth}{l X}\n");
                writer.write("\\thead{Change Date} & \\thead{Notes} \\\\\n");
                writer.write("\\hline\n");

                for (HistoryEntry historyEntry : historyEntriesPerPage.get(pagei)) {
                    historyEntry.writeTo(writer);
                }

                // Table footer.
                writer.write("\\hline\n");
                writer.write("\\end{tabularx}\n");
                writer.write("\\end{table}\n");
                writer.write("\\newpage\n");
            }
        }

    }

    private static List<List<HistoryEntry>> paginate(SortedMap<Date, StringBuilder> dateToLog) {

        List<List<HistoryEntry>> entriesPerPage = new ArrayList<>();
        int lineCount = 0;
        for (Map.Entry<Date, StringBuilder> entry : dateToLog.entrySet()) {
            String entryString = entry.getValue().toString();
            int lineCountForEntry = entryString.length() / CHARACTERS_PER_LINE + 1;
            lineCount += lineCountForEntry;
            int pageNo = lineCount / LINES_PER_PAGE;

            if (entriesPerPage.size() <= pageNo) {
                entriesPerPage.add(new ArrayList<>(LINES_PER_PAGE));
            }
            List<HistoryEntry> l = entriesPerPage.get(pageNo);
            l.add(new HistoryEntry(entry.getKey(), entryString, pageNo));
        }

        return entriesPerPage;
    }

    private static SortedMap<Date, StringBuilder> collectHistoryEntries(List<String> lines)
        throws ParseException {
        TreeMap<Date, StringBuilder> dateToLog = new TreeMap<>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy Z");
        StringBuilder currentEntry = null;
        Date entryDate = null;
        boolean skipEntry = false;
        for (String line : lines) {
            if (line.startsWith("commit")) {
                // Start of new entry
                if (currentEntry != null && entryDate != null) {
                    StringBuilder existingLog = dateToLog.get(entryDate);
                    existingLog.append(currentEntry);
                }
                currentEntry = new StringBuilder();
                entryDate = null;
                skipEntry = false;
            } else if (line.startsWith("Date: ")) {
                // Start of new date
                String dateLine = line.substring(6, line.length()).trim();
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(dateFormat.parse(dateLine));
                calendar.set(Calendar.HOUR, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.AM_PM, Calendar.AM);
                entryDate = calendar.getTime();
                if (!dateToLog.containsKey(entryDate)) {
                    dateToLog.put(entryDate, new StringBuilder());
                }
            } else if (line.startsWith("Author")) {
                // Ignore
            } else if (line.matches("\\s+") || line.length() == 0) {
                // Ignore lines with only white space
            } else if (line.contains("Merge branch")) {
                // Ignore log entries that are merges.
                skipEntry = true;
                currentEntry = null;
            } else if (!skipEntry) {
                // Content
                if (currentEntry == null) {
                    throw new NullPointerException("currentEntry == null.  line=\"" + line + "\"");
                }
                line = line.trim();
                currentEntry.append("~");
                currentEntry.append(escapeLatex(line));
            }
        }
        if (currentEntry != null && entryDate != null) {
            // Append last entry
            StringBuilder existingLog = dateToLog.get(entryDate);
            existingLog.append(currentEntry);
        }
        return dateToLog;
    }

    private static List<String> readProcessOutput(String[] command, File outputFile)
        throws InterruptedException, IOException {
        Process process = null;
        List<String> lines = new ArrayList<>(1024);
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            // processBuilder.inheritIO();
            process = processBuilder.start();
            // System.out.println("Running git process.");
            BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(process.getInputStream()));

            for (String line = bufferedReader.readLine(); line != null; line = bufferedReader
                .readLine()) {
                // System.out.println("lineRead " + line);
                lines.add(line);
            }
            process.waitFor(1, TimeUnit.SECONDS);

        } catch (IOException ioe) {
            System.out.println("Writing error to latex file.");
            try {
                writeErrorToLatexFile(ioe, outputFile);
            } catch (IOException ignored) {
            }
            throw ioe;
        } finally {
            if (process != null) {
                try {
                    process.getErrorStream().close();
                } catch (Exception ignored) {
                }
                try {
                    process.getOutputStream().close();
                } catch (Exception ignored) {
                }
                try {
                    process.getInputStream().close();
                } catch (Exception ignored) {
                }
            }
        }

        if (process.exitValue() != 0) {
            throw new IOException(
                "Process exited with non-zero exit code (" + process.exitValue() + ").");
        }
        return lines;
    }

    /**
     * Returns a file name without its extension so blah.txt would become blah (no dot).
     */
    public static String stripFileExtension(String fname) {
        int dotIndex = fname.lastIndexOf('.');
        if (dotIndex == -1 || dotIndex == 0) {
            return fname;
        } else {
            return fname.substring(0, dotIndex);
        }
    }

    /**
     * Returns the file extension
     */
    public static String fileExtension(String fname) {
        int dotIndex = fname.lastIndexOf('.');
        if (dotIndex == -1 || dotIndex == 0 || dotIndex == (fname.length() - 1)) {
            return "";
        } else {
            return fname.substring(dotIndex + 1, fname.length());
        }
    }

    public static String changeFileExtension(String fname, String newExtension) {
        if (newExtension.charAt(0) != '.') {
            return newExtension = "." + newExtension;
        }
        String prefix = stripFileExtension(fname);
        return prefix + newExtension;
    }

    /**
     * Given the specified file name this appends the creation date before the file name extension.
     */
    public static String appendCreationDate(String fname) {
        String extension = fileExtension(fname);
        if (extension.length() != 0) {
            extension = "." + extension;
        }
        String prefix = stripFileExtension(fname);
        // TODO: Is this the correct format?
        SimpleDateFormat dateFormatter = new SimpleDateFormat("dd-MMM-yyyy");
        return prefix + "-" + dateFormatter.format(new Date()) + extension;
    }
}

