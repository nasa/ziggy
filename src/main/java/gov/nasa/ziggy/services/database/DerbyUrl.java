package gov.nasa.ziggy.services.database;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The result of parsing the Derby URL. This is needed because the Derby DataSource objects will not
 * parse their own URLs.
 *
 * @author Sean McCauliff
 */
public class DerbyUrl {
    public final static int DERBY_DEFAULT_PORT = 1523;

    final String databaseName;
    /** e.g. ";create=true" */
    final String attributes;
    final String hostName;
    final int portNumber;

    public DerbyUrl(String databaseName, String attributes, String hostName, int portNumber) {
        this.databaseName = databaseName;
        this.attributes = attributes;
        this.hostName = hostName;
        this.portNumber = portNumber;
    }

    public static DerbyUrl parseDerbyUrl(String url) {
        Pattern p = Pattern.compile("jdbc:derby:([^;]+)(;.+)?");
        Matcher m = p.matcher(url);
        if (!m.matches()) {
            throw new IllegalArgumentException("Invalid derby url \" " + url + "\".");
        }

        String databaseName = null;
        String hostName = null;
        String attributes = "";
        int portNumber = DERBY_DEFAULT_PORT;

        if (m.group(1).startsWith("//")) {
            // parse network url
            Pattern netPattern = Pattern.compile("//([^:/]+)(:\\d+)?/(.+)");
            Matcher netMatcher = netPattern.matcher(m.group(1));
            if (!netMatcher.matches()) {
                throw new IllegalArgumentException(
                    "Bad network specification " + "for derby url \"" + m.group(1) + "\".");
            }

            hostName = netMatcher.group(1);
            if (netMatcher.group(2) != null) {
                portNumber = Integer.parseInt(netMatcher.group(2).substring(1));
            }
            databaseName = netMatcher.group(3);
        } else {
            databaseName = m.group(1);
        }

        if (m.groupCount() > 1) {
            attributes = m.group(2) == null ? "" : m.group(2).substring(1);
        }

        return new DerbyUrl(databaseName, attributes, hostName, portNumber);
    }

    /**
     * @return the attributes
     */
    public String getAttributes() {
        return attributes;
    }

    /**
     * @return the databaseName
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * @return the hostName
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * @return the portNumber
     */
    public int getPortNumber() {
        return portNumber;
    }
}
