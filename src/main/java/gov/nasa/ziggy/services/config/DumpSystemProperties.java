package gov.nasa.ziggy.services.config;

import java.util.Properties;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.configuration2.ImmutableConfiguration;

/**
 * Display all system properties, and then display all ZiggyConfiguration properties.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class DumpSystemProperties {
    /**
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("System Properties:");
        Properties props = System.getProperties();
        System.out.println(props.entrySet()
            .stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .sorted()
            .collect(Collectors.joining("\n")));

        System.out.println("\nZiggy Configuration:");
        ImmutableConfiguration configuration = ZiggyConfiguration.getInstance();
        System.out
            .println(StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(configuration.getKeys(),
                    Spliterator.ORDERED), false)
                .map(k -> k + "=" + configuration.getString(k))
                .sorted()
                .collect(Collectors.joining("\n")));
    }
}
