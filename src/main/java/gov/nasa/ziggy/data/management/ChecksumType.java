package gov.nasa.ziggy.data.management;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import org.apache.commons.codec.digest.DigestUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import jakarta.xml.bind.annotation.XmlEnum;

/**
 * Defines the supported checksum types. These are a subset of the types available in
 * {@link DigestUtils}, feel free to add additional types if a use-case presents itself.
 *
 * @author PT
 */
@XmlEnum
public enum ChecksumType {
    MD5 {
        @Override
        @SuppressFBWarnings(value = "WEAK_MESSAGE_DIGEST_MD5", justification = """
            Checksums used for integrity, not security, hence
               weak checksums are acceptable.
               """)
        @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
        public String checksum(Path file) {
            try {
                return DigestUtils.md5Hex(new FileInputStream(file.toFile()));
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Unable to open file input stream for " + file.toString(), e);
            }
        }
    },
    SHA1 {
        @Override
        @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
        public String checksum(Path file) {
            try {
                return DigestUtils.sha1Hex(new FileInputStream(file.toFile()));
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Unable to open file input stream for " + file.toString(), e);
            }
        }
    },
    SHA256 {
        @Override
        @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
        public String checksum(Path file) {
            try {
                return DigestUtils.sha256Hex(new FileInputStream(file.toFile()));
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Unable to open file input stream for " + file.toString(), e);
            }
        }
    },
    SHA512 {
        @Override
        @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
        public String checksum(Path file) {
            try {
                return DigestUtils.sha512Hex(new FileInputStream(file.toFile()));
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Unable to open file input stream for " + file.toString(), e);
            }
        }
    };

    public abstract String checksum(Path file);

    public String value() {
        return name();
    }

    public static ChecksumType fromValue(String v) {
        return ChecksumType.valueOf(v);
    }
}
