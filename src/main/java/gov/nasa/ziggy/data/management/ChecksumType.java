package gov.nasa.ziggy.data.management;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

import javax.xml.bind.annotation.XmlEnum;

import org.apache.commons.codec.digest.DigestUtils;

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
        public String checksum(Path file) throws IOException {
            return DigestUtils.md5Hex(new FileInputStream(file.toFile()));
        }
    },
    SHA1 {
        @Override
        public String checksum(Path file) throws IOException {
            return DigestUtils.sha1Hex(new FileInputStream(file.toFile()));
        }
    },
    SHA256 {
        @Override
        public String checksum(Path file) throws IOException {
            return DigestUtils.sha256Hex(new FileInputStream(file.toFile()));
        }
    },
    SHA512 {
        @Override
        public String checksum(Path file) throws IOException {
            return DigestUtils.sha512Hex(new FileInputStream(file.toFile()));
        }
    };

    public abstract String checksum(Path file) throws IOException;

    public String value() {
        return name();
    }

    public static ChecksumType fromValue(String v) {
        return ChecksumType.valueOf(v);
    }
}
