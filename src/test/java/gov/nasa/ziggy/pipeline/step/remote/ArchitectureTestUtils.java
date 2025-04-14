package gov.nasa.ziggy.pipeline.step.remote;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;

public class ArchitectureTestUtils {

    public static List<Architecture> architectures() {
        Architecture san = mockedArchitecture("san", "Sandy Bridge", 16, 32, 0.47F, 10);
        Architecture ivy = mockedArchitecture("ivy", "Ivy Bridge", 20, 64, 0.66F, 10);
        Architecture has = mockedArchitecture("has", "Haswell", 24, 128, 0.80F, 10);
        Architecture bro = mockedArchitecture("bro", "Broadwell", 28, 128, 1.00F, 10);
        Architecture sky = mockedArchitecture("sky_ele", "Skylake", 40, 192, 1.59F, 10);
        Architecture cas = mockedArchitecture("cas_ait", "Cascade Lake", 40, 192, 1.64F, 10);
        Architecture rom = mockedArchitecture("rom_ait", "Rome", 128, 512, 4.06F, 10);
        return List.of(san, ivy, has, bro, sky, cas, rom);
    }

    public static Map<String, Architecture> architectureByName() {
        List<Architecture> architectures = architectures();
        Map<String, Architecture> architectureByName = new HashMap<>();
        for (Architecture architecture : architectures) {
            architectureByName.put(architecture.getName(), architecture);
        }
        return architectureByName;
    }

    private static Architecture mockedArchitecture(String name, String description, int cores,
        int ramGigabytes, float costFactor, float bandwidthGbps) {
        Architecture architecture = Mockito.spy(Architecture.class);
        Mockito.doReturn(name).when(architecture).getName();
        Mockito.doReturn(description).when(architecture).getDescription();
        Mockito.doReturn(cores).when(architecture).getCores();
        Mockito.doReturn(ramGigabytes).when(architecture).getRamGigabytes();
        Mockito.doReturn(costFactor).when(architecture).getCost();
        Mockito.doReturn(bandwidthGbps).when(architecture).getBandwidthGbps();
        return architecture;
    }
}
