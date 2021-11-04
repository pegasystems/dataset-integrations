package com.pega.bigdata.dataset.file.pgp;

import com.google.common.io.CharStreams;
import com.google.common.io.Resources;
import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Security;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class BcPgpDecEncTest {

    @AfterAll
    public static void tearDown() {
        Security.removeProvider(new BouncyCastleProvider().getName());
    }

    @Test
    public void encryptStreamOfData(TestInfo testInfo) throws IOException {
        byte[] publicKey = Resources.toString(Resources.getResource("public-ascii-armor.pgp"), StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8);
        byte[] privateKey = Resources.toString(Resources.getResource("private-ascii-armor.pgp"), StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8);

        File actualFile = encrypt(publicKey, testInfo.getDisplayName(),"line1\n", "line2\n", "line3");
        String expectedContent =
                "line1\n" +
                        "line2\n" +
                        "line3";
        String actualContent = decrypt(privateKey, actualFile);

        assertEquals(expectedContent, actualContent);
    }

    @Test
    public void encryptUsingBinaryKeys(TestInfo testInfo) throws Exception {
        byte[] publicKey = Files.readAllBytes(new File(Resources.getResource("public.pgp").toURI()).toPath());
        byte[] privateKey = Files.readAllBytes(new File(Resources.getResource("private.pgp").toURI()).toPath());

        File actualFile = encrypt(publicKey, testInfo.getDisplayName(),"line1\n", "line2\n", "line3");
        String expectedContent =
                "line1\n" +
                        "line2\n" +
                        "line3";
        String actualContent = decrypt(privateKey, actualFile);

        assertEquals(expectedContent, actualContent);
    }

    private String decrypt(byte[] privateKey, File actualFile) throws IOException {
        BcPgpDecryptor bcPgpDecryptor = new BcPgpDecryptor(privateKey, "install");
        InputStream inputStream = bcPgpDecryptor.apply(new FileInputStream(actualFile));
        File file = new File(System.getProperty("java.io.tmpdir"), "doc.decoded");
        FileUtils.copyInputStreamToFile(inputStream, file);

        return CharStreams.toString(new InputStreamReader(bcPgpDecryptor.apply(new FileInputStream(actualFile)), StandardCharsets.UTF_8));
    }

    private File encrypt(byte[] publicKey,  String testName, String... strings) throws IOException {
        final int BUFFSIZE = 8 * 1024;

        BcPgpEncryptor bcPgpEncryptor = new BcPgpEncryptor(publicKey);

        try (final OutputStream fileOutput = Files.newOutputStream(Paths.get(System.getProperty("java.io.tmpdir"), testName + ".pgp"));
             final BufferedOutputStream bufferedOut = new BufferedOutputStream(fileOutput, BUFFSIZE);
             final OutputStream encodedContentOutputStream = bcPgpEncryptor.apply(bufferedOut)
        ) {
            for (String str : strings) {
                encodedContentOutputStream.write(str.getBytes(StandardCharsets.UTF_8));
            }
        }

        return Paths.get(System.getProperty("java.io.tmpdir"), testName + ".pgp").toFile();
    }
}