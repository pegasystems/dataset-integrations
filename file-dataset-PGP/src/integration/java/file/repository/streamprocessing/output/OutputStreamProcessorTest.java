
package file.repository.streamprocessing.output;

import com.google.common.io.Resources;
import com.pega.bigdata.dataset.file.processor.PgpDecryptor;
import com.pega.bigdata.dataset.file.processor.PgpEncryptor;
import com.pega.bigdata.dataset.file.repository.CompressionType;
import com.pega.bigdata.dataset.file.repository.RepositoryFileSystemAdapter;
import com.pega.bigdata.dataset.file.repository.streamprocessing.input.RepositoryFileReaderFactory;
import com.pega.bigdata.dataset.file.repository.streamprocessing.output.OutputStreamProcessor;
import com.pega.bigdata.dataset.file.repository.streamprocessing.output.OutputStreamProcessorBuilder;
import com.pega.bigdata.dataset.file.repository.streamprocessing.sampletest.InputStreamShiftingProcessing;
import com.pega.bigdata.dataset.file.repository.streamprocessing.sampletest.OutputStreamShiftingProcessing;
import com.pega.bigdata.dataset.readers.ContentReader;
import com.pega.bigdata.dataset.utils.filescollection.FileSystemAdapter;
import com.pega.decision.dsm.strategy.clipboard.DSMPegaAPI;
import com.pega.pegarules.priv.util.Base64;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.context.PRThread;
import com.pega.pegarules.pub.context.ThreadContainer;
import com.pega.pegarules.pub.runtime.PublicAPI;
import com.pega.pegarules.storage.FileStorageManager;
import file.repository.mockedrepository.MockedRepositoryManager;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileStorageManager.class, ThreadContainer.class})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*"})
public class OutputStreamProcessorTest {
    private static final String RECORD = "simple record";
    private static final String PRIVATE_KEY = "private-ascii-armor.pgp";
    private static final String PUBLIC_KEY = "public-ascii-armor.pgp";
    private static final String FILE_NAME = "path";
    private static final String PASSPHRASE = "install";
    private static final String REPOSITORY_NAME = "test_file_system";
    private MockedRepositoryManager repositoryManager;

    static final OutputStream OUTPUT_STREAM = mock(OutputStream.class);

    private OutputStreamProcessor processor;

    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    private PRThread mockedThread;
    private PublicAPI publicAPImock = new DSMPegaAPI(null);

    private void mockPRThread() {
        mockedThread = Mockito.mock(PRThread.class);
        mockStatic(ThreadContainer.class);
        when(ThreadContainer.get()).thenReturn(mockedThread);
        when(mockedThread.getPublicAPI()).thenReturn(publicAPImock);

        ClipboardPage keyPage = publicAPImock.createPage("@baseclass", "D_PGPKeys");
        keyPage.putString("Public_key", Base64.encode(resource(PUBLIC_KEY)));

        keyPage.putString("Private_key", Base64.encode(resource(PRIVATE_KEY)));
        keyPage.putString("Passphrase", PASSPHRASE);
    }


    @Before
    public void setUp() {
        repositoryManager = new MockedRepositoryManager(REPOSITORY_NAME);
        processor = (os, file) -> {
            if (OUTPUT_STREAM.equals(os)) {
                return byteArrayOutputStream;
            }
            fail("Expected different output stream");
            return null;
        };

        mockPRThread();
    }

    @After
    public void tearDown() {
        Security.removeProvider(new BouncyCastleProvider().getName());
    }

    @Test
    public void testEncryptionBuild() throws Exception {
        OutputStreamProcessorBuilder outputStreamProcessorBuilder = new OutputStreamProcessorBuilder();
        outputStreamProcessorBuilder.withCustomProcessing(PgpEncryptor.class.getName());
        OutputStreamProcessor outputStreamProcessor = outputStreamProcessorBuilder.build();

        writeRecord(outputStreamProcessor);

        FileSystemAdapter fileSystemAdapter = getFileSystemAdapterUsingContent(byteArrayOutputStream.toByteArray());

        RepositoryFileReaderFactory.Builder builder = RepositoryFileReaderFactory.builder(REPOSITORY_NAME);
        builder.withCustomProcessing(PgpDecryptor.class.getName());
        RepositoryFileReaderFactory fileReaderFactory = builder.build();

        Optional<ContentReader> bufferedReader = fileReaderFactory.create(fileSystemAdapter, FILE_NAME);

        assertEquals(RECORD, readFirstLine(bufferedReader.orElseGet(null)));
    }

    @Test
    public void testEncryptionWithGzipBuild() throws Exception {
        OutputStreamProcessorBuilder outputStreamProcessorBuilder = new OutputStreamProcessorBuilder();
        outputStreamProcessorBuilder.withCustomProcessing(PgpEncryptor.class.getName());
        outputStreamProcessorBuilder.withCompressionType(CompressionType.GZIP);
        OutputStreamProcessor outputStreamProcessor = outputStreamProcessorBuilder.build();

        writeRecord(outputStreamProcessor);

        FileSystemAdapter fileSystemAdapter = getFileSystemAdapterUsingContent(byteArrayOutputStream.toByteArray());

        RepositoryFileReaderFactory.Builder builder = RepositoryFileReaderFactory.builder(REPOSITORY_NAME);
        builder.withCustomProcessing(PgpDecryptor.class.getName());
        builder.withCompressionType(CompressionType.GZIP);
        RepositoryFileReaderFactory fileReaderFactory = builder.build();

        Optional<ContentReader> bufferedReade = fileReaderFactory.create(fileSystemAdapter, FILE_NAME);

        assertEquals(RECORD, readFirstLine(bufferedReade.orElseGet(null)));
    }

    @Test
    public void testEncryptionWithGzipAndCustomProcessingBuild() throws Exception {
        OutputStreamProcessorBuilder outputStreamProcessorBuilder = new OutputStreamProcessorBuilder();
        outputStreamProcessorBuilder.withCustomProcessing(PgpEncryptor.class.getName());
        outputStreamProcessorBuilder.withCompressionType(CompressionType.GZIP);
        outputStreamProcessorBuilder.withCustomProcessing(OutputStreamShiftingProcessing.class.getCanonicalName());
        OutputStreamProcessor outputStreamProcessor = outputStreamProcessorBuilder.build();

        writeRecord(outputStreamProcessor);

        FileSystemAdapter fileSystemAdapter = getFileSystemAdapterUsingContent(byteArrayOutputStream.toByteArray());

        RepositoryFileReaderFactory.Builder builder = RepositoryFileReaderFactory.builder(REPOSITORY_NAME);
        builder.withCustomProcessing(PgpDecryptor.class.getName());
        builder.withCompressionType(CompressionType.GZIP);
        builder.withCustomProcessing(InputStreamShiftingProcessing.class.getCanonicalName());
        RepositoryFileReaderFactory fileReaderFactory = builder.build();

        Optional<ContentReader> bufferedReade = fileReaderFactory.create(fileSystemAdapter, FILE_NAME);

        assertEquals(RECORD, readFirstLine(bufferedReade.orElseGet(null)));
    }

    @Test
    public void testEncryptionWithCustomProcessingBuild() throws Exception {
        OutputStreamProcessorBuilder outputStreamProcessorBuilder = new OutputStreamProcessorBuilder();
        outputStreamProcessorBuilder.withCustomProcessing(PgpEncryptor.class.getName());
        outputStreamProcessorBuilder.withCustomProcessing(OutputStreamShiftingProcessing.class.getCanonicalName());
        OutputStreamProcessor outputStreamProcessor = outputStreamProcessorBuilder.build();

        writeRecord(outputStreamProcessor);

        FileSystemAdapter fileSystemAdapter = getFileSystemAdapterUsingContent(byteArrayOutputStream.toByteArray());

        RepositoryFileReaderFactory.Builder builder = RepositoryFileReaderFactory.builder(REPOSITORY_NAME);
        builder.withCustomProcessing(PgpDecryptor.class.getName());
        builder.withCustomProcessing(InputStreamShiftingProcessing.class.getCanonicalName());
        RepositoryFileReaderFactory fileReaderFactory = builder.build();

        Optional<ContentReader> bufferedReade = fileReaderFactory.create(fileSystemAdapter, FILE_NAME);

        assertEquals(RECORD, readFirstLine(bufferedReade.orElseGet(null)));
    }

    private void writeRecord(OutputStreamProcessor outputStreamProcessor) throws IOException {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStreamProcessor.process(byteArrayOutputStream, FILE_NAME), StandardCharsets.UTF_8))) {
            bufferedWriter.write(RECORD);
        }
    }

    private String readFirstLine(ContentReader bufferedReade) throws IOException {
        if (bufferedReade != null) {
            Optional<BufferedReader> nextFile = bufferedReade.openNextFile();
            if (nextFile.isPresent()) {
                return nextFile.get().readLine();
            }
        }
        return "";
    }

    private FileSystemAdapter getFileSystemAdapterUsingContent(byte[] content) {
        repositoryManager.createFile(REPOSITORY_NAME, FILE_NAME, new ByteArrayInputStream(content));

        return new RepositoryFileSystemAdapter(() -> repositoryManager, REPOSITORY_NAME);
    }

    protected byte[] resource(String name) {
        try {
            return Resources.toByteArray(Resources.getResource(name));
        } catch (Exception e) {
            throw new AssertionError("cannot load resource needed for test: " + name);
        }
    }
}
