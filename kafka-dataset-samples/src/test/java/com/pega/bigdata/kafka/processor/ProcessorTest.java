package com.pega.bigdata.kafka.processor;

import com.pega.pegarules.pub.clipboard.ClipboardPage;
import java.util.function.BiFunction;

import static com.pega.bigdata.kafka.processor.MessageTransformator.ENCRYPTION_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessorTest {

    static void checkProcessingCorrectness(int shift, BiFunction<byte[], ClipboardPage, byte[]> processor, String expectedMessageAfterProcessing, String messageToProcess) {
        ClipboardPage headersPage = mock(ClipboardPage.class);
        when(headersPage.getString(ENCRYPTION_KEY)).thenReturn(Integer.toString(shift));
        assertArrayEquals(
                expectedMessageAfterProcessing.getBytes(),
                processor.apply(messageToProcess.getBytes(), headersPage)
        );
    }

}
