/*
 *
 * Copyright (c) 2021  Pegasystems Inc.
 * All rights reserved.
 *
 * This  software  has  been  provided pursuant  to  a  License
 * Agreement  containing  restrictions on  its  use.   The  software
 * contains  valuable  trade secrets and proprietary information  of
 * Pegasystems Inc and is protected by  federal   copyright law.  It
 * may  not be copied,  modified,  translated or distributed in  any
 * form or medium,  disclosed to third parties or used in any manner
 * not provided for in  said  License Agreement except with  written
 * authorization from Pegasystems Inc.
 */
package file.repository.mockedrepository;

import com.pega.pegarules.storage.FileStorageManager;
import org.mockito.ArgumentMatchers;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Use the following test set up to enable mocking
 * @RunWith(PowerMockRunner.class)
 * @PrepareForTest({ThreadContainer.class, FileStorageManager.class })
 */
final class FileStorageManagerMocks {

    private FileStorageManagerMocks() {}

    static void mockFileStorageManager(MockedRepository repository) {
        PowerMockito.mockStatic(FileStorageManager.class);

        when(FileStorageManager.getFile(ArgumentMatchers.any(String.class), ArgumentMatchers.anyBoolean()))
                .thenAnswer(invocation -> {
                    String path = (String) invocation.getArguments()[0];
                    return new MockedIFile(repository.getMetadata(path), repository);
                });

        try {
            when(FileStorageManager.getOutputStream(ArgumentMatchers.anyString(), ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean()))
                    .then(invocation -> {
                        String path = (String) invocation.getArguments()[0];
                        return repository.openFile(path);
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
