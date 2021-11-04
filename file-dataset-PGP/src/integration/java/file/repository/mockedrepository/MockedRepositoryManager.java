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

import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.platform.integrationengine.repository.File;
import com.pega.platform.integrationengine.repository.RepositoryError;
import com.pega.platform.integrationengine.repository.RepositoryException;
import com.pega.platform.integrationengine.repository.RepositoryManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MockedRepositoryManager implements RepositoryManager {

    private final String name;
    private final MockedRepository repository;

    public MockedRepositoryManager(String name) {
        this.name = name;
        repository = new MockedRepository(repositoryPrefix());
        FileStorageManagerMocks.mockFileStorageManager(repository);
    }

    public void setBroken(boolean broken) {
        repository.setBroken(broken);
    }

    public void addBrokenPath(String path) {
        repository.addBrokenPath(fullRepositoryFilePath(path));
    }

    public void addBrokenStream(String path) {
        repository.addBrokenStream(fullRepositoryFilePath(path));
    }

    public void addSpecialStream(String path, Function<byte[], InputStream> streamCreator) {
        repository.addSpecialStream(fullRepositoryFilePath(path), streamCreator);
    }

    public void setClosedStreamsVerificationEnabled(boolean closedStreamsVerificationEnabled) {
        repository.setClosedStreamsVerificationEnabled(closedStreamsVerificationEnabled);
    }

    public void verifyAllStreamsClosed() throws IOException {
        repository.verifyAllStreamsClosed();
    }

    private String fullRepositoryFilePath(String repositoryPath) {
        if (!repositoryPath.startsWith("/")) {
            repositoryPath = "/" + repositoryPath;
        }
        return repositoryPath(repositoryPath);
    }

    private String repositoryPrefix() {
        return repositoryPath("");
    }

    private String repositoryPath(String path) {
        return String.format("file://%s:/%s", name, path);
    }

    public Map<String, byte[]> getAllFiles() {
        Map<String, byte[]> files = new HashMap<>();
        String repositoryPrefix = repositoryPrefix();
        repository.getFiles().entrySet()
                .forEach(entry -> files.put(
                        entry.getKey().substring(repositoryPrefix.length()),
                        entry.getValue().toByteArray()));
        return files;
    }

    @Override
    public void register(Map<String, String> repositorySettings) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void register(Map<String, String> repositorySettings, boolean skipInvalidRepos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void register(Map<String, String> repositorySettings, boolean skipInvalidRepos, boolean publishOtherNodes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean deregister(String repository) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean deregister(String repository, boolean publishOtherNodes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String createFile(String repository, String path, InputStream content) {
        return createFile(repository, path, content, System.currentTimeMillis());
    }

    @Override
    public String createFile(String repository, String path, String content) {
        return createFile(repository, path, content, System.currentTimeMillis());
    }

    public String createFile(String repository, String path, String content, long lastModified) {
        return createFile(repository, path, new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), lastModified);
    }

    public String createFile(String repository, String path, InputStream content, long lastModified) {
        checkName(repository);
        String fullPath = fullRepositoryFilePath(path);
        this.repository.createFile(fullPath, content, lastModified);
        return fullPath;
    }

    @Override
    public InputStream getInputStream(String repository, String path) {
        checkName(repository);
        String fullPath = fullRepositoryFilePath(path);
        return this.repository.getFileContent(fullPath);
    }

    public byte[] getBytes(String repository, String path) {
        checkName(repository);
        String fullPath = fullRepositoryFilePath(path);
        return this.repository.getBytes(fullPath);
    }

    @Override
    public String getFile(String repository, String path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public File getMetadata(String repository, String path) {
        checkName(repository);
        MockedRepository.FileMetadata metadata = this.repository.getMetadata(fullRepositoryFilePath(path));
        if (!metadata.exists()) {
            throw new RepositoryException(RepositoryError.ISSUE_GETTING_FILE);
        }
        return new MockedFile(path, metadata);
    }

    @Override
    public boolean exists(String repository, String path) {
        checkName(repository);
        return this.repository.exists(fullRepositoryFilePath(path));
    }

    @Override
    public Collection<File> listFiles(String repository, String path) {
        checkName(repository);
        int rootLength = fullRepositoryFilePath("/").length() - 1;
        return this.repository.listFiles(fullRepositoryFilePath(path))
                .stream()
                .map(fileMetadata -> new MockedFile(fileMetadata.getPath().substring(rootLength), fileMetadata))
                .collect(Collectors.toList());
    }

    @Override
    public boolean delete(String repository, String path) {
        checkName(repository);
        return this.repository.delete(fullRepositoryFilePath(path));
    }

    @Override
    public boolean createFolder(String repository, String path) {
        checkName(repository);
        return this.repository.createDirectory(fullRepositoryFilePath(path));
    }

    @Override
    public boolean recursiveDelete(String repository, String path) {
        checkName(repository);
        return this.repository.deleteRecursively(fullRepositoryFilePath(path));
    }

    @Override
    public void noteChange(ClipboardPage repositoryPage, boolean isDelete) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void noteChange(ClipboardPage repositoryPage, boolean isDelete, boolean publishOtherNodes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initCache() {
        throw new UnsupportedOperationException();
    }

    private void checkName(String name) {
        if (!name.equals(this.name)) {
            throw new IllegalArgumentException(name + " != " + this.name);
        }
    }

    public void addSpecialOutputStream(String path, Supplier<OutputStream> outputStream) {
        this.repository.addSpecialOutputStream(fullRepositoryFilePath(path), outputStream);
    }

    private final class MockedFile implements File {

        private final String path;
        private final MockedRepository.FileMetadata metadata;

        private MockedFile(String path, MockedRepository.FileMetadata metadata) {
            this.path = path;
            this.metadata = metadata;
        }

        @Override
        public boolean isDirectory() {
            return metadata.isDirectory();
        }

        @Override
        public String getName() {
            if ("/".equals(path)) {
                return path;
            }
            int lastSlashIdx = path.indexOf('/');
            return path.substring(lastSlashIdx);
        }

        @Override
        public String getPath() {
            return path;
        }

        @Override
        public long length() {
            return metadata.getSize();
        }

        @Override
        public Date lastModified() {
            return new Date(metadata.getLastModified());
        }
    }
}
