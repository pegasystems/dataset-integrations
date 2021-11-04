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

import com.pega.platform.integrationengine.repository.RepositoryError;
import com.pega.platform.integrationengine.repository.RepositoryException;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class MockedRepository {

    private static final Function<byte[], InputStream> DEFAULT_STREAM_CREATOR = bytes -> new ByteArrayInputStream(bytes);
    private final Map<String, ByteArrayOutputStream> files = new ConcurrentHashMap<>();
    private final Map<String, Long> lastModified = new ConcurrentHashMap<>();
    private final Set<String> directories = new HashSet<>();
    private final String repoPrefix;
    private final String root;
    private final ConcurrentLinkedQueue<Closeable> openedStreams = new ConcurrentLinkedQueue<>();
    private boolean broken;
    private List<String> brokenPaths = new ArrayList<>();
    private final Map<String, Function<byte[], InputStream>> specialStreams = new HashMap<>();
    private final Map<String, Supplier<OutputStream>> specialOutputStreams = new HashMap<String, Supplier<OutputStream>>();
    private boolean closedStreamsVerificationEnabled = true; //spied output streams don't handle big load well, OOM can happen

    MockedRepository(String repoPrefix) {
        this.repoPrefix = repoPrefix;
        this.root = repoPrefix + "/";
    }

    Map<String, ByteArrayOutputStream> getFiles() {
        return files;
    }

    void setBroken(boolean broken) {
        this.broken = broken;
    }

    void addBrokenPath(String path) {
        path = sanitizePath(path);
        brokenPaths.add(path);
    }

    void addBrokenStream(String path) {
        addSpecialStream(path, bytes -> new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException();
            }
        });
    }

    void addSpecialStream(String path, Function<byte[], InputStream> streamCreator) {
        specialStreams.put(sanitizePath(path), streamCreator);
    }

    void addSpecialOutputStream(String path, Supplier<OutputStream> outputStream) {
        specialOutputStreams.put(sanitizePath(path), outputStream);
    }

    void setClosedStreamsVerificationEnabled(boolean closedStreamsVerificationEnabled) {
        this.closedStreamsVerificationEnabled = closedStreamsVerificationEnabled;
    }

    boolean exists(String path) {
        if (broken || brokenPaths.contains(path)) {
            throw new RepositoryException(RepositoryError.ISSUE_CHECKING_IF_EXISTS);
        }

        return isDirectory(path) || isFile(path);
    }

    boolean isDirectory(String path) {
        if (isRoot(path)) {
            return true;
        }
        path = sanitizePath(path);
        return directories.contains(path);
    }

    boolean isFile(String path) {
        path = sanitizePath(path);
        return files.containsKey(path);
    }

    InputStream getFileContent(String path) {
        path = sanitizePath(path);
        if (broken || brokenPaths.contains(path) || (!isFile(path))) {
            throw new RepositoryException(RepositoryError.ISSUE_GETTING_STREAM);
        }
        InputStream stream = specialStreams.getOrDefault(path, DEFAULT_STREAM_CREATOR).apply(files.get(path).toByteArray());
        return addToOpenedStreams(stream);
    }

    byte[] getBytes(String path) {
        path = sanitizePath(path);
        if (broken || brokenPaths.contains(path) || (!isFile(path))) {
            throw new RepositoryException(RepositoryError.ISSUE_GETTING_STREAM);
        }
        return files.get(path).toByteArray();
    }

    boolean createDirectory(String path) {
        path = sanitizePath(path);
        if (broken || brokenPaths.contains(path) || isFile(path)) {
            throw new RepositoryException(RepositoryError.ISSUE_CREATING_FOLDER);
        }
        if (isDirectory(path)) {
            return false;
        }
        createDirectory(parent(path));
        directories.add(path);
        lastModified.put(path, System.currentTimeMillis());
        return true;
    }

    void createFile(String path, InputStream content, long lastModified) {
        path = sanitizePath(path);
        if (broken || brokenPaths.contains(path) || isDirectory(path)) {
            throw new RepositoryException(RepositoryError.ISSUE_CREATING_FILE);
        }
        checkParentExists(path);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            IOUtils.copy(content, outputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        files.put(path, outputStream);
        this.lastModified.put(path, lastModified);
    }

    OutputStream openFile(String path) {
        path = sanitizePath(path);
        if (broken || brokenPaths.contains(path)) {
            throw new RepositoryException(RepositoryError.ISSUE_CREATING_FILE);
        }
        if (isDirectory(path)) {
            throw new RepositoryException(RepositoryError.ISSUE_GETTING_STREAM);
        }
        checkParentExists(path);

        OutputStream outputStream;
        if (specialOutputStreams.containsKey(path)) {
            outputStream = addToOpenedStreams(specialOutputStreams.get(path).get());
        } else {
            outputStream = addToOpenedStreams(new ByteArrayOutputStream());
            files.put(path, (ByteArrayOutputStream) outputStream);
        }
        return outputStream;
    }

    FileMetadata getMetadata(String path) {
        path = sanitizePath(path);
        if (broken) {
            throw new RepositoryException(RepositoryError.ISSUE_GETTING_FILE);
        }
        return new FileMetadata(path, isDirectory(path), isFile(path), size(path), lastModified(path));
    }

    List<FileMetadata> listFiles(String dirPath) {
        final String sanitizedPath = sanitizePath(dirPath);
        if (broken || brokenPaths.contains(sanitizedPath)) {
            throw new RepositoryException(RepositoryError.ISSUE_LISTING_FILES);
        }
        if (!isDirectory(sanitizedPath)) {
            return Collections.emptyList();
        }
        List<String> children = new ArrayList<>();
        children.addAll(directories.stream().filter(dir -> sanitizedPath.equals(parent(dir))).collect(Collectors.toList()));
        children.addAll(files.keySet().stream().filter(file -> sanitizedPath.equals(parent(file))).collect(Collectors.toList()));
        return children.stream().map(path -> getMetadata(path)).collect(Collectors.toList());
    }

    boolean delete(String path) {
        if (broken || brokenPaths.contains(path)) {
            throw new RepositoryException(RepositoryError.ISSUE_DELETING_FILE);
        }
        if (isFile(path)) {
            files.remove(path);
            return true;
        }
        if (isDirectory(path)) {
            if (listFiles(path).isEmpty()) {
                directories.remove(path);
                return true;
            }
            throw new RepositoryException(RepositoryError.FOLDER_NOT_EMPTY);
        }
        return false;
    }

    boolean deleteRecursively(String path) {
        if (broken || brokenPaths.contains(path)) {
            throw new RepositoryException(RepositoryError.ISSUE_DELETING_FILE);
        }
        if (!exists(path)) {
            return false;
        }
        for (FileMetadata child : listFiles(path)) {
            deleteRecursively(child.getPath());
        }
        return delete(path);
    }

    void verifyAllStreamsClosed() throws IOException {
        if (closedStreamsVerificationEnabled) {
            for (Closeable stream : openedStreams) {
                verify(stream, times(1)).close();
            }
        }
    }

    private <T extends Closeable> T addToOpenedStreams(T stream) {
        if (closedStreamsVerificationEnabled) {
            stream = spy(stream);
        }
        openedStreams.add(stream);
        return stream;
    }

    private void checkParentExists(String path) {
        String parent = parent(path);
        if (!isDirectory(parent)) {
            throw new IllegalStateException("Parent doesn't exist for path " + path);
        }
    }

    private String parent(String path) {
        if (isRoot(path)) {
            throw new IllegalStateException("Root has no parent");
        }
        int lastSlashIdx = path.lastIndexOf('/');
        if (lastSlashIdx == root.length()) {
            return root + "/";
        }
        return path.substring(0, Math.max(lastSlashIdx, root.length()));
    }

    private long size(String path) {
        if (!files.containsKey(path)) {
            return 0;
        }
        return files.get(path).toByteArray().length;
    }

    private String sanitizePath(String path) {
        if (isRoot(path)) {
            return path;
        }
        if (!path.startsWith(root) && path.startsWith(repoPrefix)) {
            path = root + path.substring(repoPrefix.length());
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length()-1);
        }
        return path;
    }

    private boolean isRoot(String path) {
        return root.equals(path);
    }

    private long lastModified(String path) {
        return lastModified.getOrDefault(path, 0L);
    }


    final class FileMetadata {
        private final String path;
        private final boolean isDirectory;
        private final boolean isFile;
        private final long size;
        private final long lastModified;

        private FileMetadata(String path, boolean isDirectory, boolean isFile, long size, long lastModified) {
            this.path = path;
            this.isDirectory = isDirectory;
            this.isFile = isFile;
            this.size = size;
            this.lastModified = lastModified;
        }

        public String getPath() {
            return path;
        }

        public boolean isDirectory() {
            return isDirectory;
        }

        public boolean isFile() {
            return isFile;
        }

        public boolean exists() {
            return isDirectory() || isFile();
        }

        public long getSize() {
            return size;
        }

        public long getLastModified() {
            return lastModified;
        }
    }
}
