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

import com.pega.pegarules.priv.storage.IFile;

import java.util.List;

public class MockedIFile implements IFile {

    private final MockedRepository.FileMetadata metadata;
    private final MockedRepository repository;

    public MockedIFile(MockedRepository.FileMetadata metadata, MockedRepository repository) {
        this.metadata = metadata;
        this.repository = repository;
    }

    @Override
    public boolean canRead() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canWrite() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists() {
        return metadata.exists();
    }

    @Override
    public IFile getAbsoluteFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getAbsolutePath() {
        return metadata.getPath();
    }

    @Override
    public IFile getCanonicalFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCanonicalPath() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getParent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IFile getParentFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDirectory() {
        return metadata.isDirectory();
    }

    @Override
    public boolean isFile() {
        return metadata.isFile();
    }

    @Override
    public long lastModified() {
        return metadata.getLastModified();
    }

    @Override
    public long length() {
        return metadata.getSize();
    }

    @Override
    public String[] list() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IFile[] listFiles() {
        List<MockedRepository.FileMetadata> children = repository.listFiles(metadata.getPath());
        return children.stream().map(fileMetadata -> new MockedIFile(fileMetadata, repository)).toArray(MockedIFile[]::new);
    }

    @Override
    public boolean mkdir() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean renameTo(IFile aDest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean setLastModified(long aTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLockCode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void recursiveDelete() {
        throw new UnsupportedOperationException();
    }
}
