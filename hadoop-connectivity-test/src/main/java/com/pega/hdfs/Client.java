package com.pega.hdfs;
/*
 *
 * Copyright (c) 2023 Pegasystems Inc.
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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class Client {

    private static Logger logger = LoggerFactory.getLogger(Client.class);

    private final ClientConfiguration configuration;

    public Client(ClientConfiguration configuration) {
        this.configuration = configuration;
    }

    public void doTest() {
        configuration.krb5ConfPath().ifPresent(path -> {
            System.setProperty("java.security.krb5.conf", path);
        });
        UserGroupInformation.setConfiguration(configuration.hadoopConfiguration());
        logIn();
        Test test;
        if (logIn()) {
            try (FileSystem fs = FileSystem.get(configuration.hadoopConfiguration())) {
                test = new TestWithConnection(fs);
            } catch (Exception e) {
                logger.error("Cannot connect to HDFS", e);
                test = new FailedTest();
            }
        } else {
            test = new FailedTest();
        }
        test.printResult();
    }

    private boolean logIn() {
        return configuration.clientPrincipal()
                .map(principal -> configuration.keytabPath()
                        .map(keytab -> loginFromKeytab(principal, keytab))
                        .orElseGet(() -> loginUser(principal)))
                .orElseGet(() -> {
                    logger.warn("No principal specified. Log in not attempted");
                    return true;
                });
    }

    private static boolean loginFromKeytab(String principal, String keytab) {
        logger.debug("Login user {} with keytab {}", principal, keytab);
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            return true;
        } catch (Exception e) {
            logger.error("Failed to login from keytab", e);
            return false;
        }
    }

    private boolean loginUser(String principal) {
        logger.debug("Login user {} without keytab", principal);
        try {
            UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(principal));
            return true;
        } catch (Exception e) {
            logger.error("Failed to login user " + principal, e);
            return false;
        }
    }

    private interface Test {
        void printResult();
    }

    private class TestWithConnection implements Test {
        private final Path workDir = new Path(configuration.testWorkDir());
        private boolean workDirExists;
        private boolean workIsDirectory;
        private List<Path> existingFiles = new ArrayList<>();
        private boolean canReadDirectory;
        private boolean canListFiles;
        private boolean canCreateFile;
        private boolean canDeleteFile;

        private TestWithConnection(FileSystem fs) {
            checkWorkDirExists(fs);
            if (workDirExists && workIsDirectory) {
                listFiles(fs);
                modifyFile(fs);
            }
        }

        private void checkWorkDirExists(FileSystem fs) {
            try {
                workDirExists = fs.exists(workDir);
                if (workDirExists) {
                    workIsDirectory = fs.getFileStatus(workDir).isDirectory();
                }
                canReadDirectory = true;
            } catch (Exception e) {
                logger.error("Error while checking work directory existence", e);
            }
        }

        private void listFiles(FileSystem fs) {
            try {
                RemoteIterator<LocatedFileStatus> files =  fs.listFiles(workDir, false);
                while (files.hasNext()) {
                    existingFiles.add(files.next().getPath());
                }
                canListFiles = true;
            } catch (Exception e) {
                logger.error("Error while listing files", e);
            }
        }

        private void modifyFile(FileSystem fs) {
            Path filePath = new Path(workDir, "testfile");
            try {
               try (FSDataOutputStream os = fs.create(new Path(workDir, "testfile"))) {
                    os.writeBytes("test");
                }
                canCreateFile =  fs.exists(filePath);
            } catch (Exception e) {
                logger.error("Error while writing file in work directory", e);
            }
            if (canCreateFile) {
                try {
                    fs.delete(filePath, false);
                    canDeleteFile = true;
                } catch (Exception e) {
                    logger.error("Error while deleting file in work directory", e);
                }
            }
        }

        @Override
        public void printResult() {
            System.out.println("TEST RESULTS:");
            System.out.println("Connected to HDFS: true");
            System.out.println("Work directory: " + workDir);
            System.out.println("Can access work directory metadata: " + canReadDirectory);
            System.out.println("Work directory exists: " + workDirExists);
            if (workDirExists) {
                System.out.println("Work path is directory: " + workIsDirectory);
            }
            if (workDirExists && workIsDirectory) {
                System.out.println("Can list work directory files: " + canListFiles);
                if (canListFiles) {
                    System.out.println("Number of files in work directory: " + existingFiles.size());
                    if (!existingFiles.isEmpty()) {
                        System.out.println("Files in work directory:");
                        existingFiles.forEach(file -> System.out.println("\t" + file));
                    }
                }
                System.out.println("Can create file in work directory: " + canCreateFile);
                System.out.println("Can delete file in work directory: " + canDeleteFile);
            } else {
                System.out.println("Skipped listing and writing files tests because work directory doesn't exist or is not a directory");
            }
        }
    }

    private class FailedTest implements Test {

        @Override
        public void printResult() {
            System.out.println("TEST RESULTS:");
            System.out.println("Connected to HDFS: false");
        }
    }
}
