package com.pega.bigdata.dataset.file.pgp;/*
 *
 * Copyright (c) 2021 Pegasystems Inc.
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

import com.pega.pegarules.pub.PRRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchProviderException;
import java.security.Security;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.BouncyGPG;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.InMemoryKeyring;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfig;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfigs;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converting pgp encrypted input stream to the decrypted one using BouncyCastle library.
 */
public class BcPgpDecryptor {
    private static final Logger LOG = LoggerFactory.getLogger(BcPgpEncryptor.class);

    private final KeyringConfig keyringConfig;

    public BcPgpDecryptor(String pgpPrivateKey, String pgpPassPhrase) {
        this(pgpPrivateKey.getBytes(StandardCharsets.UTF_8), pgpPassPhrase);
    }

    public BcPgpDecryptor(byte[] privateKey, String pgpPassPhrase) {
        InMemoryKeyring keyRing;
        try {
            keyRing = KeyringConfigs.forGpgExportedKeys(keyId -> pgpPassPhrase.toCharArray());
            keyRing.addSecretKey(privateKey);
        } catch (IOException | PGPException e) {
            throw new PRRuntimeException("Couldn't instantiate a PGP Decryptor", e);
        }

        this.keyringConfig = keyRing;
    }

    public InputStream apply(InputStream inputStream) {
        Security.addProvider(new BouncyCastleProvider());
        PreventingDoubleCloseWrapper preventingDoubleCloseWrapper = new PreventingDoubleCloseWrapper(inputStream);

        try {
            final InputStream plaintextStream = BouncyGPG
                    .decryptAndVerifyStream()
                    .withConfig(keyringConfig)
                    .andIgnoreSignatures()
                    .fromEncryptedInputStream(preventingDoubleCloseWrapper);

            return new UnderlyingStreamCloser(plaintextStream, preventingDoubleCloseWrapper);
        } catch (NoSuchProviderException | IOException e) {
            LOG.error("Error during stream decryption", e);
            throw new PRRuntimeException("Could not decrypt an input stream", e);
        }
    }

    private static class InputStreamWrapper extends InputStream {

        private final InputStream wrapped;

        public InputStreamWrapper(InputStream wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return wrapped.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return wrapped.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return wrapped.skip(n);
        }

        @Override
        public int available() throws IOException {
            return wrapped.available();
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }

        @Override
        public synchronized void mark(int readlimit) {
            wrapped.mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            wrapped.reset();
        }

        @Override
        public boolean markSupported() {
            return wrapped.markSupported();
        }

        @Override
        public int read() throws IOException {
            return wrapped.read();
        }
    }

    /**
     * Closing underlying stream, because PGP encrypting stream doesn't do it in its close method
     */
    private static final class UnderlyingStreamCloser extends InputStreamWrapper {

        private final InputStream underlyingStream;

        private UnderlyingStreamCloser(InputStream encryptingStream, InputStream underlyingStream) {
            super(encryptingStream);
            this.underlyingStream = underlyingStream;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                underlyingStream.close();
            }
        }
    }

    /**
     * Makes sure that stream is closed only once even if close method is called multiple times.
     * We prevent multiple close just in case PGP encrypting stream implementation changes and it starts to close
     * underlying stream
     */
    private static final class PreventingDoubleCloseWrapper extends InputStreamWrapper {

        private boolean isClosed;

        private PreventingDoubleCloseWrapper(InputStream wrapped) {
            super(wrapped);
        }

        @Override
        public void close() throws IOException {
            if (!isClosed) {
                isClosed = true;
                super.close();
            }
        }
    }
}
