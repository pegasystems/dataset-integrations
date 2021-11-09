package com.pega.bigdata.dataset.file.pgp;/*
 *
 * Copyright (c) 2019 Pegasystems Inc.
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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.SignatureException;
import java.util.Date;
import java.util.Iterator;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.BouncyGPG;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.algorithms.PGPAlgorithmSuite;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.algorithms.PGPCompressionAlgorithms;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.algorithms.PGPHashAlgorithms;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.algorithms.PGPSymmetricEncryptionAlgorithms;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.callbacks.Rfc4880KeySelectionStrategy;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.InMemoryKeyring;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfig;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfigs;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converting output stream to the encrypted one using BouncyCastle library.
 */
public class BcPgpEncryptor {

    private static final Logger LOG = LoggerFactory.getLogger(BcPgpEncryptor.class);

    private final KeyringConfig keyringConfig;
    private final String recipientId;

    public BcPgpEncryptor(String pgpPublicKey) {
        this(pgpPublicKey.getBytes(StandardCharsets.UTF_8));
    }

    public BcPgpEncryptor(byte[] publicKey) {
        try {
            InMemoryKeyring keyRing = KeyringConfigs.forGpgExportedKeys(keyId -> null);
            keyRing.addPublicKey(publicKey);

            this.keyringConfig = keyRing;
            this.recipientId = new String(getRecipientId(keyRing), StandardCharsets.UTF_8);
        } catch (PGPException | IOException e) {
            throw new PRRuntimeException("Couldn't instantiate a PGP Encryptor", e);
        }
    }

    public OutputStream apply(OutputStream outputStream) {
        Security.addProvider(new BouncyCastleProvider());
        PreventingDoubleCloseWrapper preventingDoubleCloseWrapper = new PreventingDoubleCloseWrapper(outputStream);
        OutputStream encrypted;
        try {
            encrypted = BouncyGPG
                    .encryptToStream()
                    .withConfig(this.keyringConfig)
                    .withKeySelectionStrategy(new Rfc4880KeySelectionStrategy(new Date().toInstant()))
                    .withAlgorithms(new PGPAlgorithmSuite(
                            PGPHashAlgorithms.SHA_256,
                            PGPSymmetricEncryptionAlgorithms.AES_256,
                            PGPCompressionAlgorithms.UNCOMPRESSED))
                    .toRecipient(this.recipientId)
                    .andDoNotSign()
                    .binaryOutput()
                    .andWriteTo(preventingDoubleCloseWrapper);
            return new UnderlyingStreamCloser(encrypted, preventingDoubleCloseWrapper);
        } catch (PGPException | SignatureException | NoSuchAlgorithmException | NoSuchProviderException | IOException e) {
             LOG.error("Error during stream encryption", e);
            throw new PRRuntimeException("Could not encrypt an output stream", e);
        }
    }

    private static byte[] getRecipientId(KeyringConfig keyringConfig) {
        try {
            for (Iterator<PGPPublicKeyRing> iterator = keyringConfig.getPublicKeyRings().getKeyRings(); iterator.hasNext(); ) {
                PGPPublicKeyRing publicKeyRing = iterator.next();
                if (publicKeyRing.getPublicKey().getRawUserIDs().hasNext()) {
                    return publicKeyRing.getPublicKey().getRawUserIDs().next();
                }
            }
        } catch (IOException | PGPException e) {
            LOG.error("Getting a keyring failed, please verify that public key is correct", e);
        }

        throw new PRRuntimeException("Couldn't find a PGP recipient UID in a keyring");
    }

    private static class OutputStreamWrapper extends OutputStream {
        private final OutputStream wrapped;

        public OutputStreamWrapper(OutputStream wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public void write(byte[] b) throws IOException {
            wrapped.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            wrapped.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            wrapped.flush();
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }

        @Override
        public void write(int b) throws IOException {
            wrapped.write(b);
        }
    }

    /**
     * Closing underlying stream, because PGP encrypting stream doesn't do it in its close method
     */
    private static final class UnderlyingStreamCloser extends OutputStreamWrapper {

        private final OutputStream underlyingStream;

        UnderlyingStreamCloser(OutputStream encryptingStream, OutputStream underlyingStream) {
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
    private static final class PreventingDoubleCloseWrapper extends OutputStreamWrapper {

        private boolean isClosed;

        private PreventingDoubleCloseWrapper(OutputStream wrapped) {
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
