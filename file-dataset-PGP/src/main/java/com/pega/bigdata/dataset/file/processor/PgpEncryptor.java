package com.pega.bigdata.dataset.file.processor;

import com.pega.pegarules.pub.clipboard.ClipboardPage;
import java.io.OutputStream;
import java.util.Base64;
import com.pega.bigdata.dataset.file.pgp.BcPgpEncryptor;

public class PgpEncryptor extends PgpProcessor<OutputStream> {
    @Override
    public OutputStream apply(OutputStream outputStream) {
        ClipboardPage page = getClipboardPage();

        byte[] publicKey = Base64.getDecoder().decode(page.getString(PUBLIC_KEY_PROPERTY));
        BcPgpEncryptor encryptor = new BcPgpEncryptor(publicKey);

        return encryptor.apply(outputStream);
    }
}
