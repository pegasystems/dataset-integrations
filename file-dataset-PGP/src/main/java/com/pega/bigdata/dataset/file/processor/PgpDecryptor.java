package com.pega.bigdata.dataset.file.processor;

import com.pega.pegarules.pub.clipboard.ClipboardPage;
import java.io.InputStream;
import java.util.Base64;
import com.pega.bigdata.dataset.file.pgp.BcPgpDecryptor;

public class PgpDecryptor extends PgpProcessor<InputStream>{
    @Override
    public InputStream apply(InputStream inputStream) {
        ClipboardPage page = getClipboardPage();

        byte[] privateKey = Base64.getDecoder().decode(page.getString(PRIVATE_KEY_PROPERTY));
        String passphrase = page.getString(PASSPHRASE_PROPERTY);
        BcPgpDecryptor decryptor = new BcPgpDecryptor(privateKey, passphrase);

        return decryptor.apply(inputStream);
    }
}
