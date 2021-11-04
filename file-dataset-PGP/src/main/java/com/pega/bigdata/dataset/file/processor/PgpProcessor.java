package com.pega.bigdata.dataset.file.processor;

import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.context.ThreadContainer;
import com.pega.pegarules.pub.runtime.PublicAPI;
import java.util.function.Function;

abstract class PgpProcessor<T> implements Function<T, T> {
    private final String KEYS_PAGE_NAME = "D_PGPKeys";
    protected final String PRIVATE_KEY_PROPERTY = "PrivateKey";
    protected final String PUBLIC_KEY_PROPERTY = "PublicKey";
    protected final String PASSPHRASE_PROPERTY = "Passphrase";

    protected final ClipboardPage getClipboardPage(){
        PublicAPI tools = ThreadContainer.get().getPublicAPI();
        return tools.findPage(KEYS_PAGE_NAME);
    }
}
