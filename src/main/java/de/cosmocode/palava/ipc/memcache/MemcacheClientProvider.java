package de.cosmocode.palava.ipc.memcache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.transcoders.BaseSerializingTranscoder;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

/**
 * Provider for {@link MemcachedClientIF}.
 *
 * @since 3.0
 * @author Willi Schoenborn
 */
final class MemcacheClientProvider implements Provider<MemcachedClientIF> {

    private final List<InetSocketAddress> addresses;
    private boolean binary;
    private int compressionThreshold = -1;
    private HashAlgorithm hashAlgorithm = HashAlgorithm.NATIVE_HASH;
    
    @Inject
    public MemcacheClientProvider(@Named(MemcacheConfig.ADRESSES) String addresses) {
        Preconditions.checkNotNull(addresses, "Addresses");
        this.addresses = AddrUtil.getAddresses(addresses);
    }

    @Inject(optional = true)
    void setBinary(@Named(MemcacheConfig.BINARY) boolean binary) {
        this.binary = binary;
    }
    
    @Inject(optional = true)
    void setCompressionThreshold(@Named(MemcacheConfig.COMPRESSION_THRESHOLD) int compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
    }
    
    @Inject(optional = true)
    void setHashAlgorithm(@Named(MemcacheConfig.HASH_ALGORITHM) HashAlgorithm hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }
        
    @Override
    public MemcachedClientIF get() {
        final ConnectionFactory factory;
        
        if (binary) {
            factory = new BinaryConnectionFactory(
                    BinaryConnectionFactory.DEFAULT_OP_QUEUE_LEN,
                    BinaryConnectionFactory.DEFAULT_READ_BUFFER_SIZE,
                    hashAlgorithm
            );
        } else {
            factory = new DefaultConnectionFactory(
                    DefaultConnectionFactory.DEFAULT_OP_QUEUE_LEN,
                    DefaultConnectionFactory.DEFAULT_READ_BUFFER_SIZE,
                    hashAlgorithm
            );
        }
        
        try {
            final MemcachedClient client = new MemcachedClient(factory, addresses);

            if (compressionThreshold >= 0) {
                if (client.getTranscoder() instanceof BaseSerializingTranscoder) {
                    final BaseSerializingTranscoder transcoder = (BaseSerializingTranscoder) client.getTranscoder();
                    transcoder.setCompressionThreshold(compressionThreshold);
                } else {
                    throw new UnsupportedOperationException(
                        "cannot set compression threshold; transcoder does not extend BaseSeralizingTranscoder");
                }
            }

            return new DestroyableMemcachedClient(client);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
    
}
