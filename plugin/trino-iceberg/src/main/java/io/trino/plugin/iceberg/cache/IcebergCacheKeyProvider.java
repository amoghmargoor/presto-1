package io.trino.plugin.iceberg.cache;

import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.cache.CacheKeyProvider;

import java.io.IOException;
import java.util.Optional;

public class IcebergCacheKeyProvider
        implements CacheKeyProvider {
    @Override
    public Optional<String> getCacheKey(TrinoInputFile delegate)
            throws IOException
    {
        return Optional.of(delegate.location().path());
    }
}
