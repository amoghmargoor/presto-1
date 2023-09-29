/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.alluxio;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class AlluxioFileSystemCacheConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    static final String CACHE_DIRECTORIES = "fs.cache.directories";
    static final String CACHE_MAX_SIZES = "fs.cache.max-sizes";
    static final String CACHE_MAX_PERCENTAGES = "fs.cache.max-disk-usage-percentages";

    private List<String> cacheDirectories;
    private List<DataSize> maxCacheSizes = ImmutableList.of();
    private Duration cacheTTL = Duration.valueOf("7d");
    private List<Double> maxCacheDiskUsagePercentages = ImmutableList.of();
    private DataSize cachePageSize = DataSize.valueOf("1MB");
    private boolean shadowCacheEnabled;

    @NotNull
    public List<String> getCacheDirectories()
    {
        return cacheDirectories;
    }

    @Config(CACHE_DIRECTORIES)
    @ConfigDescription("Base directory to cache data. A comma-separated list of directories to use")
    public AlluxioFileSystemCacheConfig setCacheDirectories(String cacheDirectories)
    {
        this.cacheDirectories = cacheDirectories == null ? null : SPLITTER.splitToList(cacheDirectories);
        return this;
    }

    public List<DataSize> getMaxCacheSizes()
    {
        return maxCacheSizes;
    }

    @Config(CACHE_MAX_SIZES)
    @ConfigDescription("The maximum cache sizes available for cache. A comma-separated list of sizes if supplying several cache directories")
    public AlluxioFileSystemCacheConfig setMaxCacheSizes(String maxCacheSizes)
    {
        this.maxCacheSizes = SPLITTER.splitToStream(firstNonNull(maxCacheSizes, "")).map(DataSize::valueOf).collect(toImmutableList());
        return this;
    }

    @NotNull
    @MinDuration("0s")
    public Duration getCacheTTL()
    {
        return cacheTTL;
    }

    @Config("fs.cache.ttl")
    @ConfigDescription("The ttl of the cache")
    public AlluxioFileSystemCacheConfig setCacheTTL(Duration cacheTTL)
    {
        this.cacheTTL = cacheTTL;
        return this;
    }

    public List<Double> getMaxCacheDiskUsagePercentages()
    {
        return maxCacheDiskUsagePercentages;
    }

    @Config(CACHE_MAX_PERCENTAGES)
    @ConfigDescription("The maximum percentage (0.0-1.0) of total disk size the cache can use. A comma-separated list of percentages if supplying several cache directories")
    public AlluxioFileSystemCacheConfig setMaxCacheDiskUsagePercentages(String maxCacheDiskUsagePercentages)
    {
        List<Double> values = SPLITTER.splitToStream(firstNonNull(maxCacheDiskUsagePercentages, ""))
                .map(Double::valueOf)
                .collect(toImmutableList());
        values.forEach(p -> checkArgument(0.0 <= p && p <= 1.0, format("Percentage %f must be between 0 and 1", p)));
        this.maxCacheDiskUsagePercentages = values;
        return this;
    }

    @MaxDataSize("1GB")
    public DataSize getCachePageSize()
    {
        return this.cachePageSize;
    }

    @Config("fs.cache.alluxio.page-size")
    @ConfigDescription("Page size of Alluxio cache")
    public AlluxioFileSystemCacheConfig setCachePageSize(DataSize cachePageSize)
    {
        this.cachePageSize = cachePageSize;
        return this;
    }

    public boolean isShadowCacheEnabled()
    {
        return shadowCacheEnabled;
    }

    @Config("fs.cache.alluxio.shadow-cache")
    @ConfigDescription("Enable shadow cache")
    public AlluxioFileSystemCacheConfig setShadowCacheEnabled(boolean shadowCacheEnabled)
    {
        this.shadowCacheEnabled = shadowCacheEnabled;
        return this;
    }
}
