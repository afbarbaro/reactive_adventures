package com.pci.stream.prices.util;

import java.time.LocalDateTime;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.Status;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.event.EventType;
import org.ehcache.expiry.ExpiryPolicy;

import com.pci.stream.prices.model.Price;

public class CacheManagerWrapper implements AutoCloseable
{
	private final CacheManager cacheManager;

	public CacheManagerWrapper()
	{
		cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
	}

	/**
	 * Creates a new Cache with the given name.
	 * 
	 * @param name
	 *            The name of the Cache
	 * @return The created Cache
	 */
	public Cache<Long, Price> createCache(String name)
	{
		CacheConfiguration<Long, Price> cacheConfig = CacheConfigurationBuilder
				.newCacheConfigurationBuilder(Long.class, Price.class, ResourcePoolsBuilder.heap(10_000_000L))
				.withExpiry(ExpiryPolicy.NO_EXPIRY)
				.withEvictionAdvisor((k, v) -> true)
				.add(CacheEventListenerConfigurationBuilder
						.newEventListenerConfiguration(event -> {
							System.out.println(LocalDateTime.now() + " - Entry for Key " + event.getKey() + " Evicted");
						}, EventType.EVICTED))
				.build();

		return cacheManager.createCache(name, cacheConfig);
	}

	/**
	 * Closes the underlying {@link CacheManager}.
	 */
	public void close()
	{
		if (Status.UNINITIALIZED != cacheManager.getStatus())
		{
			cacheManager.close();
		}
	}
}