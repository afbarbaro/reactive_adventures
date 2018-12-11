package com.pci.stream.prices.controller;


import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.ehcache.Cache;
import org.springframework.lang.NonNull;

import com.pci.stream.prices.model.Price;
import com.pci.stream.prices.util.CacheManagerWrapper;

import reactor.core.publisher.FluxSink;

public class PricesEmitterAsync extends PricesEmitter
{
	private static final AtomicInteger emitterId = new AtomicInteger();

	// Immutable state, doesn't need to change once constructed.
	private final AtomicLong writeIndex;
	private final Semaphore semaphore;

	//Explain why these were made final.  Because these hold resources (Threads, etc.) these are only to be instantiated once.
	private final ScheduledExecutorService executor;
	private final CacheManagerWrapper cacheManager;
	private final Cache<Long, Price> cache;

	private long emissionIntervalInMillis;
	private EmissionInterval emissionInterval;
	private ScheduledFuture<?> periodicTask;
	private long frequencyChangeIndex;

	/**
	 * Emission Interval enumeration.
	 */
	static enum EmissionInterval
	{
		MILLI(1)
		{
			protected EmissionInterval next()
			{
				return SECOND;
			}
		},
		SECOND(1000)
		{
			protected EmissionInterval next()
			{
				return MINUTE;
			}
		},
		MINUTE(1000 * 60)
		{
			protected EmissionInterval next()
			{
				return MINUTE;
			}
		};

		private final long interval;
		private EmissionInterval(long interval)
		{
			this.interval = interval;
		}

		/**
		 * Parses the given string into an {@link EmissionInterval} with case-insensitive matching.
		 * 
		 * @param emissionInterval
		 *            Emission Interval String
		 * @return An {@link EmissionInterval}
		 * @throws IllegalArgumentException
		 *             if an invalid (unparseable) argument is given.
		 */
		public static EmissionInterval parse(String emissionInterval)
		{
			return valueOf(emissionInterval == null ? emissionInterval : emissionInterval.toUpperCase());
		}

		/**
		 * Returns the corresponding {@link EmissionInterval} for the given frequency matching the one that has the nearest interval that is greater or equal than the given argument.
		 * 
		 * @param interval
		 *            Frequency
		 * @return An {@link EmissionInterval}
		 * @throws IllegalArgumentException
		 *             if an invalid (unparseable) argument is given.
		 */
		public static EmissionInterval of(long interval)
		{
			EmissionInterval[] values = values();
			for (int i = 0; i < values.length; i++)
			{
				if (values[i].interval > interval)
				{
					return values[i > 0 ? i - 1 : i];
				}
			}
			return values[values.length - 1];
		}

		/**
		 * The next interval.
		 * 
		 * @return An {@link EmissionInterval}.
		 */
		protected abstract EmissionInterval next();

		public long getInterval()
		{
			return interval;
		}
	}

	/**
	 * Constructs an Asynchronous Price Emitter.
	 * 
	 * @param proxyPrices
	 *            A Map containing Proxy Prices that are use to generate prices at any interval by taking the applicable price and adding a random component on top of it.
	 * @param priceName
	 *            Price Name (for example a Stock Ticker such as TSLA)
	 * @param randomPercentage
	 *            Random percentage to apply to the corresponding Proxy Price to generate the emitted Price.
	 * @param intervalInMillis
	 *            Emission Interval in Milliseconds
	 * @param overflowThreshold
	 *            Overflow threshold (in % of the rate of emission)
	 */
	public PricesEmitterAsync(
			@NonNull Map<Integer, Price> proxyPrices,
			@NonNull String priceName,
			double randomPercentage,
			double overflowThreshold,
			long intervalInMillis)
	{
		super(proxyPrices, priceName, randomPercentage, overflowThreshold);

		this.writeIndex = new AtomicLong();
		this.semaphore = new Semaphore(0, true);

		int id = emitterId.getAndIncrement();
		this.executor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "price-async-emitter-" + id));

		this.cacheManager = new CacheManagerWrapper();
		this.cache = cacheManager.createCache("price_" + id);

		this.emissionIntervalInMillis = intervalInMillis;
		this.emissionInterval = EmissionInterval.of(intervalInMillis);
	}

	@Override
	public void accept(FluxSink<Price> sink)
	{
		periodicTask = executor.scheduleAtFixedRate(this::generate, 0, emissionInterval.getInterval(), TimeUnit.MILLISECONDS);
		emit(sink);
	}

	private void emit(FluxSink<Price> sink)
	{
		sink.onRequest(requested -> {
			System.out.println(LocalDateTime.now() + " - Emitter Received Request for " + requested + " items - (" + readIndex.get() + " - " + (readIndex.get() + requested) + ") / " + writeIndex.get());

			for (long i = 0; i < requested; i++)
			{
				if (sink.isCancelled() || executor.isShutdown())
				{
					return;
				}

				if (!acquire())
				{
					sink.complete();
					break;
				}

				long rIndex = readIndex.getAndIncrement();
				Price price = cache.get(rIndex);
				if (price == null)
				{
					System.out.println(LocalDateTime.now() + " - Price for " + rIndex + " was null");
				}
				else
				{
					sink.next(price);
					cache.remove(rIndex);
				}
			}
		});
	}

	private boolean acquire()
	{
		try
		{
			return semaphore.tryAcquire(Math.max(1_000, 2 * emissionIntervalInMillis), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e)
		{
			return false;
		}
	}

	private Price generate()
	{
		Price price = generateFor(writeIndex.getAndIncrement());

		cache.put(price.getId(), price);

		semaphore.release();

		adjustFrequencyIfNeeded();

		return price;
	}

	/**
	 * Reschedule at a different rate if the consumer is not keeping up.
	 */
	private void adjustFrequencyIfNeeded()
	{
		long overflowMax = (long) (emissionInterval.next().getInterval() * overflowThreshold);
		long wIndex = writeIndex.get();
		long rIndex = readIndex.get();
		boolean adjust = wIndex > EmissionInterval.SECOND.getInterval() && rIndex > frequencyChangeIndex &&
				(overflowCount.get() > overflowMax || (wIndex - rIndex) > overflowMax);

		if (adjust)
		{
			System.out.println(LocalDateTime.now() + " - Reducing Frequency of Emitter from " + emissionInterval + " to " + emissionInterval.next()
					+ " due to excesive accumulation (" + wIndex + " - " + rIndex + ", " + overflowCount.get() + ")");

			overflowCount.set(0);
			frequencyChangeIndex = wIndex;
			emissionInterval = emissionInterval.next();
			emissionIntervalInMillis = emissionInterval.getInterval();
			periodicTask.cancel(true);
			periodicTask = executor.scheduleAtFixedRate(this::generate, 0, emissionInterval.getInterval(), TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void close()
	{
		executor.shutdownNow();
		cacheManager.close();
	}
}