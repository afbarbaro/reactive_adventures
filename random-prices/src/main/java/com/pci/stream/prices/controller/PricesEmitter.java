package com.pci.stream.prices.controller;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
// public class PricesEmitter implements
// {
// repo.findAll()
// }
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.springframework.lang.NonNull;

import com.pci.stream.prices.model.Price;

import reactor.core.publisher.FluxSink;

/**
 * A simple emitter of prices.
 * 
 * @author abarbaro
 */
public class PricesEmitter implements Consumer<FluxSink<Price>>, AutoCloseable
{
	protected final Map<Integer, Price> proxyPrices;
	protected final String priceName;
	protected final double randomPercentage;
	protected final double overflowThreshold;
	protected final Random random;
	protected final AtomicLong readIndex;
	protected final AtomicLong overflowCount;

	/**
	 * Constructs a Price Emitter.
	 * 
	 * @param proxyPrices
	 *            A Map containing Proxy Prices that are use to generate prices at any interval by taking the applicable price and adding a random component on top of it.
	 * @param priceName
	 *            Price Name (for example a Stock Ticker such as TSLA)
	 * @param randomPercentage
	 *            Random percentage to apply to the corresponding Proxy Price to generate the emitted Price.
	 * @param overflowThreshold
	 *            Overflow threshold (in % of the rate of emission)
	 */
	public PricesEmitter(
			@NonNull Map<Integer, Price> proxyPrices,
			@NonNull String priceName,
			double randomPercentage,
			double overflowThreshold)
	{
		this.proxyPrices = proxyPrices;
		this.priceName = priceName;
		this.randomPercentage = randomPercentage;
		this.overflowThreshold = overflowThreshold;
		this.random = new Random(0);
		this.readIndex = new AtomicLong();
		this.overflowCount = new AtomicLong();
	}

	@Override
	public void accept(FluxSink<Price> sink)
	{
		emit(sink);
	}

	private void emit(FluxSink<Price> sink)
	{
		sink.onRequest(
				requested -> {
					System.out.println(LocalDateTime.now() + " - Emitter Received Request for " + requested + " items - (" + readIndex.get() + " - " + (readIndex.get() + requested) + ")");

					for (long i = 0; i < requested; i++)
					{
						if (sink.isCancelled())
						{
							return;
						}

						long index = readIndex.getAndIncrement();
						Price price = generateFor(index);
						sink.next(price);
					}
				});
	}

	/**
	 * Increments and returns the total number of overflows that have occurs so far.
	 * 
	 * @return The total number of overflows.
	 */
	public long bufferOverflow()
	{
		return overflowCount.incrementAndGet();
	}

	/**
	 * Generates an array of n Prices.
	 * 
	 * @param n
	 *            Array size
	 * @return An array of Prices of length n.
	 */
	public final Price[] generate(int n)
	{
		Price[] data = new Price[n];
		for (int i = 0; i < n; i++)
		{
			data[i] = generateFor(i);
		}
		return data;
	}

	/**
	 * Generates a price for the given index.
	 * 
	 * @param index
	 *            Index
	 * @return Generated Price
	 */
	protected Price generateFor(long index)
	{
		LocalDateTime time = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
		Price proxy = proxyPrices.get(time.get(ChronoField.HOUR_OF_DAY));
		Price price = new Price(index, priceName, toTimestamp(time), randomize(proxy));

		return price;
	}

	protected Timestamp toTimestamp(LocalDateTime time)
	{
		return new Timestamp(time.toInstant(ZoneId.systemDefault().getRules().getOffset(time)).toEpochMilli());
	}

	protected double randomize(Price proxy)
	{
		return BigDecimal.valueOf(proxy.getPrice() * (1 + (random.nextDouble() - 0.5) * randomPercentage * 2)).setScale(4, RoundingMode.HALF_UP).doubleValue();
	}

	@Override
	public void close()
	{
	}
}