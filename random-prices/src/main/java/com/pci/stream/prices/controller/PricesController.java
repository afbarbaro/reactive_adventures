package com.pci.stream.prices.controller;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.pci.stream.prices.controller.PricesEmitterAsync.EmissionInterval;
import com.pci.stream.prices.model.Price;
import com.pci.stream.prices.util.PricesRepository;

import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;

@RestController
public class PricesController
{
	@Value("${prices.controller.emission.interval}")
	private String emissionInterval;

	@Value("${prices.controller.emission.randomness}")
	private double emissionRandomness;

	@Value("${prices.controller.emission.overflow-threshold}")
	private double emissionOverflowThreshold;

	@Value("${prices.controller.backpressure.enabled}")
	private boolean backPressureEnabled;

	@Value("${prices.controller.backpressure.max-size}")
	private int backPressureMaxSize;

	@Value("${prices.controller.logging.on-request}")
	private boolean logOnRequest;

	private final PricesRepository repo;

	public PricesController(PricesRepository repo)
	{
		this.repo = repo;
	}

	/**
	 * Gets a {@link Flux} of prices.
	 * 
	 * @param name
	 *            price name (for example a stock ticker such as TSLA).
	 * @return A {@link Flux} of containing elements of type {@link Price}.
	 */
	@GetMapping(value = "/prices/{name}", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
	public Flux<Price> get(@PathVariable String name)
	{
		// Read price data from the Database Repository
		// TODO: this needs to actually get the data from a real database CRUD service
		Map<Integer, Price> priceMap = StreamSupport.stream(repo.findAll().spliterator(), false).collect(
				Collectors.toMap(
						p -> LocalDateTime.ofInstant(Instant.ofEpochMilli(p.getTime().getTime()), ZoneId.systemDefault()).get(ChronoField.HOUR_OF_DAY),
						Function.identity()));

		// Instantiate Emitter based on the specified rate
		PricesEmitter emitter = emissionInterval.isEmpty()
				? new PricesEmitter(priceMap, name, emissionRandomness, emissionOverflowThreshold)
				: new PricesEmitterAsync(priceMap, name, emissionRandomness, emissionOverflowThreshold, EmissionInterval.parse(emissionInterval).getInterval());

		// Create Flux, with the desired options
		Flux<Price> flux = withLogging(
				withBackPressureBuffer(emitter,
						Flux.push(emitter)
								.doOnCancel(emitter::close)
								.doOnTerminate(emitter::close)));

		// Return
		return flux;
	}

	private Flux<Price> withBackPressureBuffer(final PricesEmitter emitter, final Flux<Price> flux)
	{
		if (backPressureEnabled)
		{
			AtomicBoolean overflown = new AtomicBoolean();

			// Because back-pressure is on, we enable the buffer
			return flux.onBackpressureBuffer(backPressureMaxSize, p -> {
				// If we haven't logged an overflow, we log it only once.
				if (!overflown.getAndSet(true))
				{
					System.out.println(LocalDateTime.now() + " - Producer Overwhelming Consumer " + p);
				}

				// Now we let the emitter know that a buffer overflow has occured.
				emitter.bufferOverflow();
			}, BufferOverflowStrategy.DROP_OLDEST);
		}

		return flux;
	}

	private Flux<Price> withLogging(Flux<Price> flux)
	{
		if (logOnRequest)
		{
			return flux.doOnRequest(l -> {
				System.out.println(LocalDateTime.now() + " - Producer Received Request for " + l + " items");
			});
		}
		return flux;
	}
}