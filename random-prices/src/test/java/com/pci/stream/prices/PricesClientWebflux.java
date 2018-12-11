package com.pci.stream.prices;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;

import javax.net.ssl.SSLException;

import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.util.Base64Utils;
import org.springframework.web.reactive.function.client.WebClient;

import com.pci.stream.prices.model.Price;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import reactor.core.Disposable;
import reactor.netty.http.client.HttpClient;

public final class PricesClientWebflux
{
	/**
	 * Main method for testing.
	 * 
	 * @throws IOException
	 *             If an error occurs processing the Web response.
	 */
	@Test
	public void test() throws IOException
	{
		final String uri = "https://localhost:6901/prices/TSLA";

		setupLogging();

		Disposable subscriber = client(uri, "Admin", "hola")//WebClient.create(uri)
				.get()
				.accept(MediaType.APPLICATION_STREAM_JSON)
				.retrieve()
				.bodyToFlux(Price.class)

				//	.onBackpressureBuffer(100, p -> {
				//		System.out.println(LocalDateTime.now() + " - Client Buffer Overflown " + p);
				//	}, BufferOverflowStrategy.DROP_OLDEST)


				.delayElements(Duration.ofMillis(1))

				.doOnEach(s -> {
					System.out.println(LocalDateTime.now() + " - Received " + s.get());
				})

				.subscribe();

		waitUntilDisposed(subscriber);
	}

	/**
	 * Keep waiting until subscriber is disposed.
	 * 
	 * @param subscriber
	 *            Subscriber
	 */
	private static void waitUntilDisposed(Disposable subscriber)
	{
		while (!subscriber.isDisposed())
		{
			sleep(1000);
		}
	}

	/**
	 * Needed to remove debug logging that is super verbose.
	 */
	private static void setupLogging()
	{
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("io.netty")).setLevel(ch.qos.logback.classic.Level.INFO);
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("reactor.ipc.netty")).setLevel(ch.qos.logback.classic.Level.INFO);
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("reactor.netty.channel")).setLevel(ch.qos.logback.classic.Level.INFO);
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("org.springframework.http.codec.json")).setLevel(ch.qos.logback.classic.Level.INFO);
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("org.springframework.web")).setLevel(ch.qos.logback.classic.Level.INFO);
	}

	/**
	 * Sleep without throwing {@link InterruptedException}.
	 * 
	 * @param millis
	 *            Milliseconds to sleep
	 */
	protected static void sleep(int millis)
	{
		try
		{
			Thread.sleep(millis);
		}
		catch (InterruptedException ie)
		{
			// Ignore
		}
	}

	private static WebClient client(String uri, String username, String pwd) throws SSLException
	{
		SslContext sslContext = SslContextBuilder
				.forClient()
				.trustManager(InsecureTrustManagerFactory.INSTANCE)
				.build();

		HttpClient httpClient = HttpClient.create().secure(t -> t.sslContext(sslContext));

		return WebClient.builder()
				.clientConnector(new ReactorClientHttpConnector(httpClient))
				.baseUrl(uri)
				.defaultHeader("Authorization", "Basic " + Base64Utils.encodeToString((username + ":" + pwd).getBytes(StandardCharsets.UTF_8)))
				.build();
	}
}