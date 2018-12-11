package com.pci.stream.prices;

import java.io.File;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pci.stream.prices.model.Price;
import com.pci.stream.prices.util.PricesRepository;

@EnableCaching
@SpringBootApplication
public class PricesApplication
{
	public static void main(String[] args)
	{
		SpringApplication.run(PricesApplication.class, args);
	}

	@Bean
	CommandLineRunner demoData(PricesRepository repo)
	{
		return args -> {
			repo.deleteAll();

			File file = new ClassPathResource("proxy-price.json").getFile();
			if (file.exists())
			{
				double[] prices = new ObjectMapper().readValue(file, double[].class);

				LocalDateTime today = LocalDateTime.now().truncatedTo(ChronoUnit.DAYS);
				List<Price> priceList = new ArrayList<>();

				for (int t = 0; t < prices.length; t++)
				{
					LocalDateTime tick = today.plus(t, ChronoUnit.HOURS);
					Timestamp time = new Timestamp(tick.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
					priceList.add(new Price(tick.get(ChronoField.SECOND_OF_DAY), "DUMMY", time, prices[t]));
				}

				repo.saveAll(priceList);
			}
		};
	}
}
