package com.pci.stream.prices.util;

import org.springframework.data.repository.CrudRepository;

import com.pci.stream.prices.model.Price;

public interface PricesRepository extends CrudRepository<Price, String> //ReactiveCrudRepository<Price, String>
{
}
