package com.pci.stream.prices.model;

import java.io.Serializable;
import java.sql.Timestamp;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(name = "prices")
@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Price implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Id
	private long id;
	private String name;
	private Timestamp time;
	private Double price;
}
