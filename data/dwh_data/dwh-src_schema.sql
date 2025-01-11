-- public.aircrafts definition

-- Drop table

-- DROP TABLE aircrafts;
CREATE SCHEMA IF NOT EXISTS pactravel AUTHORIZATION postgres;

CREATE TABLE pactravel.aircrafts (
	aircraft_name varchar NULL,
	aircraft_iata varchar NULL,
	aircraft_icao varchar NULL,
	aircraft_id varchar NOT NULL,
	CONSTRAINT aircrafts_pkey PRIMARY KEY (aircraft_id)
);


-- public.airlines definition

-- Drop table

-- DROP TABLE airlines;

CREATE TABLE pactravel.airlines (
	airline_id int4 NOT NULL,
	airline_name varchar NULL,
	country varchar NULL,
	airline_iata varchar NULL,
	airline_icao varchar NULL,
	alias varchar NULL,
	CONSTRAINT airlines_pkey PRIMARY KEY (airline_id)
);


-- public.airports definition

-- Drop table

-- DROP TABLE airports;

CREATE TABLE pactravel.airports (
	airport_id int4 NOT NULL,
	airport_name varchar NULL,
	city varchar NULL,
	latitude float8 NULL,
	longitude float8 NULL,
	CONSTRAINT airports_pkey PRIMARY KEY (airport_id)
);


-- public.customers definition

-- Drop table

-- DROP TABLE customers;

CREATE TABLE pactravel.customers (
	customer_id int4 NOT NULL,
	customer_first_name varchar NULL,
	customer_family_name varchar NULL,
	customer_gender varchar NULL,
	customer_birth_date date NULL,
	customer_country varchar NULL,
	customer_phone_number int8 NULL,
	CONSTRAINT customers_pkey PRIMARY KEY (customer_id)
);


-- public.hotel definition

-- Drop table

-- DROP TABLE hotel;

CREATE TABLE pactravel.hotel (
	hotel_id int4 NOT NULL,
	hotel_name varchar NULL,
	hotel_address varchar NULL,
	city varchar NULL,
	country varchar NULL,
	hotel_score float8 NULL,
	CONSTRAINT hotel_pkey PRIMARY KEY (hotel_id)
);


-- public.flight_bookings definition

-- Drop table

-- DROP TABLE flight_bookings;

CREATE TABLE pactravel.flight_bookings (
	trip_id int4 NOT NULL,
	customer_id int4 NULL,
	flight_number varchar(32) NOT NULL,
	airline_id int4 NULL,
	aircraft_id varchar(32) NULL,
	airport_src int4 NULL,
	airport_dst int4 NULL,
	departure_time time NULL,
	departure_date date NULL,
	flight_duration varchar(32) NULL,
	travel_class varchar(32) NULL,
	seat_number varchar(32) NOT NULL,
	price int4 NULL,
	CONSTRAINT flight_bookings_pkey PRIMARY KEY (trip_id, flight_number, seat_number),
	CONSTRAINT fk_flight_aircraft FOREIGN KEY (aircraft_id) REFERENCES aircrafts(aircraft_id),
	CONSTRAINT fk_flight_airline FOREIGN KEY (airline_id) REFERENCES airlines(airline_id),
	CONSTRAINT fk_flight_airport_dst FOREIGN KEY (airport_dst) REFERENCES airports(airport_id),
	CONSTRAINT fk_flight_airport_src FOREIGN KEY (airport_src) REFERENCES airports(airport_id),
	CONSTRAINT fk_flight_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);


-- public.hotel_bookings definition

-- Drop table

-- DROP TABLE hotel_bookings;

CREATE TABLE pactravel.hotel_bookings (
	trip_id int4 NOT NULL,
	customer_id int4 NULL,
	hotel_id int4 NULL,
	check_in_date date NULL,
	check_out_date date NULL,
	price int4 NULL,
	breakfast_included bool NULL,
	CONSTRAINT hotel_bookings_pkey PRIMARY KEY (trip_id),
	CONSTRAINT fk_hotel_booking_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
	CONSTRAINT fk_hotel_booking_hotel FOREIGN KEY (hotel_id) REFERENCES hotel(hotel_id)
);