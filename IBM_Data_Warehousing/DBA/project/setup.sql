CREATE DATABASE tolldata;
\c tolldata;
CREATE SCHEMA toll;

CREATE TABLE toll.tolldata(
row_id integer,
timestamp varchar(25),
vehicle_id integer,
vehicle_type varchar(10),
payment_type integer,
category_id varchar(10),
primary key (row_id)
);
