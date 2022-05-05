CREATE SCHEMA IF NOT EXISTS exercise_5;

CREATE TABLE IF NOT EXISTS exercise_5.accounts (
    customer_id int PRIMARY KEY,
    first_name varchar(255) NOT NULL,
    last_name varchar(255) NOT NULL,
    address_1 varchar(255),
    address_2 varchar(255),
    city varchar(255),
    state varchar(255),
    zip_code int,
    join_date varchar(255)
);

CREATE TABLE IF NOT EXISTS exercise_5.products (
    product_id int PRIMARY KEY,
    product_code varchar(255) NOT NULL,
    product_description varchar(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS exercise_5.transactions (
    transaction_id varchar(255) PRIMARY KEY,
    transaction_date varchar(255) NOT NULL,
    product_id int NOT NULL,
    product_code varchar(255) NOT NULL,
    product_description varchar(255) NOT NULL,
    quantity int NOT NULL,
    account_id int NOT NULL,
    FOREIGN KEY (product_id) REFERENCES exercise_5.products (product_id),
    FOREIGN KEY (account_id) REFERENCES exercise_5.accounts (customer_id)
);

