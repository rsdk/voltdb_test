CREATE TABLE transfer (
    transfer_time TIMESTAMP,
    card_num BIGINT NOT NULL,
    amount DECIMAL NOT NULL,
    purpose VARCHAR(64),
    latitude FLOAT,
    longitude FLOAT,
    country_code VARCHAR(2),
    PRIMARY KEY (card_num, transfer_time)
);
CREATE TABLE card (
    card_num BIGINT NOT NULL,
    daily_limit DECIMAL,
    monthly_limit DECIMAL,
    blocked TINYINT,
    distance_per_hour_max SMALLINT,
    customer_name VARCHAR(64),
    PRIMARY KEY (card_num)
);
CREATE TABLE country_specific_per_card (
    card_num BIGINT NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    disallowed TINYINT,
    daily_limit DECIMAL,
    PRIMARY KEY (card_num, country_code)
);
CREATE TABLE country_specific (
    country_code VARCHAR(2) NOT NULL,
    disallowed TINYINT,
    daily_limit DECIMAL,
    PRIMARY KEY (country_code)
);
CREATE TABLE countries (
    country_code VARCHAR(2) NOT NULL UNIQUE,
    country_name VARCHAR(64) NOT NULL,
    notes VARCHAR(200)
);
CREATE INDEX country_specific_per_card_idx ON country_specific_per_card (card_num, country_code);
CREATE INDEX transfer_idx ON transfer (card_num, country_code, transfer_time);

PARTITION TABLE transfer ON COLUMN card_num;
PARTITION TABLE card ON COLUMN card_num;
PARTITION TABLE country_specific_per_card ON COLUMN card_num;
PARTITION TABLE country_specific ON COLUMN country_code;
PARTITION TABLE countries ON COLUMN country_code;

CREATE PROCEDURE FROM CLASS Insert_country;
CREATE PROCEDURE FROM CLASS Insert_country_specific;
CREATE PROCEDURE FROM CLASS Insert_country_specific_per_card;
CREATE PROCEDURE FROM CLASS Insert_card;
CREATE PROCEDURE FROM CLASS Select_all_countries;

CREATE PROCEDURE FROM CLASS new_transfer;

PARTITION PROCEDURE Insert_country ON TABLE countries COLUMN country_code;
PARTITION PROCEDURE new_transfer ON TABLE transfer COLUMN card_num;
