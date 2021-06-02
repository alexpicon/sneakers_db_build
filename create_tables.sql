-- Creates the schema for the sneakers database.

CREATE TABLE brands
(
    brand text,
    PRIMARY KEY (brand)
);

CREATE TABLE genders
(
    gender text,
    PRIMARY KEY (gender)
);

CREATE TABLE sneakers
(
    sku                  text,
    brand                text NOT NULL,
    colorway             text,
    estimatedMarketValue integer,
    gender               text NOT NULL,
    image_original       text,
    image_small          text,
    image_thumbnail      text,
    link_flightClub      text,
    link_goat            text,
    link_stadiumGoods    text,
    link_stockX          text,
    name                 text,
    releaseDate          date,
    releaseYear          integer CHECK (releaseYear >= 0),
    retailPrice          integer,
    silhouette           text,
    story                text,
    PRIMARY KEY (sku),
    FOREIGN KEY (brand) REFERENCES brands (brand),
    FOREIGN KEY (gender) REFERENCES genders (gender)
);

CREATE TABLE images_360
(
    sku      text,
    position integer,
    image    text,
    FOREIGN KEY (sku) REFERENCES sneakers (sku)
);

-- full text search table
CREATE
VIRTUAL TABLE sneakers_fts
USING FTS5(`name`, brand, silhouette, colorway, sku, content='', tokenize='porter unicode61 remove_diacritics 2');

-- a ranker that makes sense when searching for sneakers
-- trust me, I know what I'm doing
INSERT INTO sneakers_fts(sneakers_fts, rank)
VALUES ('rank', 'bm25(5.0, 3.0, 2.0, 1.0, 1.0)');
