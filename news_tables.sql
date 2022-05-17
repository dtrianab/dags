-- create pet table
CREATE TABLE IF NOT EXISTS newsapi (
    title VARCHAR,
    author VARCHAR,
    source_id VARCHAR,
    source_name VARCHAR,
    description_news VARCHAR,
    content VARCHAR,
    pub_date DATE,
    url_news VARCHAR,
    photo_url VARCHAR);


CREATE TABLE IF NOT EXISTS FinViz (
    news_id  SERIAL PRIMARY KEY,
    ticker VARCHAR,
    date_news date,
    time_news time,
    headline VARCHAR,
    url_news VARCHAR,
    neg VARCHAR,
    neu VARCHAR,
    pos VARCHAR,
    compound VARCHAR 
    );    

CREATE TABLE IF NOT EXISTS tmp_FinViz (
    news_id  SERIAL PRIMARY KEY,
    ticker VARCHAR,
    date_news date,
    time_news time,
    headline VARCHAR,
    url_news VARCHAR,
    neg VARCHAR,
    neu VARCHAR,
    pos VARCHAR,
    compound VARCHAR 
    );      