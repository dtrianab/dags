CREATE SCHEMA IF NOT EXISTS raw_news;

-- create news api table 
CREATE TABLE IF NOT EXISTS raw_news.newsapi (
    title VARCHAR,
    author VARCHAR,
    source_id VARCHAR,
    source_name VARCHAR,
    description_news VARCHAR,
    content VARCHAR,
    pub_date DATE,
    url_news VARCHAR,
    photo_url VARCHAR);

-- Fin Viz Tables
CREATE TABLE IF NOT EXISTS raw_news.FinViz (
    news_id  SERIAL PRIMARY KEY,
    ticker VARCHAR,
    date_news date,
    time_news time,
    headline VARCHAR,
    url_news VARCHAR
    );    

CREATE TABLE IF NOT EXISTS raw_news.tmp_FinViz (
    news_id  SERIAL PRIMARY KEY,
    ticker VARCHAR,
    date_news date,
    time_news time,
    headline VARCHAR,
    url_news VARCHAR
    );      