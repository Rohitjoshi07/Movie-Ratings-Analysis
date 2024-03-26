--create external table for movie-ratings data
CREATE EXTERNAL TABLE `movie_ratings_data` (
`id` string,
`status`  varchar(255),
`genres`  varchar(1000),
`release_date` date,
`runtime` int,
`year` string,
`budget` float,
`revenue` float,
`vote_count` int,
`vote_average` float,
`title`  varchar(255),
`production_companies`  varchar(1000)
)
partitioned by (
    `month` string
)

stored as parquet

LOCATION
    's3://tmdb-movies-datalake/movie-ratings-filtered/'

--crate view for filtered data
create view movie_ratings_filtered as (
SELECT id,revenue,release_date, year as release_year, month as release_month,
        budget, status, vote_average, vote_count
        from movie_ratings_data
        where release_date is not null
)

-- create view for genre level info
create view movie_info_by_genre as
select release_year, release_month, genre, count(*) as movie_count, sum(budget) as total_budget_by_genre, sum(revenue) as total_revenue_by_genre, sum(vote_count) as total_votes_by_genre,
    CORR(budget, revenue) AS budget_revenue_correlation
from (
 select id,revenue,release_date,year(release_date) as release_year, month(release_date) as release_month,
        budget,vote_average, vote_count, lower(genre) AS genre
    FROM movie_ratings_data
    CROSS JOIN UNNEST(split(genres, ', ')) AS t(genre)
    )
where genre IN ('action', 'thriller', 'crime', 'comedy', 'drama', 'romance', 'horror', 'adventure', 'documentary', 'tv movie', 'fantasy', 'mystery', 'history', 'science fiction', 'family', 'war', 'western','animation', 'na')

group by
release_year, release_month, genre;


-- create view for production companies cummulative data

create view pc_cummilative_data as

WITH PC_FILTERED AS (
select release_year, release_month, production_company, dense_rank() over(partition by production_company order by release_year) as pc_rank   from
(select id, year(release_date) as release_year, month(release_date) as release_month,
        budget, revenue, lower(production_company) AS production_company
    FROM movie_ratings_data
    CROSS JOIN UNNEST(split(production_companies, ', ')) AS t(production_company)
)
where production_company is not null and production_company NOT IN ('action', 'thriller', 'crime', 'comedy', 'drama', 'romance', 'horror', 'adventure', 'documentary', 'tv movie', 'fantasy', 'mystery', 'history', 'science fiction', 'family', 'war', 'western', 'animation', 'na', '') and LENGTH(production_company)>5 and regexp_like(production_company,'^[a-zA-Z]+(\.[a-zA-Z]+)*$')),

pc_yearly__agg_data as(
select release_year, count(distinct production_company) as pc_distinct_count
from PC_FILTERED
where pc_rank=1
group by release_year),

pc_cumulative_data AS (
    SELECT release_year, pc_distinct_count,
        SUM(pc_distinct_count) OVER (ORDER BY release_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pc_cumm_count
    FROM
        pc_yearly__agg_data
)

select release_year, pc_distinct_count, pc_cumm_count from pc_cumulative_data











