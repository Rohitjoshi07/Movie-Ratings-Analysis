# Movie-Ratings-Analysis

## OVERVIEW

This repository contains transformation and analysis of [Movie-Ratings](https://www.kaggle.com/datasets/alanvourch/tmdb-movies-daily-updates) Data from Kaggle. Some key features are:
1. Data Cleaning
2. Data Transformation
3. Querying using Athena
4. Dashboarding using AWS quick sight
  
## Architecture
![Architecture](https://github.com/Rohitjoshi07/Movie-Ratings-Analysis/blob/main/diagrams/architecture.png?raw=true)


## Folder Structure

/diagrams -- contains the analysis sample plots, architecture etc. </br>
Movie-rating-dashboard.pdf -- pdf file of quicksight dashboard  </br>
Movie-ratings-assignment-notebook.ipynb  --notebook contains detailed analysis, filtering and transformation using spark  </br>
movie-ratings-etl.py --python file for the ELTL process[Data Read, Data Stored, Data Transformed, Transformed Data Stored]  </br>
athena_queries.sql  -- athena queries to create tables/views  </br>

## Stats about Data

Original Data:
```
Total Entries in original data: 908174


Column 'id' has 0 null values.
Column 'title' has 124 null values.
Column 'vote_average' has 177 null values.
Column 'vote_count' has 175 null values.
Column 'status' has 187 null values.
Column 'release_date' has 86386 null values.
Column 'revenue' has 198 null values.
Column 'runtime' has 195 null values.
Column 'budget' has 193 null values.
Column 'imdb_id' has 355135 null values.
Column 'original_language' has 259 null values.
Column 'original_title' has 231 null values.
Column 'overview' has 153629 null values.
Column 'popularity' has 347 null values.
Column 'tagline' has 735508 null values.
Column 'genres' has 260615 null values.
Column 'production_companies' has 473791 null values.
Column 'production_countries' has 346323 null values.
Column 'spoken_languages' has 346883 null values.
Column 'cast' has 300356 null values.
Column 'director' has 167398 null values.
Column 'director_of_photography' has 671060 null values.
Column 'writers' has 466194 null values.
Column 'producers' has 613829 null values.
Column 'music_composer' has 802466 null values.

```

```
Distinct status in original data: 74
Distinct genres in original data: 42205
Distinct production companies in original data: 210603
```

Cleaned/Filterd Data:
```
Total entries in cleaned data: 821655
Distinct status in cleaned data: 5


Column 'id' has 0 null values.
Column 'status' has 0 null values.
Column 'genres' has 0 null values.
Column 'release_date' has 0 null values.
Column 'year' has 0 null values.
Column 'month' has 0 null values.
Column 'budget' has 0 null values.
Column 'revenue' has 0 null values.
Column 'vote_count' has 0 null values.
Column 'vote_average' has 0 null values.
Column 'title' has 0 null values.
Column 'production_companies' has 0 null values.
```
Outliers:
```
Entries with runtime greater than 300 or less than 0: 1360
Entries with vote average greater than 90 or less than 0: 0
Entries with budget or revenue less than 0: 1
Entries in cleaned df after remove outliers: 820295
```


## Dashboard

Please find the AWS Quicksight dashboard [here](https://ap-south-1.quicksight.aws.amazon.com/sn/dashboards/ff09eb0c-a494-410a-9d6a-642307bac1e9/views/0762d5aa-84fd-4732-94b7-de93972da3f3?directory_alias=rohitjoshi09)


Sample Analysis Images:
![Movie Released By Year](https://github.com/Rohitjoshi07/Movie-Ratings-Analysis/blob/main/diagrams/movie_released_by_year.png?raw=true)
![Yearly budget by Genre](https://github.com/Rohitjoshi07/Movie-Ratings-Analysis/blob/main/diagrams/yearly_budget_by_Genre.png?raw=true)
![YOY Growth of ProductionCompanies](https://github.com/Rohitjoshi07/Movie-Ratings-Analysis/blob/main/diagrams/yoy_growth_pc.png?raw=true)

