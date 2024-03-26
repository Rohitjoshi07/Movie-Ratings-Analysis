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

## Dashboard

Please find the AWS Quicksight dashboard [here](https://ap-south-1.quicksight.aws.amazon.com/sn/dashboards/ff09eb0c-a494-410a-9d6a-642307bac1e9/views/0762d5aa-84fd-4732-94b7-de93972da3f3?directory_alias=rohitjoshi09)


Sample Analysis Images:
![Movie Released By Year](https://github.com/Rohitjoshi07/Movie-Ratings-Analysis/blob/main/diagrams/movie_released_by_year.png?raw=true)
![Yearly budget by Genre](https://github.com/Rohitjoshi07/Movie-Ratings-Analysis/blob/main/diagrams/yearly_budget_by_Genre.png?raw=true)
![YOY Growth of ProductionCompanies](https://github.com/Rohitjoshi07/Movie-Ratings-Analysis/blob/main/diagrams/yoy_growth_pc.png?raw=true)

