# Analyzing Google Playstore Application Data Using Pyspark

## Introduction
In this project, we will analyze data related to mobile applications available on the Google Play Store using PySpark. We will perform various data cleaning and analysis tasks to gain insights into the dataset. Below are the key steps and findings of our analysis.

## Importing Necessary Libraries
```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
```

## Creating the Google Playstore Application DataFrame
```python
# Create a Spark session
spark = SparkSession.builder.appName("GooglePlayStore").getOrCreate()

# Read the CSV file
df = spark.read.csv('googleplaystore.csv', header=True, inferSchema=True, escape='"')
```

## Analyzing Data Distribution for Each Column
We start by examining the distribution of data in each column.

### Column Descriptions
- **App (string):** The name of the mobile app.
- **Category (string):** The category to which the app belongs.
- **Rating (double):** The average user rating of the app.
- **Reviews (string):** The number of user reviews for the app.
- **Size (string):** The size of the app.
- **Installs (string):** The number of app installations.
- **Type (string):** Whether the app is free or paid.
- **Price (string):** The price of the app.
- **Content Rating (string):** The content rating for the app.
- **Genres (string):** The genre(s) of the app.
- **Last Updated (string):** The date when the app was last updated.
- **Current Ver (string):** The current version of the app.
- **Android Ver (string):** The minimum Android version required to run the app.

We analyze each column and display the top 5 entries in terms of count.

### Examples:
- **Top 5 Apps:**
```python
df['App'].show(truncate=False)
```

- **Top 5 Categories:**
```python
df['Category'].show(truncate=False)
```

- **Top 5 Ratings:**
```python
df['Rating'].show(truncate=False)
```

- **Top 5 Reviews:**
```python
df['Reviews'].show(truncate=False)
```

- **Top 5 Sizes:**
```python
df['Size'].show(truncate=False)
```

- **Top 5 Installs:**
```python
df['Installs'].show(truncate=False)
```

- **Top 5 Types:**
```python
df['Type'].show(truncate=False)
```

- **Top 5 Prices:**
```python
df['Price'].show(truncate=False)
```

- **Top 5 Content Ratings:**
```python
df['Content Rating'].show(truncate=False)
```

- **Top 5 Genres:**
```python
df['Genres'].show(truncate=False)
```

- **Top 5 Last Updated Dates:**
```python
df['Last Updated'].show(truncate=False)
```

- **Top 5 Current Versions:**
```python
df['Current Ver'].show(truncate=False)
```

- **Top 5 Android Versions:**
```python
df['Android Ver'].show(truncate=False)
```

## Data Cleaning
We perform data cleaning tasks, including dropping unnecessary columns and converting the data types of 'Reviews,' 'Installs,' and 'Price' columns from string to integer.

### Dropping Unnecessary Columns
```python
df = df.drop("Size", "Content Rating", "Last Updated", "Current Ver", "Android Ver")
```

### Fixing Data Types
```python
# Convert "Reviews" column to integer
df = df.withColumn("Reviews", col("Reviews").cast(IntegerType()))

# Clean and convert "Installs" column to integer
df = df.withColumn("Installs", regexp_replace(col("Installs"), "[^0-9]", "").cast(IntegerType()))

# Clean and convert "Price" column to integer
df = df.withColumn("Price", regexp_replace(col("Price"), "[^0-9]", "").cast(IntegerType()))
```

## Data Analysis
### Top 5 Apps with the Most Reviews
We identify the top 5 apps with the highest total number of reviews.

```python
# Group by 'App' and sum the 'Reviews' for each app
apps_review_sum = df.groupBy('App').agg(sum('Reviews').alias('Total Reviews'))

# Sort the DataFrame by 'Total Reviews' in descending order
top_5_apps_by_reviews = apps_review_sum.orderBy(col('Total Reviews').desc()).limit(5)

# Show the top 5 apps with the most sum of reviews
top_5_apps_by_reviews.show(truncate=False)
```

### Top 5 Most Installed Apps
We identify the top 5 apps with the highest total number of installations.

```python
# Group by 'App' and sum the 'Installs' for each app
apps_installs_sum = df.groupBy('App').agg(sum('Installs').alias('Total Installs'))

# Sort the DataFrame by 'Total Installs' in descending order
top_5_apps_by_installs = apps_installs_sum.orderBy(col('Total Installs').desc()).limit(5)

# Show the top 5 apps with the most sum of installations
top_5_apps_by_installs.show(truncate=False)
```

### Analysis of App Installations by Category
We analyze the total number of installations by category.

```python
# Group by 'Category' and sum the 'Installs' for each category
categories_installs_sum = df.groupBy('Category').agg(sum('Installs').alias('Total Installs'))

# Sort the DataFrame by 'Total Installs' in descending order
categories_installs_sum = categories_installs_sum.orderBy(col('Total Installs').desc())

# Show the category-wise distribution of installed apps
categories_installs_sum.show(truncate=False)
```

### Top 5 Apps with the Highest Ratings
We identify the top 5 apps with the highest user ratings.

```python
# Filter out invalid ratings and select apps with ratings <= 5
top_apps_by_ratings = df.filter(~isnan(col('Rating'))).filter(df['Rating'] <= 5)

# Sort the DataFrame by 'Rating' in descending order
top_apps_by_ratings = top_apps_by_ratings.orderBy(col('Rating').desc()).limit(5)

# Show the top 5 apps with the highest ratings
top_apps_by_ratings.select('App', 'Rating', 'Type').show(truncate=False)
```

### Top 5 Paid Apps with the Highest Ratings
We identify the top 5 paid apps with the highest user ratings.

```python
# Filter out invalid ratings, select apps with ratings <= 5, and filter by 'Type' == 'Paid'
top_paid_apps_by_ratings = df.filter(~isnan(col('Rating'))).filter(df['Rating'] <= 5).filter(df['Type']=='Paid')

# Sort the DataFrame by 'Rating' in descending order
top_paid_apps_by_ratings = top_paid_apps_by_ratings.orderBy(col('Rating').desc()).limit(5)

# Show the top 5 paid apps with the highest ratings


top_paid_apps_by_ratings.select('App', 'Rating', 'Type').show(truncate=False)
```

## Conclusion
This project provides insights into the Google Play Store application data using PySpark. We analyzed the distribution of data in each column, cleaned the dataset, and performed various data analysis tasks, such as identifying top apps by reviews, installations, ratings, and more.

The analysis can be used to make informed decisions about app development, marketing, and category selection on the Google Play Store platform.
