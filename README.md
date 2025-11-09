# Week 11 – PySpark Data Pipeline

**Author:** Franco Amigo  | **Environment:** Databricks | **Dataset:** NYC Yellow Taxi Trip Records (2006–2010)

---

## 📘 Dataset Description and Source

This assignment was conducted in the Databricks environment using the **New York City Yellow Taxi Trip Data**, available in the default Databricks dataset repository:

The dataset includes millions of trip-level records from yellow taxi services in New York City between 2006 and 2010.  
Each record contains information such as:
- Pickup and drop-off timestamps and coordinates  
- Trip distance and passenger count  
- Fare amount, tips, taxes, and tolls  
- Payment type (cash or card)

For this assignment, data from **2006 to 2010** were ingested, but the analysis focuses on **2010 trips** for computational efficiency.

---

## ⚙️ Data Processing Pipeline

1. **Data Loading:**  
   All CSV files from 2006–2010 were loaded using `spark.read.csv()` with `inferSchema=True` and `header=True`.

2. **Filtering:**  
   Filtered trips corresponding to 2010 and removed records with invalid or missing values for `fare_amount`, `tip_amount`, or `trip_distance`.

3. **Data Type Conversion:**  
   Converted numeric columns (e.g., `fare_amount`, `trip_distance`, `tip_amount`) from string to `double`. 
   Timestamps were converted using `to_timestamp()` for accurate date and time operations.

4. **Feature Engineering:**  
   Created derived variables: 
   - `trip_minutes` = (dropoff_ts - pickup_ts) / 60  
   - Extracted `year`, `month`, and `hour` from pickup timestamp  

5. **Transformations and Aggregations:**  
   - Applied multiple filters (`trip_distance > 0`, `fare_amount >= 0`, `tip_amount >= 0`)  
   - Grouped data by `month`, and `payment_type`  
   - Computed:
     - `count(*)` → number of trips  
     - `avg(fare_amount)`  
     - `avg(tip_amount)`  
     - `percentile_approx(tip_amount, 0.99)` 
     - `avg(trip_distance)` and `avg(trip_minutes)`

6. **SQL Queries:**  
   Registered the cleaned dataset as a temporary view `trips` and executed two SQL queries:
   - 99 percentile tip amount by month (`99_tip_amount`)
   - Average number of trips per weekday (`COUNT / DISTINCT date`)

7. **Storage:**  
   Final outputs were written to Parquet tables.

---
## ⚖️ Actions vs Transformations

In Spark, **transformations** are *lazy operations* that define a logical plan but do not immediately trigger computation.  
They build a lineage of transformations that Spark optimizes before execution.

Example:
```python
aagg_pay_month = (
    df_f.groupBy("month", "payment_type")
        .agg(
            F.count("*").alias("n_trips"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_minutes").alias("avg_duration")
        )
        .orderBy( "month")
)

```
At this point, no data has been read or computed yet. Spark only builds the plan.

By contrast, actions such as .show(), .count(), or .write() are eager operations.
They trigger the execution of the entire lineage and cause Spark to launch a job.

Example:
```python
aagg_pay_month = (
    df_f.groupBy("month", "payment_type")
        .agg(
            F.count("*").alias("n_trips"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_minutes").alias("avg_duration")
        )
        .orderBy( "month")
)
aagg_pay_month.show()
```
This command forces Spark to execute all the previous transformations, read the data, and display the output.

📸 Screenshots below show the difference:

The first screenshot (agg_pay_month) defines the plan.

<img width="1357" height="328" alt="image" src="https://github.com/user-attachments/assets/d069497d-d4c9-4827-bf02-1c6aee1ba6d2" />

The second screenshot (agg_pay_month.show()) triggers actual computation.

<img width="1403" height="693" alt="image" src="https://github.com/user-attachments/assets/6849ce62-8adc-4001-9168-4ed3844de77a" />


---
## 🚀 Performance Analysis

Spark optimizations observed:

- **Predicate Pushdown:** Filters on `pickup_datetime` (year = 2010) and numeric values were pushed to the data scan phase, minimizing file reads.
- **Column Pruning:** Only the columns required for transformations and aggregations were scanned.
- **Lazy Evaluation:** Transformations were not executed until an action (`show()`, `write()`) triggered computation.

The main performance limitation occurred during data ingestion, as reading multiple compressed CSV files (`.csv.gz`) required schema inference for each file, increasing initialization time.  
This could be improved by converting the raw data into **Parquet format**, enabling faster access and better optimization.  

---

## 📊 Key Findings

- **Payment patterns:**  
  - Credit card trips showed **higher average fares (\$10–11)** and **average tip amounts around \$2**, equivalent to roughly **20% tip rate**.  
  - Cash transactions recorded **almost no tips**, as expected since cash gratuities are not digitally captured.

- **Trip characteristics:**  
  - Average trip distance remained stable, between **2.5 and 3.5 miles**.  
  - Mean trip duration ranged from **10 to 13 minutes** across months.  
  - These indicators suggest consistent trip patterns and passenger behavior throughout the year.

- **Tip distribution:**  
  - The **99th percentile of tip amounts** ranged between **\$6 and \$7** for all months.  
  - Tip values were relatively stable, showing no strong seasonal variation.

- **Temporal trends:**  
  - **Fridays and Saturdays** were the busiest days, with approximately **490,000 and 480,000 trips per day**, respectively.  
  - **Mondays** registered the lowest number of daily trips.  
  - Overall trip volumes remained consistent throughout the year, with only minor monthly fluctuations.
    
---

### 📸 Screenshots

-  `.explain()` output showing Spark’s physical execution plan

<img width="1366" height="506" alt="image" src="https://github.com/user-attachments/assets/c0c7f91d-63c5-4c09-9123-08424652c04c" />

<img width="1385" height="512" alt="image" src="https://github.com/user-attachments/assets/1ddb43e0-2eaf-486a-97f3-151508f3b43a" />

<img width="1377" height="512" alt="image" src="https://github.com/user-attachments/assets/a11345d8-d86c-469c-a66c-761eb1ec3523" />


- Spark UI “Query Details” view with DAG visualization
<img width="1487" height="845" alt="image" src="https://github.com/user-attachments/assets/1294e443-de51-45a5-8a8f-d858604e250c" />

<img width="1482" height="851" alt="image" src="https://github.com/user-attachments/assets/f8b8249a-6eb7-4ba7-be03-833b0b633efc" />

<img width="1476" height="837" alt="image" src="https://github.com/user-attachments/assets/c5d1d036-c130-48ca-b377-865110cdd8ff" />

-  Spark SQL results for both queries
   
 __99 Percentile Tip Amount by Month__

<img width="158" height="301" alt="image" src="https://github.com/user-attachments/assets/060ef6e2-c28d-4aef-8ebc-37e15d2ce757" />

  __Average trip by day of week__
 
<img width="178" height="202" alt="image" src="https://github.com/user-attachments/assets/c76a1fea-fc0b-4a52-a58f-2cd252121597" />


---

## 📂 Files in Repository

| File | Description |
|------|--------------|
| `nyc_taxi.ipynb` | Main Databricks notebook with transformations and analysis |
| `nyc_taxi.html` | Exported notebook |
| `README.md` | Project description, performance analysis, and summary |






