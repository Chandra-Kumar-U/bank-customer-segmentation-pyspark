# bank-customer-segmentation-pyspark
PySpark project segmenting bank customers based on balance, credit score and savings rate using Databricks
# Bank Customer Segmentation 

## Project Overview
A PySpark project built on Databricks that segments bank customers
based on their balance, credit score and savings rate using
real world CSV data.

## Tech Stack
- **Platform:** Databricks
- **Language:** PySpark (Python)
- **Dependencies:** See `requirements.txt`

## Project Structure
```
├── src/
│   └── bank_customer_segmentation.py    # Main segmentation code
├── data/
│   └── bank_customers.csv               # Input dataset
├── README.md                             # Project documentation
├── requirements.txt                      # Project dependencies
└── LICENSE
```

## Dataset
Real world style banking dataset with 25 customer records containing
balance, monthly income, monthly expenses, credit score,
number of products and years with bank.

- **Source:** bank_customers.csv
- **Location:** data/ folder

## Analysis Performed

### Transformations
| Transformation | Description |
|----------------|-------------|
| customer_segment | Premium / Regular / Basic based on balance |
| savings_rate | Percentage of income saved each month |
| credit_category | Excellent / Good / Fair / Poor based on credit score |

### GroupBy Analysis
- Total customers per segment
- Average balance per segment
- Average credit score per segment
- Average savings rate per segment

### Window Function
- Ranked customers by balance within each customer segment

### Spark SQL
- Average balance and credit score by city
- Ordered by average balance descending

## Output Tables
| Table | Description |
|-------|-------------|
| bank_customer_segments | Cleaned and segmented customer data |
| customer_segment_summary | Aggregated summary by customer segment |

## Key Insights
- Premium customers have significantly higher savings rates
- Excellent credit score customers tend to have higher balances
- Mumbai and Delhi have the highest average customer balances

## How to Run
1. Upload `bank_customers.csv` to your Databricks Volume
2. Open Databricks and create a new notebook
3. Copy the code from `src/bank_customer_segmentation.py`
4. Update the file path to match your Volume location
5. Run all cells sequentially
6. Check Delta tables in your catalog