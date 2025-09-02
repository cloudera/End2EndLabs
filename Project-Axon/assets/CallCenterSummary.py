#
# Copyright (c) 2025 Cloudera, Inc. All rights reserved.
#

from pyspark.sql import SparkSession
import sys

### Update the username
username = "aktiwari".replace("-", "_")  ## Example: "apac01"

# Expect 1 arg: interaction_date
if len(sys.argv) != 2:
    print("Usage: CallCenterSummary.py <interaction_date>")
    sys.exit(1)

#### DB Name and App Name accordingly
db_name = "callcenter_data"
interaction_date = sys.argv[1]  # Example: 2025-08-12
appName = username + "-CDE-CallCenter-Summary"

spark = SparkSession \
    .builder \
    .appName(appName) \
    .getOrCreate()

print("...............................")
print(f"Running Call Center Summary Report for {interaction_date}")

# Create the summary table if it doesn't exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {db_name}.callcenter_interaction_summary (
    interactiondate DATE,
    avg_satisfaction_rating DOUBLE,
    total_resolved_calls BIGINT,
    total_unresolved_calls BIGINT,
    avg_duration DOUBLE,
    call_count BIGINT
)
""")

# Build the aggregation query for the given date
callCenterSummaryQuery = f"""
SELECT
    CAST(interactiondate AS DATE) AS interactiondate,
    AVG(satisfactionrating) AS avg_satisfaction_rating,
    SUM(CASE WHEN issueresolved = 'Yes' THEN 1 ELSE 0 END) AS total_resolved_calls,
    SUM(CASE WHEN issueresolved = 'No' THEN 1 ELSE 0 END) AS total_unresolved_calls,
    AVG(duration) AS avg_duration,
    COUNT(*) AS call_count
FROM {db_name}.callcenter_interaction
WHERE interactiondate = '{interaction_date}'
GROUP BY interactiondate
"""

# Append the results
print("...............................")
print("Appending summary data...")
summary_df = spark.sql(callCenterSummaryQuery)
summary_df.write.mode("append").insertInto(f"{db_name}.callcenter_interaction_summary")

# Show results for that date
print("...............................")
print(f"Results for {interaction_date}:")
spark.sql(f"""
SELECT * 
FROM {db_name}.callcenter_interaction_summary
WHERE interactiondate = '{interaction_date}'
""").show(truncate=False)
