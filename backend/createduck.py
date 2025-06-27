import duckdb

con = duckdb.connect("linkedin_jobs.db")

con.execute("""
DROP TABLE IF EXISTS linkedin_jobs;

CREATE TABLE linkedin_jobs AS
SELECT
    row_number() OVER () AS id,
    *
FROM (
    SELECT * FROM read_csv_auto('archive/postings.csv', ignore_errors=true)
    LIMIT 1000
) AS sub;
""")

# Get first 100 descriptions from the table
descriptions = con.execute("""
    SELECT description
    FROM linkedin_jobs
    LIMIT 1000;
""").fetchdf()

print(descriptions)
