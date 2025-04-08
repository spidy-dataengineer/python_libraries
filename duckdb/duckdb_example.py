import duckdb

# 간단한 SQL 실행
result = duckdb.query("SELECT 42").fetchall()
print(result)  # [(42,)]

import pandas as pd

df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

# DataFrame을 DuckDB에 연결
duckdb.register("people", df)

# SQL 실행
result = duckdb.query("SELECT * FROM people WHERE age > 28").to_df()
print(result)

"""
# CSV 읽기 (duckdb.query가 자동으로 파일 읽음)
df = duckdb.query("SELECT * FROM 'people.csv'").to_df()
print(df)

df = duckdb.query("SELECT * FROM 'data.parquet'").to_df()
print(df)

# 새로운 DuckDB 파일 생성 및 연결
conn = duckdb.connect("mydb.duckdb")

# 테이블 생성
conn.execute("CREATE TABLE IF NOT EXISTS users(name TEXT, age INTEGER)")

# 데이터 삽입
conn.execute("INSERT INTO users VALUES ('Alice', 25), ('Bob', 30)")

# 조회
df = conn.execute("SELECT * FROM users").fetchdf()
print(df)
"""

# 새로운 DuckDB 파일 생성 및 연결
conn = duckdb.connect("mydb.duckdb")

# 테이블 생성
conn.execute("CREATE TABLE IF NOT EXISTS users(name TEXT, age INTEGER)")

# 데이터 삽입
conn.execute("INSERT INTO users VALUES ('Alice', 25), ('Bob', 30)")

# 조회
df = conn.execute("SELECT * FROM users").fetchdf()
print(df)

df = pd.DataFrame({
    'id': [1, 2, 3],
    'value': ['A', 'B', 'C']
})

# 테이블로 저장
# conn.register('temp_df', df)
# conn.execute("CREATE TABLE new_tables AS SELECT * FROM temp_df")

# 그룹별 평균 나이
duckdb.query("""
SELECT name, age,
       AVG(age) OVER() AS avg_age
FROM people
""").show()

# 1. Arrow, Polars 연동
# 1-1. Arrow Table → DuckDB
import pyarrow as pa
import duckdb

# Arrow 테이블 생성
arrow_table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})

# DuckDB에 등록
duckdb.register('arrow_tbl', arrow_table)

# SQL 쿼리
df = duckdb.query("SELECT * FROM arrow_tbl WHERE a > 1").to_df()
print(df)

# 1-2. Polars 사용
import polars as pl

pl_df = pl.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
duckdb.register("pl_df", pl_df)

df = duckdb.query("SELECT * FROM pl_df WHERE a < 3").to_df()

# 📍 2. Parquet 직접 저장 및 쿼리 푸시다운
# 2-1. Parquet 파일로 저장
duckdb.query("COPY (SELECT * FROM people) TO 'people.parquet' (FORMAT PARQUET)")

# 2-2. Parquet 컬럼 푸시다운 (select에서 필요한 컬럼만 읽음)
df = duckdb.query("SELECT name FROM 'people.parquet'").to_df()

# 📍 3. Persistent Database
# 3-1. DuckDB DB 파일로 사용하기
"""
conn = duckdb.connect("analytics.duckdb")  # 파일 기반 DB

conn.execute("CREATE TABLE IF NOT EXISTS logs(id INTEGER, message TEXT)")
conn.execute("INSERT INTO logs VALUES (1, 'OK'), (2, 'FAIL')")
df = conn.execute("SELECT * FROM logs").fetchdf()

"""

#📍  4. CTE, 서브쿼리, 윈도우 함수
# 4-1. CTE (WITH 문)
duckdb.query("""
WITH filtered AS (
    SELECT * FROM people WHERE age > 28
)
SELECT name FROM filtered
""").show()

# 4-2. 윈도우 함수
duckdb.query("""
SELECT name, age,
       RANK() OVER (ORDER BY age DESC) AS rank
FROM people
""").show()

# 📍 5. 복잡한 Join, Subquery, JSON
# 5-1. JSON 파싱
duckdb.query("""
SELECT json_extract('{"name":"Alice","age":30}', '$.name') AS name
""").show()

# 5-2. Join 예제
# 두 테이블 조인
duckdb.query("""
SELECT a.name, b.city
FROM people a
JOIN cities b ON a.city_id = b.id
""").show()

# 📍 6. UDF (사용자 정의 함수)
# Python 함수 → SQL 함수로 등록
def add_one(x: int) -> int:
    return x + 1

duckdb.create_function("add_one", add_one)

duckdb.query("SELECT add_one(5)").show()  # 6

# 📍 7. View, Temp Table
duckdb.query("CREATE VIEW old_people AS SELECT * FROM people WHERE age > 30")
duckdb.query("SELECT * FROM old_people").show()

# 📍 8. SQL Functions & Expression 예제
"""-- SQL에서 직접 제공하는 함수들
SELECT
UPPER(name),
LENGTH(name),
age BETWEEN 30 AND 40,
CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END as level
FROM people"""

