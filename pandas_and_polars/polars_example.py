"""
Apache Arrow 모델을 사용하여 메모리상에서 칼럼 구조로 데이터를 정의하고,
 이를 기반으로 벡터화(vectorized) 연산과 SIMD(Single Instruction Multiple Data)를 사용한
 CPU 최적화를 하여 성능을 높였습니다.
 zero-copy 데이터 공유가 가능하고 직렬화/역직렬화 효율이 매우 높아서 여러 코어나 프로세스가 작업할 때
데이터 교환 비용을 줄일 수 있습니다.
"""
import ray
import polars as pl

# Ray Dataset -> Polars DataFrame
ds = ray.data.read_parquet("data.parquet")
pl_df = pl.from_arrow(ray.get(ds.to_arrow_refs()))

# Polars DataFrame -> Ray Dataset
pl_df = pl.read_parquet("data.parquet")
ds = ray.data.from_arrow(pl_df.to_arrow())

# 데이터 읽어오기
import polars as pl

# CSV 파일 읽기
df = pl.read_csv("data.csv")

# Parquet 파일 읽기
df_parquet = pl.read_parquet("data.parquet")

# JSON 파일 읽기
df_json = pl.read_json("data.json")

# SQL 데이터베이스에서 읽기 (Connector 필요)
# df_sql = pl.read_database("SELECT * FROM table_name", connection)

# 데이터 조회
# 상위 5개 행 출력
print(df.head(5))

# 데이터 구조 확인
print(df.describe())

# 특정 컬럼 선택
df.select("column_name")

# 여러 컬럼 선택
df.select(["col1", "col2"])

# 컬럼 추가 및 변환
# 새로운 컬럼 추가
df = df.with_columns((pl.col("col1") * 2).alias("col1_doubled"))

# 기존 컬럼 변환
df = df.with_columns(pl.col("col2").str.to_uppercase())

# 필터링
df_filtered = df.filter(pl.col("age") > 30)

# 정렬
df_sorted = df.sort("age", descending=True)

# 그룹화 및 집계
df_grouped = df.groupby("category").agg(pl.col("value").sum().alias("total_value"))

# lazy evaluation (지연 실행)
df_lazy = pl.scan_csv("data.csv")  # LazyFrame 사용
df_result = df_lazy.filter(pl.col("age") > 30).select(["name", "age"])
df_result.collect()  # 최적화된 실행

# SQL 스타일 쿼리
ctx = pl.SQLContext()
ctx.register("df", df)

result = ctx.execute("SELECT name, age FROM df WHERE age > 30")
print(result)

# 2개의 데이터프레임 합치기
df1.vstack(df2)  # 수직 결합 (행 추가)
df1.hstack(df2)  # 수평 결합 (열 추가)

# 조인
df_joined = df1.join(df2, on="id", how="inner")

