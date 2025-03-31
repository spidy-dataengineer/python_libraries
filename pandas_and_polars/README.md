# Pandas vs Polars 비교

## 1. 개요
Pandas와 Polars는 Python에서 데이터 처리를 위한 라이브러리입니다. 하지만 내부 아키텍처, 성능, 사용 방식에서 차이가 있습니다.

|  | **Pandas** | **Polars** |
|---|---|---|
| **출시 연도** | 2008년 | 2020년 |
| **언어** | Python(Cython 사용) | Rust(Python 바인딩 제공) |
| **설계 철학** | 사용 편의성 및 유연성 | 고성능 및 병렬 처리 |
| **데이터 구조** | `DataFrame`, `Series` | `DataFrame`, `Series` |
| **멀티코어 지원** | 단일 스레드 중심 | 멀티코어 병렬 처리 가능 |

## 2. 성능 비교
### 2.1 데이터 로딩 속도
Polars는 멀티스레드로 데이터를 로드하므로 Pandas보다 빠릅니다.
```python
import pandas as pd
import polars as pl

csv_file = "large_data.csv"

# Pandas
df_pandas = pd.read_csv(csv_file)

# Polars
df_polars = pl.read_csv(csv_file)

# 결과: Pandas (10초) vs Polars (2~4초)
```
### 2.2 연산 속도

```python
import numpy as np

# 1000만 개의 데이터 생성
df_pandas = pd.DataFrame({"A": np.random.randint(0, 100, 10_000_000), "B": np.random.rand(10_000_000)})
df_polars = pl.DataFrame(df_pandas)

# Pandas groupby 평균 연산
%time df_pandas.groupby("A")["B"].mean()

# Polars groupby 평균 연산
%time df_polars.groupby("A").agg(pl.col("B").mean())

# 결과: Pandas (1.5초) vs Polars (0.2초)
```
## 3. 메모리 사용량
Polars는 메모리를 더 효율적으로 사용합니다.
```python
import sys
print("Pandas 메모리 사용량:", sys.getsizeof(df_pandas))  # 약 800MB
print("Polars 메모리 사용량:", sys.getsizeof(df_polars))  # 약 450MB
```

## 4. 병렬 처리 및 성능 최적화
Polars는 자동 병렬화를 지원합니다.
```python
# Pandas apply 사용
df_pandas["C"] = df_pandas["B"].apply(lambda x: x * 2)

# Polars 벡터화 연산 (자동 병렬 처리)
df_polars = df_polars.with_columns((pl.col("B") * 2).alias("C"))
```

## 5. API 사용법 비교
기본 연산
```python
# Pandas
df_pandas["new_col"] = df_pandas["B"] * 2
df_pandas_filtered = df_pandas[df_pandas["A"] > 50]

# Polars
df_polars = df_polars.with_columns((pl.col("B") * 2).alias("new_col"))
df_polars_filtered = df_polars.filter(pl.col("A") > 50)
```
GroupBy
```python
# Pandas
df_pandas.groupby("A")["B"].sum()

# Polars
df_polars.groupby("A").agg(pl.col("B").sum())
```
null 값 처리
```python
# Pandas
df_pandas.dropna()
df_pandas.fillna(0)

# Polars
df_polars.drop_nulls()
df_polars.fill_null(0)
```
## 6. 확장성
Polars는 LazyFrame을 활용하여 대규모 데이터 처리가 가능합니다.
```python
lazy_df = df_polars.lazy()
result = lazy_df.filter(pl.col("A") > 50).select(["A", "B"]).collect()
```

## 7. Pandas vs. Polars 선택 기준
| **기준**        | **Pandas**                           | **Polars**                              |
|-----------------|--------------------------------------|-----------------------------------------|
| **사용 용도**   | 전통적인 데이터 분석 및 시각화       | 대규모 데이터 처리 및 성능 최적화      |
| **성능**        | 단일 스레드로 작동하여 상대적으로 느림 | 멀티스레드 지원으로 빠른 처리 속도     |
| **메모리 효율** | 메모리 사용량이 높을 수 있음         | 메모리 사용이 효율적                   |
| **병렬 처리**   | 기본적으로 지원하지 않음             | 기본적으로 지원                        |
| **사용 난이도** | 친숙하고 배우기 쉬움                 | 새로운 문법이지만 익숙해지면 사용 용이 |
| **확장성**      | 단일 머신 환경에 적합                | LazyFrame 등을 활용하여 확장성 높음    |
