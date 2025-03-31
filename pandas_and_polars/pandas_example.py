import pandas as pd
import numpy as np

# 데이터 파일 읽기
# df = pd.read_csv('data.csv')
# df_excel = pd.read_excel('data.xlsx', sheet_name=0)  # 첫 번째 시트
# df_json = pd.read_json('data.json')
# df_parquet = pd.read_parquet('data.parquet')

# 데이터 파일 쓰기
# df.to_csv('output.csv', index=False)
# df.to_excel('output.xlsx', index=False)
# df.to_json('output.json')
# df.to_parquet('output.parquet')

"""
Series는 기본적으로 아래와 같이 값의 리스트를 넘겨주어 만들 수 있습니다.
또한 값이 위치하고 있는 정보인 인덱스(index)가 Series 에 같이 저장되게 되는데요.
따로 전달해주지 않는 한 기본적으로 0부터 시작하여 1씩 증가하는 정수 인덱스가 사용됨
"""

s = pd.Series([1, 3, 5, np.nan, 6, 8],name="column1")
print(s)
df_s = pd.DataFrame(s)
print(df_s)
# 0    1.0
# 1    3.0
# 2    5.0
# 3    NaN
# 4    6.0
# 5    8.0
# dtype: float64

# 여러 개의 Series를 DataFrame으로 변환
s1 = pd.Series([10, 20, 30], name="A")
s2 = pd.Series([40, 50, 60], name="B")

# 여러 개의 Series를 DataFrame으로 변환
df = pd.DataFrame({"Column1": s1, "Column2": s2})
print(df)

# Series의 인덱스를 DataFrame의 컬럼으로 추가
s = pd.Series([100, 200, 300], index=["A", "B", "C"], name="values")

# DataFrame 변환 시 인덱스를 컬럼으로 추가
df = s.reset_index()
df.columns = ["IndexColumn", "Values"]
print(df)

# Series를 행(Row)로 추가하는 방법
s = pd.Series([1, 2, 3], index=["A", "B", "C"], name="Row1")

# Transpose (T) 사용
df = pd.DataFrame(s).T
print(df)


dates = pd.date_range('20130101', periods=6)
# DatetimeIndex(['2013-01-01', '2013-01-02', '2013-01-03', '2013-01-04',
#                '2013-01-05', '2013-01-06'],
#               dtype='datetime64[ns]', freq='D')
print(dates)

df = pd.DataFrame(np.random.randn(6,4), index=dates, columns=list('ABCD'))
#                   A         B         C         D
# 2013-01-01  0.469112 -0.282863 -1.509059 -1.135632
# 2013-01-02  1.212112 -0.173215  0.119209 -1.044236
# 2013-01-03 -0.861849 -2.104569 -0.494929  1.071804
# 2013-01-04  0.721555 -0.706771 -1.039575  0.271860
# 2013-01-05 -0.424972  0.567020  0.276232 -1.087401
# 2013-01-06 -0.673690  0.113648 -1.478427  0.524988
print(df)

df2 = pd.DataFrame({'A': 1.,
                    'B': pd.Timestamp('20130102'),
                    'C': pd.Series(1, index=list(range(4)), dtype='float32'),
                    'D': np.array([3]*4, dtype='int32'),
                    'E': pd.Categorical(['test', 'train', 'test', 'train']),
                    'F': 'foo'})
print(df2)
print("\n")

#      A          B    C  D      E    F
# 0  1.0 2013-01-02  1.0  3   test  foo
# 1  1.0 2013-01-02  1.0  3  train  foo
# 2  1.0 2013-01-02  1.0  3   test  foo
# 3  1.0 2013-01-02  1.0  3  train  foo

#  기본 정보 조회
df.head()   # 상위 5개 행 출력
df.tail()   # 하위 5개 행 출력
df.info()   # 데이터 타입 및 결측치 확인
df.describe()  # 수치형 데이터 통계 요약
df.shape   # 행과 열 개수 출력
df.columns  # 컬럼명 확인
df.dtypes   # 컬럼별 데이터 타입 확인
df.isnull().sum()  # 결측치 개수 확인

print("")
# 데이터 선택 및 필터링
print(df['A'])  # 특정 컬럼 선택
print(df[['A', 'B']])  # 여러 컬럼 선택
print(df.iloc[0])  # 첫 번째 행 선택 (index 기준) ,위치를 이용하여 선택하기
print(df.iloc[0:5])  # 0~4번째 행 선택
df.loc[dates[0]]
# A    0.469112
# B   -0.282863
# C   -1.509059
# D   -1.135632
# Name: 2013-01-01 00:00:00, dtype: float64 # 이름을 이용하여 선택하기
df.loc[:,['A','B']]
#                    A         B
# 2013-01-01  0.469112 -0.282863
# 2013-01-02  1.212112 -0.173215
# 2013-01-03 -0.861849 -2.104569
# 2013-01-04  0.721555 -0.706771
# 2013-01-05 -0.424972  0.567020
# 2013-01-06 -0.673690  0.113648

print(df.loc[df['A'] > 0])  # 조건 필터링 df.A처럼 가능, 조건을 이용하여 선택하기
print(df.loc[df['B'] == 'value', ['A', 'B']])  # 특정 조건 만족하는 행에서 여러 컬럼 선택

## 맨 처음 3개의 행을 가져옵니다.
df[0:3]
#                    A         B         C         D
# 2013-01-01  0.469112 -0.282863 -1.509059 -1.135632
# 2013-01-02  1.212112 -0.173215  0.119209 -1.044236
# 2013-01-03 -0.861849 -2.104569 -0.494929  1.071804

## 인덱스명에 해당하는 값들을 가져옵니다.
df['20130102':'20130104']
#                    A         B         C         D
# 2013-01-02  1.212112 -0.173215  0.119209 -1.044236
# 2013-01-03 -0.861849 -2.104569 -0.494929  1.071804
# 2013-01-04  0.721555 -0.706771 -1.039575  0.271860

"""
Method	Selection Type	Usage	Example
.loc[]	Label-based indexing	Selects by row/column labels	df.loc[2, "name"] (Selects row with index 2 and column "name")
.iloc[]	Integer-based indexing	Selects by row/column positions	df.iloc[2, 1] (Selects the 3rd row and 2nd column)
"""

# 데이터 수정
df['new_col'] = df['col1'] * 2  # 새로운 컬럼 추가
df['col1'] = df['col1'].fillna(0)  # 결측치 대체
df = df.drop(columns=['col_to_remove'])  # 컬럼 삭제
df = df.rename(columns={'old_name': 'new_name'})  # 컬럼명 변경
df = df.sort_values(by='col1', ascending=False)  # 정렬

# 데이터 변환
df['col1'] = df['col1'].astype(int)  # 데이터 타입 변환
df['date'] = pd.to_datetime(df['date_col'])  # 날짜 변환
df['category'] = df['category'].astype('category')  # 카테고리 변환

# 그룹핑 및 집계
df.groupby('col1').agg({'col2': 'sum', 'col3': 'mean'})
df.pivot_table(values='col1', index='col2', columns='col3', aggfunc='sum')
df['col1'].value_counts()  # 고유값 개수

# 데이터 병합 및 결합
df1 = pd.DataFrame({'id': [1, 2, 3], 'value1': ['A', 'B', 'C']})
df2 = pd.DataFrame({'id': [1, 2, 3], 'value2': ['X', 'Y', 'Z']})

df_merged = pd.merge(df1, df2, on='id', how='inner')  # 조인
df_concat = pd.concat([df1, df2], axis=0)  # 행 추가

# 데이터 결측치 처리
df.dropna()  # 결측치 제거
df.fillna(0)  # 결측치 대체
df.replace({'old_value': 'new_value'})  # 특정 값 변경

# 고급 기능 (적용 및 벡터화 연산)
df['new_col'] = df['col1'].apply(lambda x: x * 2)  # apply 함수 적용
df['new_col'] = df['col1'].map({'A': 1, 'B': 2})  # 값 매핑

# 병렬처리 및 최적화
import dask.dataframe as dd

df = dd.read_csv('large_data.csv')
df.compute()  # 연산 실행
df.eval("new_col = col1 + col2", inplace=True)
