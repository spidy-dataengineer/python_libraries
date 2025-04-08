# 🐤 DuckDB: Lightweight OLAP Database for Analytics

[DuckDB](https://duckdb.org)는 **빠르고 가벼운 컬럼 기반 분석형 DBMS**입니다. Pandas, Parquet, Arrow 등과 자연스럽게 연동되며, 머신러닝 전처리나 로그 분석, OLAP 쿼리 등을 빠르게 처리할 수 있습니다.

---

## 🚀 DuckDB 주요 특징

- **In-Process Execution**: 서버 없이 Python, R, CLI 등에서 바로 사용 가능
- **Columnar Engine**: 열 지향 형식으로 읽기 최적화, 대용량 분석에 강함
- **SQL 지원**: 완전한 ANSI SQL 지원, Pandas/Polars와 자연스럽게 연동
- **Parquet/Arrow 네이티브 지원**: 외부 파일을 로딩 없이 직접 SQL로 조회 가능
- **빠른 성능**: Predicate/Projection Pushdown, Vectorized Execution 등 최적화
- **Lightweight**: SQLite처럼 가볍지만 OLAP에 특화된 엔진
- **멀티 플랫폼 지원**: Python, R, Node.js, C++, CLI 등 다양한 인터페이스 제공

---

## ✅ DuckDB 장점

| 장점 | 설명 |
|------|------|
| 🔄 Pandas/Polars 연동 | DataFrame과 바로 연결해 SQL 사용 가능 |
| 💾 Parquet/CSV 읽기 최적화 | 컬럼 단위 I/O 및 조건 푸시다운으로 빠름 |
| 🧠 인메모리 + 파일 모드 | 메모리 기반 분석 또는 `.duckdb` 파일로 저장 가능 |
| 🛠️ 복잡한 SQL 지원 | JOIN, 윈도우 함수, CTE, JSON 처리, UDF까지 가능 |
| 🔌 외부 라이브러리 없이 사용 | Pandas 없이도 단독 SQL 분석 가능 |
| 🐍 Jupyter 친화적 | Notebook에서 SQL ↔ Python 자유롭게 전환 가능 |

---
