"""
DAG Factory를 사용하여 생성된 DAG는 Airflow의 DAGs 폴더에 메모리 상에서 동적으로 로드됩니다.
즉, DAG가 파일로 직접 저장되는 것이 아니라, Airflow 실행 시 Python 코드 내에서 globals()를 사용하여 동적으로 DAG가 등록됩니다.

Python의 globals()는 현재 실행 중인 전역(global) 네임스페이스에 있는 모든 변수와 객체를 딕셔너리 형태로 반환하는 함수입니다.
즉, 실행 중인 Python 스크립트에서 전역 변수를 동적으로 추가하거나 변경할 수 있도록 해주는 기능입니다.

많아질 수록 메모리에 대한 부담이 있으므로, python file로 변환해서 만들어 둔다 거나 하는 방향도 필요해 보임
"""


import os
from pathlib import Path

# The following import is here so Airflow parses this file
# from airflow import DAG
import dagfactory

DEFAULT_CONFIG_ROOT_DIR = "C:/Users/jiho3/IdeaProjects/python_libraries/airflow_dagfactory"

CONFIG_ROOT_DIR = Path(os.getenv("CONFIG_ROOT_DIR", DEFAULT_CONFIG_ROOT_DIR))

config_file = str(CONFIG_ROOT_DIR / "example_dag_factory.yml")

example_dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
example_dag_factory.clean_dags(globals())
example_dag_factory.generate_dags(globals())
