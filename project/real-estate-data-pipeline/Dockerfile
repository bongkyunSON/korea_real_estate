# 1. 베이스 이미지 선택 (파이썬 3.9 버전 사용)
FROM apache/airflow:2.9.2-python3.12

# 2. Airflow 사용자 설정
USER airflow

# 3. 필요한 파이썬 라이브러리 설치
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt