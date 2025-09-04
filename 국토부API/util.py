import os
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from PublicDataReader import TransactionPrice

load_dotenv()

service_key = os.getenv("PUBLIC_DATA_HUB")
api = TransactionPrice(service_key)

# DB 연결 문자열과 스키마를 환경변수에서 우선 적용
# DATABASE_URL 예) postgresql://user:pass@host.docker.internal:5432/dbname
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres:5432/airflow")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=3600)
with engine.connect() as conn:
    conn.execute(text("SELECT 1"))
print("✅ PostgreSQL 연결 성공")

# pandas 호환을 위한 문자열 (현재 pandas 직접 연결은 사용하지 않음)
DATABASE = DATABASE_URL

seoul_gungu_list = [
        ("1111000000", "종로구"),
        ("1114000000", "중구"), 
        ("1117000000", "용산구"),
        ("1120000000", "성동구"),
        ("1121500000", "광진구"),
        ("1123000000", "동대문구"),
        ("1126000000", "중랑구"),
        ("1129000000", "성북구"),
        ("1130500000", "강북구"),
        ("1132000000", "도봉구"),
        ("1135000000", "노원구"),
        ("1138000000", "은평구"),
        ("1141000000", "서대문구"),
        ("1144000000", "마포구"),
        ("1147000000", "양천구"),
        ("1150000000", "강서구"),
        ("1153000000", "구로구"),
        ("1154500000", "금천구"),
        ("1156000000", "영등포구"),
        ("1159000000", "동작구"),
        ("1162000000", "관악구"),
        ("1165000000", "서초구"),
        ("1168000000", "강남구"),
        ("1171000000", "송파구"),
        ("1174000000", "강동구")
    ]


def generate_months(start_year=2000, start_month=1, end_year=None, end_month=None):
    """
    동적으로 년월 리스트를 생성하는 함수

    Args:
        start_year (int): 시작 연도 (기본값: 2000)
        start_month (int): 시작 월 (기본값: 1)
        end_year (int): 종료 연도 (기본값: 현재 연도)
        end_month (int): 종료 월 (기본값: 현재 월)

    Returns:
        list: "YYYYMM" 형태의 문자열 리스트
    """
    if end_year is None:
        end_year = datetime.now().year
    if end_month is None:
        end_month = datetime.now().month

    months = []
    current_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 1)

    while current_date <= end_date:
        months.append(current_date.strftime("%Y%m"))
        current_date += relativedelta(months=1)

    return months


def generate_recent_months(months_count=6):
    """
    최근 N개월의 년월 리스트를 생성

    Args:
        months_count (int): 최근 몇 개월인지 (기본값: 6)

    Returns:
        list: "YYYYMM" 형태의 문자열 리스트
    """
    months = []
    current_date = datetime.now().replace(day=1)

    for i in range(months_count):
        months.append(current_date.strftime("%Y%m"))
        current_date -= relativedelta(months=1)

    return list(reversed(months))


def generate_year_months(year):
    """
    특정 연도의 모든 월을 생성

    Args:
        year (int): 연도

    Returns:
        list: "YYYYMM" 형태의 문자열 리스트
    """
    return [f"{year}{month:02d}" for month in range(1, 13)]


# 호환성을 위한 기본 months 리스트 (필요시 사용)
months = generate_months(2000, 1, 2025, 6)

# 실제 사용 예시들
if __name__ == "__main__":
    # 전체 기간 (2000년 1월 ~ 현재)
    all_months = generate_months()
    print(f"전체 기간: {len(all_months)}개월")
    print(f"최근 5개: {all_months[-5:]}")

    # 최근 6개월만
    recent_months = generate_recent_months(6)
    print(f"최근 6개월: {recent_months}")

    # 2024년 전체
    year_2024 = generate_year_months(2024)
    print(f"2024년 전체: {year_2024}")

    # 특정 기간
    period_months = generate_months(2023, 1, 2024, 12)
    print(f"2023-2024 기간: {len(period_months)}개월")
