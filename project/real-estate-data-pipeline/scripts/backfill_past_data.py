import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import pandas as pd
from dotenv import load_dotenv
from PublicDataReader import TransactionPrice 
from sqlalchemy import create_engine, text


from utils.get_region_codes import get_seoul_sigungu_codes

load_dotenv(dotenv_path="../.env") 

# --- 1. 초기 설정: 스크립트 실행을 위한 준비 단계 ---

print(">> 스크립트 초기 설정 시작...")

PUBLIC_DATA_API_KEY = os.getenv("PUBLIC_DATA_HUB")
DATABASE_URL = os.getenv("DATABASE_URL_HOST")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

# 필수 환경 변수가 있는지 확인합니다.
if not PUBLIC_DATA_API_KEY or not DATABASE_URL:
    raise ValueError("PUBLIC_DATA_HUB 또는 DATABASE_URL_HOST 환경 변수가 설정되지 않았습니다.")

# 데이터베이스 연결 엔진을 생성합니다.
print(">> 데이터베이스 연결 시도 ")
try:
    # pool_pre_ping과 pool_recycle 옵션을 추가하여 안정성을 높입니다.
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=3600
    )

    # 순수 SQLAlchemy 연결 테스트를 먼저 수행합니다.
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        print(f"✅ [진단 성공] 순수 SQLAlchemy 연결 테스트 성공! DB 응답: {result.scalar()}")

except Exception as e:
    print(f"❌ [진단 실패] 데이터베이스 연결 중 심각한 오류 발생: {e}")
    # 연결 실패 시 스크립트를 중단하는 것이 안전합니다.
    sys.exit(1)


# PublicDataReader 인스턴스를 생성합니다.
api = TransactionPrice(PUBLIC_DATA_API_KEY)
print("✅ PublicDataReader 초기화 완료!")

print(">> 모든 초기 설정 완료. \n")


def fetch_and_save_apt_trade_data(sigungu_code: str, year_month: str, district_name: str):
    """지정된 지역과 연월의 '아파트 매매' 실거래가 데이터를 가져와 DB에 저장합니다."""
    
    # table_name = "raw_apt_trade"  # 매매 데이터를 저장할 테이블 이름
    table_name = "raw_apt_jeonse" # 전월세 데이터를 저장할 테이블 이름
    
    print(f"-> [{district_name}({sigungu_code}) / {year_month}] 데이터 수집 시도...")
    
    try:
        # TransactionPrice API를 사용하여 데이터 조회
        original_df = api.get_data(
            property_type="아파트",
            trade_type="전월세", #전월세, 매매
            sigungu_code=sigungu_code,
            year_month=year_month,
        )

        if original_df.empty:
            print(f"   INFO: 데이터 없음.")
            return

        # PublicDataReader가 반환한 DataFrame을 순수 pandas DataFrame으로 변환합니다.
        df = pd.DataFrame(original_df)
        
        with engine.connect() as connection:
            df.to_sql(
                name=table_name,
                con=connection,  # engine이 아닌 connection을 전달
                schema=DB_SCHEMA,
                if_exists="append",
                index=False
            )
        print(f"   SUCCESS: {len(df)}건 저장 완료.")

    except Exception as e:
        # 특정 요청에서 에러가 발생하더라도 전체 스크립트가 멈추지 않도록 처리
        print(f"   ERROR: 데이터 처리 중 오류 발생: {e}")


# --- 테스트를 위해 함수를 한번 직접 호출해봅시다 ---
if __name__ == "__main__":
    
    # 1. 수집할 기간과 지역 목록을 정의합니다.
    start_year, start_month = 2011, 1
    end_year, end_month = 2025, 7
    
    seoul_codes = get_seoul_sigungu_codes()
    
    print("--- 과거 데이터 대량 적재(Backfill) 시작 ---")
    print(f"수집 기간: {start_year}-{start_month:02d} ~ {end_year}-{end_month:02d}")
    print(f"수집 대상: 서울시 {len(seoul_codes)}개 자치구")
    
    # 2. 월별로 순회하는 루프를 만듭니다.
    for year in range(start_year, end_year + 1):
        # 시작 연도는 지정된 월부터, 그 외 연도는 1월부터 시작
        s_month = start_month if year == start_year else 1
        # 종료 연도는 지정된 월까지, 그 외 연도는 12월까지
        e_month = end_month if year == end_year else 12

        for month in range(s_month, e_month + 1):
            year_month_str = f"{year}{month:02d}"
            
            # 3. 각 월마다 모든 지역을 순회합니다.
            for district, code in seoul_codes.items():
                fetch_and_save_apt_trade_data(
                    sigungu_code=code,
                    year_month=year_month_str,
                    district_name=district
                )
    
    print("\n--- 모든 데이터 적재 완료 ---")

