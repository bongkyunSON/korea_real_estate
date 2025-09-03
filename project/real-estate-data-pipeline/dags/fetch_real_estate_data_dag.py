from __future__ import annotations

import os
from datetime import datetime

import pendulum
from airflow.decorators import dag, task

@dag(
    dag_id="fetch_real_estate_data",
    # 매월 1일 새벽 3시에 실행되도록 설정합니다.
    schedule="0 3 1 * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["real_estate", "api", "final"],
)
def fetch_real_estate_data_dag():
    """
    매월 초, 국토교통부 API를 통해 '지난달'의 서울시 전체 아파트 매매 실거래가
    데이터를 수집하여 DB에 증분 적재하는 DAG입니다.
    
    데이터 처리 방식:
    1. DB에서 해당 월의 기존 데이터를 읽어옵니다.
    2. API에서 해당 월의 신규 데이터를 모두 가져옵니다.
    3. 두 데이터를 비교하여 중복되지 않는 '순수 신규' 데이터만 DB에 추가합니다.
    """

    @task
    def get_target_month(data_interval_start: pendulum.DateTime) -> str:
        """
        Airflow의 실행 시점을 기준으로 '지난달'을 YYYYMM 형식의 문자열로 변환합니다.
        """
        target_month_str = data_interval_start.strftime("%Y%m")
        print(f"이번 작업의 대상 월은 '{target_month_str}' 입니다.")
        return target_month_str

    @task
    def process_data_for_month(target_month: str):
        """
        특정 월의 데이터를 '비교 후 증분 적재' 방식으로 처리합니다.
        모든 외부 라이브러리 import와 객체 생성은 이 Task 내부에서 수행됩니다.
        """
        # --- 1. Task 내부에서 모든 의존성 import 및 초기화 ---
        import sys
        import pandas as pd
        from dotenv import load_dotenv
        from sqlalchemy import create_engine, text
        from PublicDataReader import TransactionPrice

        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from utils.get_region_codes import get_seoul_sigungu_codes
        
        load_dotenv()
        DATABASE_URL = os.getenv("DATABASE_URL")
        PUBLIC_DATA_API_KEY = os.getenv("PUBLIC_DATA_HUB")
        DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

        if not PUBLIC_DATA_API_KEY or not DATABASE_URL:
            raise ValueError("API 키 또는 데이터베이스 URL이 .env 파일에 설정되지 않았습니다.")
        
        engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=3600)
        api = TransactionPrice(PUBLIC_DATA_API_KEY)

        print(f"'{target_month}' 데이터 처리 시작 (비교 후 증분 적재 방식)")
        table_name = "raw_apt_trade"

        # --- 2. DB에서 기존 데이터 읽어오기 ---
        try:
            with engine.connect() as connection:
                query = text(f'SELECT * FROM {DB_SCHEMA}."{table_name}" WHERE "수집월" = :month')
                existing_df = pd.read_sql(query, connection, params={"month": target_month})
            print(f">> [DB 읽기] 성공. '{target_month}'월의 기존 데이터 {len(existing_df)}건을 찾았습니다.")
        except Exception as e:
            print(f">> [DB 읽기] 실패 또는 데이터 없음: {e}")
            existing_df = pd.DataFrame()

        # --- 3. API에서 신규 데이터 수집하기 ---
        print(f">> [API 수집] 시작. '{target_month}'월의 신규 데이터를 수집합니다.")
        seoul_codes = get_seoul_sigungu_codes()
        api_df_list = []
        for district, code in seoul_codes.items():
            try:
                original_df = api.get_data(
                    property_type="아파트",
                    trade_type="매매",
                    sigungu_code=code,
                    year_month=target_month,
                )
                if not original_df.empty:
                    api_df_list.append(pd.DataFrame(original_df))
            except Exception as e:
                print(f"   - {district} API 호출 중 오류 발생 (건너뜁니다): {e}")
        
        if not api_df_list:
            print(">> [API 수집] 신규 데이터가 없어 작업을 중단합니다.")
            return
        
        api_df = pd.concat(api_df_list, ignore_index=True)
        print(f">> [API 수집] 성공. 총 {len(api_df)}건의 데이터를 API로부터 가져왔습니다.")

        # --- 4. 데이터 비교 및 신규 데이터만 필터링 ---
        print(">> [데이터 비교] 시작. 기존 데이터와 신규 데이터를 비교합니다.")
        if not existing_df.empty:
            existing_df = existing_df[api_df.columns]
        
        combined_df = pd.concat([api_df, existing_df], ignore_index=True)
        new_data_df = combined_df.drop_duplicates(keep=False)

        # --- 5. 신규 데이터가 있을 경우에만 DB에 저장 ---
        if new_data_df.empty:
            print(">> [최종 저장] 신규 데이터가 없습니다. 작업을 종료합니다.")
            return

        print(f">> [최종 저장] {len(new_data_df)}건의 새로운 데이터를 DB에 저장합니다.")
        try:
            with engine.connect() as connection:
                new_data_df.to_sql(
                    name=table_name,
                    con=connection,
                    schema=DB_SCHEMA,
                    if_exists="append",
                    index=False,
                )
            print(">> [최종 저장] 성공!")
        except Exception as e:
            print(f">> [최종 저장] 실패: {e}")
            raise e

    # --- Task 실행 순서 정의 ---
    target_month_value = get_target_month(data_interval_start="{{ data_interval_start }}")
    process_data_for_month(target_month=target_month_value)

# Airflow가 DAG 객체를 인식할 수 있도록 변수에 할당합니다.
fetch_real_estate_data = fetch_real_estate_data_dag()