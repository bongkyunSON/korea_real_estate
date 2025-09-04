import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import warnings

# 경고 메시지 무시
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

def get_db_engine():
    """데이터베이스 연결 엔진을 생성하고 반환합니다."""
    dotenv_path = os.path.join(os.path.dirname(os.getcwd()), '.env')
    if not os.path.exists(dotenv_path):
         dotenv_path = os.path.join(os.getcwd(), '.env')
            
    load_dotenv(dotenv_path=dotenv_path)
    
    database_url = os.getenv("DATABASE_URL_HOST")
    if not database_url:
        raise ValueError("DATABASE_URL_HOST 환경 변수가 설정되지 않았습니다.")
    
    return create_engine(database_url)

def process_trade_data(engine, schema):
    """매매 데이터를 처리하고 피처를 생성합니다."""
    print("--- [1/4] 매매 데이터 처리 시작 ---")
    
    print(">> raw_apt_trade 테이블 로딩 중...")
    df = pd.read_sql(f'SELECT * FROM {schema}."raw_apt_trade"', engine)
    print(f">> 매매 데이터 {len(df)}건 로딩 완료.")

    # 데이터 정제 및 타입 변환
    df['dealAmount'] = pd.to_numeric(df['dealAmount'].str.replace(',', '').str.strip(), errors='coerce')
    numeric_cols = ['excluUseAr', 'dealYear', 'dealMonth', 'dealDay', 'buildYear', 'floor']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 결측치 및 이상치 처리
    df.dropna(subset=['dealAmount', 'excluUseAr'], inplace=True)
    df = df[df['floor'] > 0].copy()

    # 피처 엔지니어링
    df['deal_datetime'] = pd.to_datetime(df['dealYear'].astype(str) + '-' + 
                                       df['dealMonth'].astype(str) + '-' + 
                                       df['dealDay'].astype(str), errors='coerce')
    df['price_per_pyeong'] = df['dealAmount'] / (df['excluUseAr'] / 3.3058)

    sgg_map = df.groupby('sggCd')['sggNm'].first().to_dict()
    df['sggNm'] = df['sggCd'].map(sgg_map)

    dong_avg_price = df.groupby(['sggNm', 'umdNm'])['price_per_pyeong'].transform('mean')
    df['dong_avg_price'] = dong_avg_price
    
    # 불필요한 컬럼 제거
    final_cols = [
        'sggCd', 'umdCd', 'jibun', 'aptNm', 'excluUseAr', 'floor', 'buildYear', 
        'dealAmount', 'deal_datetime', 'sggNm', 'umdNm', 
        'price_per_pyeong', 'dong_avg_price'
    ]
    df_final = df[final_cols].copy()
    
    print("--- 매매 데이터 처리 완료 ---")
    return df_final

def process_rent_data(engine, schema):
    """전월세 데이터를 처리하고 피처를 생성합니다."""
    print("--- [2/4] 전월세 데이터 처리 시작 ---")
    
    print(">> raw_apt_jeonse 테이블 로딩 중...")
    df = pd.read_sql(f'SELECT * FROM {schema}."raw_apt_jeonse"', engine)
    print(f">> 전월세 데이터 {len(df)}건 로딩 완료.")

    # 데이터 정제 및 타입 변환
    numeric_cols = [
        'deposit', 'monthlyRent', 'excluUseAr', 'buildYear', 'dealYear', 
        'dealMonth', 'dealDay', 'floor', 'preDeposit', 'preMonthlyRent'
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df.dropna(subset=['deposit', 'monthlyRent', 'excluUseAr'], inplace=True)

    # 피처 엔지니어링
    df['rent_type'] = np.where(df['monthlyRent'] == 0, '전세', '월세')
    df['deal_datetime'] = pd.to_datetime(df['dealYear'].astype(str) + '-' + 
                                       df['dealMonth'].astype(str) + '-' + 
                                       df['dealDay'].astype(str), errors='coerce')

    sgg_map = df.groupby('sggCd')['sggNm'].first().to_dict()
    df['sggNm'] = df['sggCd'].map(sgg_map)
    
    # 계약 기간 파싱
    term = df['contractTerm'].str.split('~', expand=True)
    df['contract_start_date'] = pd.to_datetime('20' + term[0].str.replace('.', '-'), errors='coerce')
    df['contract_end_date'] = pd.to_datetime('20' + term[1].str.replace('.', '-'), errors='coerce')

    # 시장 특성 피처 생성
    df_jeonse = df[df['rent_type'] == '전세'].copy()
    df_wolse = df[df['rent_type'] == '월세'].copy()

    df_jeonse['price_per_pyeong'] = df_jeonse['deposit'] / (df_jeonse['excluUseAr'] / 3.3058)
    df_wolse['deposit_per_pyeong'] = df_wolse['deposit'] / (df_wolse['excluUseAr'] / 3.3058)

    # 구별/동별 평균값 계산
    for group_col in ['sggNm', 'umdNm']:
        # 전세
        jeonse_avg = df_jeonse.groupby(group_col)['price_per_pyeong'].transform('mean')
        df[f'{group_col}_avg_pp_jeonse'] = jeonse_avg
        # 월세
        wolse_deposit_avg = df_wolse.groupby(group_col)['deposit_per_pyeong'].transform('mean')
        df[f'{group_col}_avg_pp_wolse_deposit'] = wolse_deposit_avg
        wolse_rent_avg = df_wolse.groupby(group_col)['monthlyRent'].transform('mean')
        df[f'{group_col}_avg_monthly_rent'] = wolse_rent_avg

    final_cols = [
        'sggCd', 'umdCd', 'jibun', 'aptNm', 'excluUseAr', 'floor', 'buildYear', 'deposit', 'monthlyRent',
        'deal_datetime', 'sggNm', 'umdNm', 'rent_type', 'contract_start_date', 'contract_end_date',
        'sggNm_avg_pp_jeonse', 'umdNm_avg_pp_jeonse', 'sggNm_avg_pp_wolse_deposit',
        'umdNm_avg_pp_wolse_deposit', 'sggNm_avg_monthly_rent', 'umdNm_avg_monthly_rent'
    ]
    df_final = df[final_cols].copy()
    
    print("--- 전월세 데이터 처리 완료 ---")
    return df_final

def analyze_gap_investment(df_trade, df_rent):
    """갭투자 데이터를 분석합니다."""
    print("--- [3/4] 갭투자 분석 시작 ---")

    sales_df = df_trade[df_trade['deal_datetime'].dt.year >= 2022].copy()
    sales_df['deal_year'] = sales_df['deal_datetime'].dt.year

    jeonse_df = df_rent[
        (df_rent['rent_type'] == '전세') &
        (df_rent['contract_start_date'].notna()) &
        (df_rent['contract_end_date'].dt.year >= 2022)
    ].copy()

    apartment_key = ['sggNm', 'umdNm', 'jibun', 'aptNm', 'excluUseAr', 'floor']
    sales_df['sale_id'] = range(len(sales_df))

    total_sales = sales_df.groupby(['deal_year', 'sggNm', 'umdNm']).size().rename('총 매매 건수')

    merged_df = pd.merge(sales_df, jeonse_df[apartment_key + ['contract_start_date', 'contract_end_date']], on=apartment_key, how='inner')
    
    gap_mask = (merged_df['deal_datetime'] >= merged_df['contract_start_date']) & \
               (merged_df['deal_datetime'] <= merged_df['contract_end_date'])

    unique_gap_deals = merged_df[gap_mask].drop_duplicates(subset=['sale_id'])
    gap_counts = unique_gap_deals.groupby(['deal_year', 'sggNm', 'umdNm']).size().rename('갭투자 건수')

    summary_df = pd.concat([total_sales, gap_counts], axis=1).fillna(0).astype(int)
    summary_df['갭투자 비율(%)'] = ((summary_df['갭투자 건수'] / summary_df['총 매매 건수']) * 100).round(2)
    
    print(f">> 갭투자 분석 완료. 총 {len(summary_df)}개 동별 데이터 생성.")
    print("--- 갭투자 분석 완료 ---")
    return summary_df.reset_index()

def save_to_db(df, table_name, engine, schema):
    """데이터프레임을 데이터베이스 테이블에 저장합니다."""
    print(f">> '{table_name}' 테이블 저장 중... ({len(df)}건)")
    df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
    print(f">> '{table_name}' 테이블 저장 완료.")

def main():
    """메인 실행 함수"""
    engine = get_db_engine()
    schema = os.getenv("DB_SCHEMA", "public")
    
    # 1. 매매 데이터 처리 및 저장
    feature_trade_df = process_trade_data(engine, schema)
    
    # 2. 전월세 데이터 처리 및 저장
    feature_rent_df = process_rent_data(engine, schema)
    
    # 3. 갭투자 분석
    analytics_gap_df = analyze_gap_investment(feature_trade_df, feature_rent_df)
    
    # 4. 최종 테이블 저장
    print("\n--- [4/4] 최종 데이터베이스 저장 시작 ---")
    save_to_db(feature_trade_df, "feature_apt_trade", engine, schema)
    save_to_db(feature_rent_df, "feature_apt_rent", engine, schema)
    save_to_db(analytics_gap_df, "analytics_gap_investment", engine, schema)
    print("--- 모든 작업이 성공적으로 완료되었습니다. ---")

if __name__ == "__main__":
    main()
