import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import warnings

# 경고 메시지 무시
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# --- 서울시 시군구 코드-이름 매핑 정보 ---
SEOUL_SGG_MAP = {
    "11110": "종로구", "11140": "중구", "11170": "용산구", "11200": "성동구",
    "11215": "광진구", "11230": "동대문구", "11260": "중랑구", "11290": "성북구",
    "11305": "강북구", "11320": "도봉구", "11350": "노원구", "11380": "은평구",
    "11410": "서대문구", "11440": "마포구", "11470": "양천구", "11500": "강서구",
    "11530": "구로구", "11545": "금천구", "11560": "영등포구", "11590": "동작구",
    "11620": "관악구", "11650": "서초구", "11680": "강남구", "11710": "송파구",
    "11740": "강동구"
}

def get_db_engine():
    """데이터베이스 연결 엔진을 생성하고 반환합니다."""
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)

    database_url = os.getenv("DATABASE_URL_HOST")
    if not database_url:
        raise ValueError("DATABASE_URL_HOST 환경 변수가 설정되지 않았습니다.")

    print("데이터베이스 연결 엔진 생성 중...")
    engine = create_engine(database_url)
    print("✅ 데이터베이스 연결 성공!")
    return engine

def process_trade_data(engine, schema):
    """매매 데이터를 처리하고 피처를 생성합니다."""
    print("--- [1/4] 매매 데이터 처리 시작 ---")
    df = pd.read_sql(f'SELECT * FROM {schema}."raw_apt_trade"', engine)
    print(f">> 매매 데이터 {len(df)}건 로딩 완료.")

    numeric_cols = ['dealAmount', 'excluUseAr', 'dealYear', 'dealMonth', 'dealDay', 'buildYear', 'floor']
    for col in numeric_cols:
        if col == 'dealAmount':
             df[col] = pd.to_numeric(df[col].str.replace(',', '').str.strip(), errors='coerce')
        else:
             df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df.dropna(subset=['dealAmount', 'excluUseAr'], inplace=True)
    df = df[df['floor'] > 0].copy()
    df['deal_datetime'] = pd.to_datetime(df['dealYear'].astype(str) + '-' + df['dealMonth'].astype(str) + '-' + df['dealDay'].astype(str), errors='coerce')
    df['price_per_pyeong'] = df['dealAmount'] / (df['excluUseAr'] / 3.3058)
    df['sggnm'] = df['sggCd'].map(SEOUL_SGG_MAP)
    df.dropna(subset=['sggnm'], inplace=True)
    
    dong_avg_price = df.groupby(['sggnm', 'umdNm'])['price_per_pyeong'].transform('mean')
    df['dong_avg_price'] = dong_avg_price

    column_rename_map = {
        'sggCd': '시군구코드', 'umdCd': '읍면동코드', 'jibun': '지번', 'aptNm': '아파트명',
        'excluUseAr': '전용면적(㎡)', 'floor': '층', 'buildYear': '건축년도',
        'dealAmount': '거래금액(만원)', 'deal_datetime': '거래일자', 'sggnm': '시군구명',
        'umdNm': '읍면동명', 'price_per_pyeong': '평당가격(만원)', 'dong_avg_price': '동별평균평당가(만원)'
    }
    df.rename(columns=column_rename_map, inplace=True)
    df_final = df[list(column_rename_map.values())].copy()
    df_final['거래일자'] = pd.to_datetime(df_final['거래일자']).dt.date

    print("--- 매매 데이터 처리 완료 ---")
    return df_final

def process_rent_data(engine, schema):
    """전월세 데이터를 처리하여 '전세'와 '월세' 테이블을 각각 생성합니다."""
    print("--- [2/4] 전월세 데이터 처리 시작 ---")
    df = pd.read_sql(f'SELECT * FROM {schema}."raw_apt_jeonse"', engine)
    print(f">> 전월세 데이터 {len(df)}건 로딩 완료.")

    numeric_cols = ['deposit', 'monthlyRent', 'excluUseAr', 'buildYear', 'dealYear', 'dealMonth', 'dealDay', 'floor']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df.dropna(subset=['deposit', 'excluUseAr'], inplace=True)
    
    df['rent_type'] = np.where(df['monthlyRent'].fillna(0) == 0, '전세', '월세')
    df['deal_datetime'] = pd.to_datetime(df['dealYear'].astype(str) + '-' + df['dealMonth'].astype(str) + '-' + df['dealDay'].astype(str), errors='coerce')
    df['sggnm'] = df['sggCd'].map(SEOUL_SGG_MAP)
    df.dropna(subset=['sggnm'], inplace=True)

    term = df['contractTerm'].str.split('~', expand=True)
    df['contract_start_date'] = pd.to_datetime('20' + term[0].str.replace('.', '-', regex=False), errors='coerce')
    df['contract_end_date'] = pd.to_datetime('20' + term[1].str.replace('.', '-', regex=False), errors='coerce')
    
    # '진짜 전세' 그룹 정의 (평균 계산용)
    true_jeonse_mask = (df['rent_type'] == '전세') & (df['deposit'] > 0) & (df['excluUseAr'] > 0)
    df_true_jeonse = df[true_jeonse_mask].copy()
    df_true_jeonse['price_per_pyeong'] = df_true_jeonse['deposit'] / (df_true_jeonse['excluUseAr'] * 3.3058)

    # '월세' 그룹 정의 (평균 계산용)
    wolse_mask = (df['rent_type'] == '월세') & (df['excluUseAr'] > 0)
    df_wolse = df[wolse_mask].copy()
    df_wolse['deposit_per_pyeong'] = df_wolse['deposit'] / (df_wolse['excluUseAr'] * 3.3058)

    # 각 동네별 평균 '지도' 생성
    jeonse_avg_map = df_true_jeonse.groupby(['sggnm', 'umdNm'])['price_per_pyeong'].mean()
    wolse_deposit_avg_map = df_wolse.groupby(['sggnm', 'umdNm'])['deposit_per_pyeong'].mean()
    wolse_rent_avg_map = df_wolse.groupby(['sggnm', 'umdNm'])['monthlyRent'].mean()

    # .map()을 이용해 전체 데이터프레임에 평균값 적용
    df['sggnm_avg_pp_jeonse'] = df.set_index(['sggnm', 'umdNm']).index.map(jeonse_avg_map)
    df['umdNm_avg_pp_jeonse'] = df.set_index(['sggnm', 'umdNm']).index.map(jeonse_avg_map)
    df['sggnm_avg_pp_wolse_deposit'] = df.set_index(['sggnm', 'umdNm']).index.map(wolse_deposit_avg_map)
    df['umdNm_avg_pp_wolse_deposit'] = df.set_index(['sggnm', 'umdNm']).index.map(wolse_deposit_avg_map)
    df['sggnm_avg_monthly_rent'] = df.set_index(['sggnm', 'umdNm']).index.map(wolse_rent_avg_map)
    df['umdNm_avg_monthly_rent'] = df.set_index(['sggnm', 'umdNm']).index.map(wolse_rent_avg_map)

    # 전세/월세 데이터 분리
    df_jeonse_final = df[df['rent_type'] == '전세'].copy()
    df_wolse_final = df[df['rent_type'] == '월세'].copy()
    
    # 전세 테이블 컬럼 정리
    jeonse_rename_map = {
        'sggCd': '시군구코드', 'umdNm': '읍면동명', 'jibun': '지번', 'aptNm': '아파트명', 'excluUseAr': '전용면적(㎡)', 'floor': '층', 
        'buildYear': '건축년도', 'deposit': '보증금(만원)', 'deal_datetime': '거래일자', 'sggnm': '시군구명', 'rent_type': '거래유형', 
        'contract_start_date': '계약시작일', 'contract_end_date': '계약종료일', 'sggnm_avg_pp_jeonse': '구별평균평당전세가(만원)', 
        'umdNm_avg_pp_jeonse': '동별평균평당전세가(만원)'
    }
    df_jeonse_final = df_jeonse_final[list(jeonse_rename_map.keys())].rename(columns=jeonse_rename_map)

    # 월세 테이블 컬럼 정리
    wolse_rename_map = {
        'sggCd': '시군구코드', 'umdNm': '읍면동명', 'jibun': '지번', 'aptNm': '아파트명', 'excluUseAr': '전용면적(㎡)', 'floor': '층', 
        'buildYear': '건축년도', 'deposit': '보증금(만원)', 'monthlyRent': '월세(만원)', 'deal_datetime': '거래일자', 'sggnm': '시군구명', 
        'rent_type': '거래유형', 'contract_start_date': '계약시작일', 'contract_end_date': '계약종료일', 
        'sggnm_avg_pp_wolse_deposit': '구별평균평당월세보증금(만원)', 'umdNm_avg_pp_wolse_deposit': '동별평균평당월세보증금(만원)', 
        'sggnm_avg_monthly_rent': '구별평균월세(만원)', 'umdNm_avg_monthly_rent': '동별평균월세(만원)'
    }
    df_wolse_final = df_wolse_final[list(wolse_rename_map.keys())].rename(columns=wolse_rename_map)

    # 날짜 컬럼 타입 변환
    for col in ['거래일자', '계약시작일', '계약종료일']:
        df_jeonse_final[col] = pd.to_datetime(df_jeonse_final[col]).dt.date
        df_wolse_final[col] = pd.to_datetime(df_wolse_final[col]).dt.date

    print("--- 전월세 데이터 처리 완료 ---")
    return df_jeonse_final, df_wolse_final

def analyze_gap_investment(df_trade, df_jeonse):
    """갭투자 데이터를 분석합니다."""
    print("--- [3/4] 갭투자 분석 시작 ---")
    sales_df = df_trade.copy()
    jeonse_df = df_jeonse.copy()

    sales_df['거래일자'] = pd.to_datetime(sales_df['거래일자'])
    jeonse_df['계약시작일'] = pd.to_datetime(jeonse_df['계약시작일'])
    jeonse_df['계약종료일'] = pd.to_datetime(jeonse_df['계약종료일'])
    
    sales_df = sales_df[sales_df['거래일자'].dt.year >= 2022].copy()
    jeonse_df.dropna(subset=['계약시작일', '계약종료일'], inplace=True)
    sales_df['거래년도'] = sales_df['거래일자'].dt.year

    sales_df['전용면적(㎡)'] = sales_df['전용면적(㎡)'].round(2)
    jeonse_df['전용면적(㎡)'] = jeonse_df['전용면적(㎡)'].round(2)
    
    apartment_key = ['시군구명', '읍면동명', '지번', '아파트명', '전용면적(㎡)', '층']
    sales_df['매매ID'] = range(len(sales_df))
    total_sales = sales_df.groupby(['거래년도', '시군구명', '읍면동명']).size().rename('총매매건수')
    
    merged_df = pd.merge(sales_df, jeonse_df[apartment_key + ['계약시작일', '계약종료일']], on=apartment_key, how='inner')
    
    gap_mask = (merged_df['거래일자'] >= merged_df['계약시작일']) & (merged_df['거래일자'] <= merged_df['계약종료일'])
    
    unique_gap_deals = merged_df[gap_mask].drop_duplicates(subset=['매매ID'])
    gap_counts = unique_gap_deals.groupby(['거래년도', '시군구명', '읍면동명']).size().rename('갭투자건수')
    summary_df = pd.concat([total_sales, gap_counts], axis=1).fillna(0).astype(int)
    
    summary_df['갭투자비율(%)'] = 0.0
    non_zero_mask = summary_df['총매매건수'] > 0
    summary_df.loc[non_zero_mask, '갭투자비율(%)'] = ((summary_df.loc[non_zero_mask, '갭투자건수'] / summary_df.loc[non_zero_mask, '총매매건수']) * 100).round(2)

    print(f">> 갭투자 분석 완료. 총 {len(summary_df)}개 동별 데이터 생성. (병합: {len(merged_df)}건, 갭투자: {len(unique_gap_deals)}건)")
    print("--- 갭투자 분석 완료 ---")
    return summary_df.reset_index()

def save_to_db(df, table_name, engine, schema):
    """데이터프레임을 데이터베이스 테이블에 저장합니다."""
    print(f">> '{table_name}' 테이블 저장 중... ({len(df)}건)")
    df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
    print(f"✅ '{table_name}' 테이블 저장 완료.")

def main():
    """메인 실행 함수"""
    print("--- 데이터 피처 엔지니어링 스크립트 시작 ---")
    engine = get_db_engine()
    schema = os.getenv("DB_SCHEMA", "public")

    feature_trade_df = process_trade_data(engine, schema)
    feature_jeonse_df, feature_wolse_df = process_rent_data(engine, schema)
    analytics_gap_df = analyze_gap_investment(feature_trade_df, feature_jeonse_df)

    print("\n--- [4/4] 최종 데이터베이스 저장 시작 ---")
    save_to_db(feature_trade_df, "feature_apt_trade", engine, schema)
    save_to_db(feature_jeonse_df, "feature_apt_jeonse", engine, schema)
    save_to_db(feature_wolse_df, "feature_apt_wolse", engine, schema)
    save_to_db(analytics_gap_df, "analytics_gap_investment", engine, schema)

    print("\n🎉 --- 모든 작업이 성공적으로 완료되었습니다. --- 🎉")

if __name__ == "__main__":
    main()