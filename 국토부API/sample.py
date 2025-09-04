import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from util import engine, api, generate_recent_months, seoul_gungu_list, DB_SCHEMA

def fetch_api_data(trade_type, month, sigungu_code, sigungu_name):
    """지정된 조건으로 국토부 API로부터 데이터를 가져옵니다."""
    try:
        df = api.get_data(
            property_type="아파트",
            trade_type=trade_type,
            sigungu_code=sigungu_code,
            year_month=month,
        )
        if df is not None and not df.empty:
            df["수집월"] = month
            df["구코드"] = sigungu_code
            df["구명"] = sigungu_name
            print(f"  ✅ [API] {sigungu_name} {month} ({trade_type}): {len(df)}건")
            return df
    except Exception as e:
        print(f"  ❌ [API] {sigungu_name} {month} ({trade_type}) 오류: {e}")
    return pd.DataFrame()

def fetch_db_data(table_name, month):
    """데이터베이스에서 특정 월의 데이터를 가져옵니다."""
    query_columns = {
        "real_estate_sales": '"법정동시군구코드", "법정동읍면동코드", "법정동지번코드", "법정동본번코드", "법정동부번코드", "도로명", "도로명시군구코드", "도로명코드", "도로명일련번호코드", "도로명지상지하코드", "도로명건물본번호코드", "도로명건물부번호코드", "법정동", "단지명", "지번", "전용면적", "계약년도", "계약월", "계약일", "거래금액", "층", "건축년도", "단지일련번호", "해제여부", "해제사유발생일", "거래유형", "중개사소재지", "등기일자", "아파트동명", "매도자", "매수자", "토지임대부아파트여부", "수집월", "구코드", "구명"',
        "real_estate_jeonse": '"법정동시군구코드", "법정동", "단지명", "지번", "전용면적", "계약년도", "계약월", "계약일", "보증금액", "월세금액", "층", "건축년도", "계약기간", "계약구분", "갱신요구권사용", "종전계약보증금", "종전계약월세", "수집월", "구코드", "구명"',
    }
    
    if table_name not in query_columns:
        print(f"   ❌ [DB] '{table_name}'은 유효한 테이블 이름이 아닙니다.")
        return pd.DataFrame()

    query = f'SELECT {query_columns[table_name]} FROM {DB_SCHEMA}."{table_name}" WHERE "수집월" = \'{month}\''
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            db_df = pd.DataFrame(result.mappings().all())
        print(f"   ✅ [DB] {table_name} '{month}'월 데이터: {len(db_df):,}건")
        return db_df
    except ProgrammingError:
        print(f"   ℹ️ [DB] '{table_name}' 테이블이 없거나 '{month}'월 데이터가 없어 스킵합니다.")
    except Exception as e:
        print(f"   ❌ [DB] 데이터 조회 중 오류 발생: {e}")
    return pd.DataFrame()

def get_unique_data(api_df, db_df):
    """API 데이터와 DB 데이터를 비교하여 새로운 데이터만 반환합니다."""
    if api_df.empty and db_df.empty:
        return pd.DataFrame()
    
    # 두 데이터프레임의 컬럼 순서와 타입을 통일
    if not api_df.empty and not db_df.empty:
        db_df = db_df[api_df.columns]
        for col in api_df.columns:
            if api_df[col].dtype != db_df[col].dtype:
                db_df[col] = db_df[col].astype(api_df[col].dtype)

    combined_df = pd.concat([api_df, db_df]).convert_dtypes()
    unique_df = combined_df.drop_duplicates(keep=False)
    
    return unique_df

def collect_data(trade_type, months_list):
    """특정 거래 유형에 대해 지정된 기간 동안의 신규 데이터를 수집합니다."""
    table_map = {"매매": "real_estate_sales", "전월세": "real_estate_jeonse"}
    table_name = table_map.get(trade_type)
    
    if not table_name:
        print(f"   ❌ '{trade_type}'은 유효한 거래 유형이 아닙니다.")
        return pd.DataFrame()

    print(f"🔄 '{trade_type}' 데이터 수집 시작 (대상 월: {months_list})")

    all_api_dfs = []
    for month in months_list:
        for cortarNo, cortarName in seoul_gungu_list:
            sigungu_code = cortarNo[:5]
            api_df_part = fetch_api_data(trade_type, month, sigungu_code, cortarName)
            if not api_df_part.empty:
                all_api_dfs.append(api_df_part)
    
    if not all_api_dfs:
        print(f"📊 [API] '{trade_type}'에 대한 신규 데이터가 없습니다.")
        return pd.DataFrame()
        
    final_api_df = pd.concat(all_api_dfs, ignore_index=True)
    
    # DB 데이터는 최신 월 기준으로만 비교
    latest_month = months_list[-1]
    db_df = fetch_db_data(table_name, latest_month)
    
    unique_df = get_unique_data(final_api_df, db_df)
    print(f"✨ '{trade_type}' 신규 데이터 총 {len(unique_df)}건 발견")
    
    return unique_df
