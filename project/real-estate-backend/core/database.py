from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from .config import DATABASE_URL

# 비동기 데이터베이스 엔진 생성
# create_async_engine 함수는 데이터베이스와의 비동기 연결을 관리합니다.
engine = create_async_engine(DATABASE_URL)

# 비동기 세션을 생성하는 팩토리
# async_sessionmaker는 데이터베이스 작업을 위한 세션을 생성합니다.
# expire_on_commit=False 옵션은 커밋 후에도 객체를 계속 사용할 수 있게 합니다.
SessionLocal = async_sessionmaker(autocommit=False, autoflush=False, bind=engine, expire_on_commit=False)

# 데이터베이스 모델의 기본 클래스
# 이 클래스를 상속받아 모든 데이터베이스 모델(테이블)을 정의하게 됩니다.
class Base(DeclarativeBase):
    pass
