# core와 models 임포트 경로 설정을 위해 sys.path에 현재 경로 추가
import sys
import os
import uvicorn
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from core.database import SessionLocal, engine, Base
from models.user import User
from routers.user import router as user_router

app = FastAPI(
    title="Real Estate AI Project API",
    description="부동산 AI 프로젝트를 위한 API 문서입니다.",
    version="0.1.0"
)

# 라우터 등록
app.include_router(user_router, prefix="/api/v1", tags=["Users"])

# 데이터베이스 세션을 API 요청/응답 주기와 맞춰 관리하는 의존성 함수
async def get_db_session() -> AsyncSession:
    async with SessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

@app.on_event("startup")
async def startup_event():
    """애플리케이션 시작 시 데이터베이스 테이블 생성"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@app.get("/", tags=["Default"])
async def health_check(session: AsyncSession = Depends(get_db_session)):
    """DB 연결 상태를 확인하는 헬스 체크 엔드포인트"""
    try:
        result = await session.execute(text("SELECT 1"))
        if result.scalar_one() == 1:
            return {"status": "ok", "db_connection": "healthy"}
        else:
            return {"status": "ok", "db_connection": "degraded"}
    except Exception as e:
        return {"status": "error", "db_connection": "unhealthy", "error": str(e)}

# uvicorn 서버를 직접 실행하기 위한 코드
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)