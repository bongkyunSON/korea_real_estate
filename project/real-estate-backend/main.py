import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# models.user에서 User 모델을 임포트합니다.
from models.user import User
from models.chat import ChatRoom, Message
from core.database import engine, Base
from routers import user as user_router
from routers import chat as chat_router

# 데이터베이스 테이블 생성
async def create_db_and_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

app = FastAPI()

# 애플리케이션 시작 시 데이터베이스 테이블 생성
@app.on_event("startup")
async def on_startup():
    await create_db_and_tables()

# CORS 미들웨어 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 실제 프로덕션에서는 특정 도메인만 허용하세요.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 기본 라우트
@app.get("/")
def read_root():
    return {"message": "Welcome to the Real Estate AI Assistant API"}

@app.get("/health")
def health_check():
    return {"status": "ok"}

# 사용자 관련 라우터 등록
app.include_router(user_router.router, prefix="/api", tags=["Users"])

# 채팅 관련 라우터 등록
app.include_router(chat_router.router, prefix="/api/chat-rooms", tags=["Chat"]) 

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)