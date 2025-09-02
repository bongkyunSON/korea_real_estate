from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from crud import user as crud_user
from schemas import user as schemas_user
from core.database import SessionLocal

# FastAPI의 의존성 주입 시스템을 사용하여 DB 세션을 가져오는 함수
async def get_db_session() -> AsyncSession:
    async with SessionLocal() as session:
        yield session

router = APIRouter()

@router.post("/users/", response_model=schemas_user.UserResponse, status_code=status.HTTP_201_CREATED)
async def create_new_user(user: schemas_user.UserCreate, db: AsyncSession = Depends(get_db_session)):
    """
    새로운 사용자를 생성합니다. (회원가입)
    """
    db_user = await crud.get_user_by_email(db, email=user.email)
    if db_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="이미 사용 중인 이메일입니다.",
        )
    created_user = await crud_user.create_user(db=db, user=user)
    return created_user