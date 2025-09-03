from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm 
from sqlalchemy.ext.asyncio import AsyncSession

from crud import user as crud_user
from schemas import user as schemas_user
from core.database import SessionLocal
from core import security
from models import user as models_user

router = APIRouter()

# --- 의존성 함수들 ---

async def get_db_session() -> AsyncSession:
    """FastAPI의 의존성 주입 시스템을 사용하여 DB 세션을 가져옵니다."""
    async with SessionLocal() as session:
        yield session

async def get_current_user(
    token: str = Depends(security.oauth2_scheme), db: AsyncSession = Depends(get_db_session)
) -> models_user.User:
    """토큰을 검증하고 현재 로그인된 사용자 정보를 반환합니다."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    token_data = security.verify_token(token, credentials_exception)
    user = await crud_user.get_user_by_email(db, email=token_data.email)
    if user is None:
        raise credentials_exception
    return user

# --- API 엔드포인트들 ---

@router.post("/users/", response_model=schemas_user.UserResponse, status_code=status.HTTP_201_CREATED)
async def create_new_user(user: schemas_user.UserCreate, db: AsyncSession = Depends(get_db_session)):
    """
    새로운 사용자를 생성합니다. (회원가입)
    """
    db_user = await crud_user.get_user_by_email(db, email=user.email)
    if db_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="이미 사용 중인 이메일입니다.",
        )
    created_user = await crud_user.create_user(db=db, user=user)
    return created_user


@router.post("/login", response_model=schemas_user.Token)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(), # 이 부분을 수정
    db: AsyncSession = Depends(get_db_session)
):
    """
    사용자 로그인 및 Access Token 발급
    """
    user = await crud_user.authenticate_user(
        db, email=form_data.username, password=form_data.password
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="이메일 또는 비밀번호가 잘못되었습니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = security.create_access_token(
        data={"sub": user.email}
    )
    return {"access_token": access_token, "token_type": "bearer"}



@router.get("/users/me", response_model=schemas_user.UserResponse)
async def read_users_me(current_user: models_user.User = Depends(get_current_user)):
    """
    현재 로그인된 사용자의 정보를 반환합니다.
    """
    return current_user

