from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from models.user import User
from schemas.user import UserCreate
from core.security import get_password_hash, verify_password

async def get_user_by_email(db: AsyncSession, email: str) -> User | None:
    """이메일로 사용자를 조회합니다."""
    result = await db.execute(select(User).filter(User.email == email))
    return result.scalars().first()

async def create_user(db: AsyncSession, user: UserCreate) -> User:
    """새로운 사용자를 생성합니다."""
    hashed_password = get_password_hash(user.password)
    db_user = User(
        email=user.email,
        hashed_password=hashed_password
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user

async def authenticate_user(db: AsyncSession, email: str, password: str) -> User | None:
    """사용자 이메일과 비밀번호로 인증을 시도합니다."""
    user = await get_user_by_email(db, email=email)
    if not user:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user