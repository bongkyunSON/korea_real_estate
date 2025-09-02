from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from models.user import User
from schemas.user import UserCreate
from core.security import get_password_hash

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