from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from models import chat as models_chat
from models import user as models_user
from schemas import chat as schemas_chat

async def create_chat_room(db: AsyncSession, user: models_user.User, chat_room_create: schemas_chat.ChatRoomCreate) -> models_chat.ChatRoom:
    """특정 사용자를 위해 새로운 채팅방을 데이터베이스에 생성합니다."""
    db_chat_room = models_chat.ChatRoom(
        name=chat_room_create.name,
        user_id=user.id
    )
    db.add(db_chat_room)
    await db.commit()
    
    # 생성된 채팅방 객체를 다시 조회하면서 messages 관계를 즉시 로딩합니다.
    result = await db.execute(
        select(models_chat.ChatRoom)
        .options(selectinload(models_chat.ChatRoom.messages))
        .where(models_chat.ChatRoom.id == db_chat_room.id)
    )
    return result.scalars().one()

async def get_chat_rooms_by_user(db: AsyncSession, user_id: int) -> list[models_chat.ChatRoom]:
    """특정 사용자가 소유한 모든 채팅방 목록을 데이터베이스에서 조회합니다."""
    result = await db.execute(
        select(models_chat.ChatRoom)
        .options(selectinload(models_chat.ChatRoom.messages)) # 즉시 로딩 옵션 추가
        .where(models_chat.ChatRoom.user_id == user_id)
        .order_by(models_chat.ChatRoom.created_at.desc())
    )
    return result.scalars().all()

async def create_message_in_chatroom(db: AsyncSession, chat_room_id: int, message: schemas_chat.MessageCreate) -> models_chat.Message:
    """특정 채팅방에 새로운 메시지를 생성하고 데이터베이스에 저장합니다."""
    db_message = models_chat.Message(
        chat_room_id=chat_room_id,
        sender=message.sender,
        content=message.content
    )
    db.add(db_message)
    await db.commit()
    await db.refresh(db_message)
    return db_message