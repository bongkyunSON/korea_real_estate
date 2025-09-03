from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from crud import chat as crud_chat
from schemas import chat as schemas_chat
from models import user as models_user
from routers.user import get_current_user, get_db_session

router = APIRouter()

@router.post("/", response_model=schemas_chat.ChatRoom, status_code=status.HTTP_201_CREATED)
async def create_new_chat_room(
    chat_room_in: schemas_chat.ChatRoomCreate,
    db: AsyncSession = Depends(get_db_session),
    current_user: models_user.User = Depends(get_current_user)
):
    """
    현재 로그인된 사용자를 위해 새로운 채팅방을 생성합니다.
    """
    return await crud_chat.create_chat_room(db=db, user=current_user, chat_room_create=chat_room_in)

@router.get("/", response_model=List[schemas_chat.ChatRoom])
async def read_user_chat_rooms(
    db: AsyncSession = Depends(get_db_session),
    current_user: models_user.User = Depends(get_current_user)
):
    """
    현재 로그인된 사용자의 모든 채팅방 목록을 조회합니다.
    """
    return await crud_chat.get_chat_rooms_by_user(db=db, user_id=current_user.id)

@router.post("/{chat_room_id}/messages/", response_model=schemas_chat.Message)
async def create_new_message_in_chatroom(
    chat_room_id: int,
    message_in: schemas_chat.MessageCreate,
    db: AsyncSession = Depends(get_db_session),
    current_user: models_user.User = Depends(get_current_user)
):
    """
    특정 채팅방에 새로운 메시지를 생성합니다.
    (추가 로직 필요: 해당 채팅방의 소유자가 맞는지 확인)
    """
    # TODO: 사용자가 이 채팅방의 소유자인지 확인하는 로직 추가 필요
    return await crud_chat.create_message_in_chatroom(db=db, chat_room_id=chat_room_id, message=message_in)