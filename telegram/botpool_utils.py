from typing import Optional
import asyncio
import logging
import re
from urllib.parse import urlparse, parse_qs

from pyrogram import Client, errors
from pyrogram.raw import functions, types
from pyrogram.raw.types import ChannelParticipantsRecent, InputChannel, Message, User as RawUser


async def get_hash_via_discussion(app: Client, link: str) -> tuple[Optional[int], Optional[int]]:
    """
    Возвращает (user_id, access_hash) автора сообщения или комментария.
    Поддерживает:
      - https://t.me/<username>/<msg_id>
      - https://t.me/<username>/<post_id>?comment=<comment_id>
    """
    try:
        url = urlparse(link)
        parts = url.path.strip("/").split("/")
        if len(parts) < 2:
            return None, None

        username = parts[0]
        primary_id = int(parts[1])
        q = parse_qs(url.query)
        comment_id = int(q.get("comment", [0])[0]) if "comment" in q else None

        if comment_id:
            discussion_msg = await app.get_discussion_message(username, primary_id)
            if not discussion_msg or not discussion_msg.chat:
                return None, None

            discussion_peer = await app.resolve_peer(discussion_msg.chat.id)

            res = await app.invoke(
                functions.channels.GetMessages(
                    channel=discussion_peer,
                    id=[types.InputMessageID(id=comment_id)]
                )
            )
        else:
            chat_peer = await app.resolve_peer(username)
            res = await app.invoke(
                functions.channels.GetMessages(
                    channel=chat_peer,
                    id=[types.InputMessageID(id=primary_id)]
                )
            )

        if not getattr(res, "messages", None):
            return None, None

        msg = res.messages[0]
        if not isinstance(msg, types.Message):
            return None, None

        from_id = getattr(msg.from_id, "user_id", None)
        if not from_id:
            return None, None

        if getattr(res, "users", None):
            for u in res.users:
                if isinstance(u, types.User) and u.id == from_id and getattr(u, "access_hash", None):
                    return int(u.id), int(u.access_hash)

        try:
            peer = await app.resolve_peer(from_id)
            if isinstance(peer, types.InputPeerUser) and getattr(peer, "access_hash", None):
                return int(from_id), int(peer.access_hash)
        except Exception:
            pass

        return None, None

    except Exception:
        return None, None
    

async def get_access_hash_from_user_id(bot: Client, user_id: int) -> int | None:
    """
    Возвращает access_hash по user_id через resolve_peer
    """
    try:
        peer = await bot.resolve_peer(int(user_id))
        if isinstance(peer, types.InputPeerUser):
            return peer.access_hash
        
    except errors.FloodWait as e:
        logging.warning(f"[get_hash:user_id] FloodWait {e.value}s (uid={user_id})")
        await asyncio.sleep(e.value + 1)
        return await get_access_hash_from_user_id(bot, user_id)
    
    except Exception as e:
        logging.debug(f"[get_hash:user_id] uid={user_id} error={e}")
        return None
    return None
