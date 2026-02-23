"""
XAI Router — WebSocket-based Explainable AI chatbot.

Endpoints:
  WS  /xai/ws/{business_id}            — Main chat WebSocket
  GET /xai/conversations/{business_id}  — List conversations for a user
  GET /xai/conversation/{conversation_id}/messages — Get messages for a conversation
  DELETE /xai/conversation/{conversation_id}       — Delete a conversation
"""

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query
from sqlalchemy import text
from database import get_db, get_db_connection
from services.session_service import session_service
from services.xai_service import XAIService
import json
import uuid
from datetime import datetime

router = APIRouter(
    prefix="/xai",
    tags=["xai"],
)

# Initialise once — constructor validates GEMINI_API_KEY and logs SDK info
xai_service = XAIService()

# ------------------------------------------------------------------
# Error code → (user-facing message, severity)
# ------------------------------------------------------------------
_ERROR_MESSAGES = {
    "quota_exceeded": (
        "The AI service rate limit has been reached. Please wait a moment and try again.",
        "warning",
    ),
    "auth_error": (
        "The AI service is not configured correctly (invalid API key). "
        "Please contact support.",
        "error",
    ),
    "gemini_error": (
        "The AI service encountered an error while processing your request. "
        "Please try again.",
        "error",
    ),
}


def _error_response(error_code: str) -> tuple[str, str]:
    """Return (user_message, severity) for a given error code."""
    return _ERROR_MESSAGES.get(
        error_code,
        ("An unexpected error occurred. Please try again.", "error"),
    )


# ------------------------------------------------------------------
# Helper: authenticate WebSocket via session cookie
# ------------------------------------------------------------------
def _authenticate_ws(websocket: WebSocket) -> dict | None:
    """Extract and validate session from WebSocket cookies."""
    cookies = websocket.cookies
    session_id = cookies.get("session_id")
    if not session_id:
        return None
    return session_service.get_session(session_id)


# ------------------------------------------------------------------
# Helper: get or create conversation
# ------------------------------------------------------------------
def _get_or_create_conversation(
    db, user_id: str, business_id: str, conversation_id: str = None
) -> str:
    """Return an existing conversation_id or create a new one."""
    if conversation_id:
        row = db.execute(
            text(
                "SELECT conversation_id FROM xai_conversations "
                "WHERE conversation_id = :cid AND user_id = :uid AND business_id = :bid"
            ),
            {"cid": conversation_id, "uid": user_id, "bid": business_id},
        ).fetchone()
        if row:
            return row[0]

    new_id = str(uuid.uuid4())
    db.execute(
        text(
            "INSERT INTO xai_conversations (conversation_id, user_id, business_id, title) "
            "VALUES (:cid, :uid, :bid, :title)"
        ),
        {"cid": new_id, "uid": user_id, "bid": business_id, "title": "New Chat"},
    )
    db.commit()
    return new_id


def _save_message(
    db,
    conversation_id: str,
    role: str,
    content: str,
    metadata: dict = None,
    severity: str = "info",
) -> str:
    """Persist a chat message to the database."""
    msg_id = str(uuid.uuid4())
    db.execute(
        text(
            "INSERT INTO xai_messages "
            "(message_id, conversation_id, role, content, metadata, severity) "
            "VALUES (:mid, :cid, :role, :content, :meta, :severity)"
        ),
        {
            "mid": msg_id,
            "cid": conversation_id,
            "role": role,
            "content": content,
            "meta": json.dumps(metadata or {}),
            "severity": severity,
        },
    )
    db.commit()
    return msg_id


def _update_conversation_title(db, conversation_id: str, title: str):
    db.execute(
        text("UPDATE xai_conversations SET title = :title WHERE conversation_id = :cid"),
        {"title": title[:500], "cid": conversation_id},
    )
    db.commit()


# ------------------------------------------------------------------
# WebSocket endpoint — Main chat
# ------------------------------------------------------------------
@router.websocket("/ws/{business_id}")
async def xai_websocket(websocket: WebSocket, business_id: str):
    """
    WebSocket chat endpoint.

    Client sends JSON:
        { "type": "query", "content": "...", "conversationId": "..." | null }

    Server sends JSON:
        { "type": "user_echo",    "messageId": "...", "content": "...", "conversationId": "...", "createdAt": "..." }
        { "type": "assistant",    "messageId": "...", "content": "...", "context": {}, "conversationId": "...", "createdAt": "..." }
        { "type": "notification", "content": "...", "severity": "info|error|warning", "conversationId": "..." }
        { "type": "conversation_created", "conversationId": "..." }
    """
    session = _authenticate_ws(websocket)
    if not session:
        await websocket.close(code=4001, reason="Unauthorized")
        return

    user_id = session.get("user_id")
    await websocket.accept()

    await websocket.send_json({
        "type": "notification",
        "content": "Connected to Pulse AI. Ask me anything about your analytics!",
        "severity": "info",
        "conversationId": None,
    })

    try:
        while True:
            raw = await websocket.receive_text()

            if raw == "ping":
                await websocket.send_text("pong")
                continue

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "notification",
                    "content": "Invalid message format.",
                    "severity": "error",
                    "conversationId": None,
                })
                continue

            msg_type = msg.get("type", "query")
            content = msg.get("content", "").strip()
            conversation_id = msg.get("conversationId")

            if msg_type != "query" or not content:
                continue

            db = get_db_connection()
            try:
                conversation_id = _get_or_create_conversation(
                    db, user_id, business_id, conversation_id
                )

                await websocket.send_json({
                    "type": "conversation_created",
                    "conversationId": conversation_id,
                })

                now = datetime.utcnow().isoformat()
                user_msg_id = _save_message(db, conversation_id, "user", content)

                # Update title on first user message
                msg_count = db.execute(
                    text(
                        "SELECT COUNT(*) FROM xai_messages "
                        "WHERE conversation_id = :cid AND role = 'user'"
                    ),
                    {"cid": conversation_id},
                ).scalar()
                if msg_count <= 1:
                    title = content[:100] + ("..." if len(content) > 100 else "")
                    _update_conversation_title(db, conversation_id, title)

                # Echo user message back to client immediately
                await websocket.send_json({
                    "type": "user_echo",
                    "messageId": user_msg_id,
                    "content": content,
                    "conversationId": conversation_id,
                    "createdAt": now,
                })

                # ── AI processing ──────────────────────────────────
                try:
                    result = await xai_service.process_query(content, business_id)

                    if result["error"]:
                        error_msg, severity = _error_response(result["error"])
                        _save_message(
                            db, conversation_id, "notification", error_msg, severity=severity
                        )
                        await websocket.send_json({
                            "type": "notification",
                            "content": error_msg,
                            "severity": severity,
                            "conversationId": conversation_id,
                        })
                    else:
                        assistant_msg_id = _save_message(
                            db,
                            conversation_id,
                            "assistant",
                            result["answer"],
                            metadata={"context": result["context"]},
                        )
                        await websocket.send_json({
                            "type": "assistant",
                            "messageId": assistant_msg_id,
                            "content": result["answer"],
                            "context": result["context"],
                            "conversationId": conversation_id,
                            "createdAt": datetime.utcnow().isoformat(),
                        })

                except Exception as e:
                    print(f"[XAI] Unhandled query processing error: {type(e).__name__}: {e}")
                    error_text = "An unexpected error occurred while processing your query."
                    _save_message(db, conversation_id, "notification", error_text, severity="error")
                    await websocket.send_json({
                        "type": "notification",
                        "content": error_text,
                        "severity": "error",
                        "conversationId": conversation_id,
                    })

            finally:
                db.close()

    except WebSocketDisconnect:
        print(f"[XAI] WebSocket disconnected — user={user_id}, business={business_id}")
    except Exception as e:
        print(f"[XAI] Unexpected WebSocket error: {type(e).__name__}: {e}")
        try:
            await websocket.close(code=1011)
        except Exception:
            pass


# ------------------------------------------------------------------
# REST endpoints — Chat history
# ------------------------------------------------------------------
@router.get("/conversations/{business_id}")
async def get_conversations(
    business_id: str, userId: str = Query(...), db=Depends(get_db)
):
    """List all conversations for a user + business, newest first."""
    rows = db.execute(
        text(
            "SELECT conversation_id, title, created_at, updated_at "
            "FROM xai_conversations "
            "WHERE user_id = :uid AND business_id = :bid "
            "ORDER BY updated_at DESC"
        ),
        {"uid": userId, "bid": business_id},
    ).fetchall()

    return {
        "conversations": [
            {
                "conversationId": r[0],
                "title": r[1],
                "createdAt": r[2].isoformat() if r[2] else None,
                "updatedAt": r[3].isoformat() if r[3] else None,
            }
            for r in rows
        ]
    }


@router.get("/conversation/{conversation_id}/messages")
async def get_messages(
    conversation_id: str, userId: str = Query(...), db=Depends(get_db)
):
    """Get all messages for a conversation."""
    conv = db.execute(
        text("SELECT user_id FROM xai_conversations WHERE conversation_id = :cid"),
        {"cid": conversation_id},
    ).fetchone()
    if not conv or conv[0] != userId:
        raise HTTPException(status_code=404, detail="Conversation not found")

    rows = db.execute(
        text(
            "SELECT message_id, role, content, metadata, severity, created_at "
            "FROM xai_messages "
            "WHERE conversation_id = :cid "
            "ORDER BY created_at ASC"
        ),
        {"cid": conversation_id},
    ).fetchall()

    return {
        "messages": [
            {
                "messageId": r[0],
                "role": r[1],
                "content": r[2],
                "metadata": json.loads(r[3]) if r[3] else {},
                "severity": r[4],
                "createdAt": r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ]
    }


@router.delete("/conversation/{conversation_id}")
async def delete_conversation(
    conversation_id: str, userId: str = Query(...), db=Depends(get_db)
):
    """Delete a conversation and all its messages."""
    conv = db.execute(
        text("SELECT user_id FROM xai_conversations WHERE conversation_id = :cid"),
        {"cid": conversation_id},
    ).fetchone()
    if not conv or conv[0] != userId:
        raise HTTPException(status_code=404, detail="Conversation not found")

    db.execute(
        text("DELETE FROM xai_conversations WHERE conversation_id = :cid"),
        {"cid": conversation_id},
    )
    db.commit()
    return {"status": "deleted"}