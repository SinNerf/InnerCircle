import json
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func as sa_func
from sqlmodel import Session, select

from app.auth import (
    create_access_token,
    get_current_admin,
    get_current_architect,
    get_current_user,
    hash_password,
    verify_password,
)
from app.models import (
    AdminAction, Badge, Book, BookPage, BookTag, Comment, Corner, Follow, Notification, Post, PostTag, PostVote,
    PrivilegeLog, Suggestion, SuggestionTag, Tag, TagCornerAccess, Title, User, UserBadge, Vote, UserCornerSubscription,
    create_db_and_tables, engine, get_session,
)
from app.ranks import RANKS, get_rank_name, has_privilege, user_rank

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.globals["get_rank_name"] = get_rank_name
templates.env.globals["has_privilege"] = has_privilege
templates.env.globals["user_rank"] = user_rank
templates.env.globals["RANKS"] = RANKS

COOKIE = dict(key="access_token", httponly=True, samesite="none", secure=True)
RESERVED_SLUGS = {"library", "suggestions"}

BUILTIN_CORNERS = [
    {"slug": "library", "name": "Library", "icon": "\U0001f4da",
     "description": "Read and publish books of knowledge.", "template_type": "library"},
    {"slug": "suggestions", "name": "Suggestions", "icon": "\U0001f4ac",
     "description": "Suggest changes and features. Vote on what matters.", "template_type": "suggestions"},
]

BUILTIN_TAGS = [
    {"slug": "illegal", "name": "Illegal", "tag_type": "rank_gated", "min_rank": 5, "required_badge_id": None},
    {"slug": "18plus", "name": "18+", "tag_type": "badge_gated", "min_rank": 1, "required_badge_id": None},
]


@asynccontextmanager
async def lifespan(_app: FastAPI):
    create_db_and_tables()
    from sqlalchemy import func as sa_func, text as sa_text
    with engine.connect() as conn:
        try:
            conn.execute(sa_text("ALTER TABLE title ADD COLUMN description VARCHAR(300) DEFAULT ''"))
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute(sa_text("ALTER TABLE user ADD COLUMN ban_reason VARCHAR(500) DEFAULT ''"))
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute(sa_text("ALTER TABLE corner ADD COLUMN is_external BOOLEAN DEFAULT 0"))
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute(sa_text("ALTER TABLE corner ADD COLUMN is_hidden BOOLEAN DEFAULT 0"))
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute(sa_text("ALTER TABLE user ADD COLUMN is_deleted BOOLEAN DEFAULT 0"))
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute(sa_text("ALTER TABLE user ADD COLUMN deleted_reason VARCHAR(500) DEFAULT ''"))
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute(sa_text("ALTER TABLE user ADD COLUMN deleted_at DATETIME DEFAULT NULL"))
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute(sa_text("ALTER TABLE badge ADD COLUMN can_view_18plus BOOLEAN DEFAULT 0"))
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute(sa_text("ALTER TABLE book ADD COLUMN updated_at DATETIME"))
            conn.execute(sa_text("UPDATE book SET updated_at = created_at WHERE updated_at IS NULL"))
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute(sa_text("ALTER TABLE tag ADD COLUMN required_badge_id INTEGER REFERENCES badge(id)"))
            conn.commit()
        except Exception:
            pass
    with Session(engine) as s:
        for c in BUILTIN_CORNERS:
            if not s.exec(select(Corner).where(Corner.slug == c["slug"])).first():
                s.add(Corner(**c))
        for t in BUILTIN_TAGS:
            if not s.exec(select(Tag).where(Tag.slug == t["slug"])).first():
                s.add(Tag(**t))
        for t in s.exec(select(Tag)).all():
            if t.tag_type == "illegal":
                t.tag_type = "rank_gated"
                s.add(t)
            elif t.tag_type == "18plus":
                t.tag_type = "badge_gated"
                s.add(t)
        first_18_badge = s.exec(select(Badge).where(Badge.can_view_18plus == True)).first()
        for t in s.exec(select(Tag).where(Tag.tag_type == "badge_gated")).all():
            if t.required_badge_id is None and first_18_badge and first_18_badge.id is not None:
                t.required_badge_id = first_18_badge.id
                s.add(t)
        has_architect = s.exec(select(User).where(User.rank >= 12)).first()
        if not has_architect:
            nyx = s.exec(select(User).where(sa_func.lower(User.username) == "nyx")).first()
            if nyx:
                nyx.rank = 12
                nyx.is_active = True
                nyx.is_banned = False
                nyx.is_deleted = False
                s.add(nyx)
        s.commit()
    yield


app = FastAPI(title="InnerCircle", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)


def _tpl(name, request, session=None, user=None, status_code=200, **ctx):
    uc = 0
    if user and session:
        uc = len(session.exec(select(Notification).where(
            Notification.user_id == user.id, Notification.is_read == False
        )).all())
        if "user_badge_ids" not in ctx:
            ctx["user_badge_ids"] = {
                bid
                for bid in session.exec(
                    select(UserBadge.badge_id).where(UserBadge.user_id == user.id)  # type: ignore[arg-type]
                ).all()
                if bid is not None
            }
        if "can_view_18plus" not in ctx:
            ctx["can_view_18plus"] = _user_can_view_18plus(session, user)
    if "user_badge_ids" not in ctx:
        ctx["user_badge_ids"] = set()
    if "can_view_18plus" not in ctx:
        ctx["can_view_18plus"] = False
    resp = templates.TemplateResponse(name, {"request": request, "user": user, "unread_count": uc, **ctx})
    if status_code != 200:
        resp.status_code = status_code
    return resp


def _suggestion_stats(session: Session, user_id: int, viewer: User | None = None) -> dict:
    subs = session.exec(select(Suggestion).where(Suggestion.author_id == user_id)).all()
    if viewer is None:
        s_ids = [s.id for s in subs if s.id is not None]
        up = down = 0
        if s_ids:
            votes = session.exec(select(Vote).where(Vote.suggestion_id.in_(s_ids))).all()  # type: ignore[attr-defined]
            up = sum(1 for v in votes if v.value == 1)
            down = sum(1 for v in votes if v.value == -1)
        approved = sum(1 for s in subs if s.is_approved)
        return {"suggestions": subs, "s_count": len(subs), "s_up": up, "s_down": down, "s_approved": approved}

    suggestion_ids = [s.id for s in subs if s.id is not None]
    tags_by_s = _load_tags_for_suggestions(session, suggestion_ids)
    visible_subs = [
        s for s in subs
        if s.id is not None and _tags_visible_for_user(session, viewer, tags_by_s.get(s.id, []))
    ]
    s_ids = [s.id for s in visible_subs if s.id is not None]
    up = down = 0
    if s_ids:
        votes = session.exec(select(Vote).where(Vote.suggestion_id.in_(s_ids))).all()  # type: ignore[attr-defined]
        up = sum(1 for v in votes if v.value == 1)
        down = sum(1 for v in votes if v.value == -1)
    approved = sum(1 for s in visible_subs if s.is_approved)
    return {"suggestions": visible_subs, "s_count": len(visible_subs), "s_up": up, "s_down": down, "s_approved": approved}


def _corner_stats(session: Session, user_id: int) -> list[dict]:
    corners = session.exec(select(Corner).where(
        Corner.slug.notin_(["library", "suggestions"]),  # type: ignore[attr-defined]
        Corner.is_external == False,
    ).order_by(Corner.created_at)).all()
    stats = []
    for corner in corners:
        posts = session.exec(select(Post).where(
            Post.corner_id == corner.id, Post.author_id == user_id,
        )).all()
        if not posts:
            continue
        p_ids = [p.id for p in posts]
        entry: dict = {"corner": corner, "count": len(posts)}
        if corner.template_type == "library":
            entry["views"] = sum(p.views for p in posts)
        else:
            votes = session.exec(select(PostVote).where(
                PostVote.post_id.in_(p_ids)  # type: ignore[attr-defined]
            )).all() if p_ids else []
            entry["up"] = sum(1 for v in votes if v.value == 1)
            entry["down"] = sum(1 for v in votes if v.value == -1)
            entry["approved"] = sum(1 for p in posts if p.is_approved)
        stats.append(entry)
    return stats


def _user_can_view_18plus(session: Session, user: User) -> bool:
    user_badge_ids = {
        bid
        for bid in session.exec(
            select(UserBadge.badge_id).where(UserBadge.user_id == user.id)  # type: ignore[arg-type]
        ).all()
        if bid is not None
    }
    allowed_badge_ids = {
        bid
        for bid in session.exec(select(Badge.id).where(Badge.can_view_18plus == True)).all()  # type: ignore[arg-type]
        if bid is not None
    }
    return bool(user_badge_ids & allowed_badge_ids)


def _tag_visible_for_user(session: Session, user: User, tag: Tag) -> bool:
    if has_privilege(user, "admin"):
        return True
    tt = tag.tag_type
    if tt == "normal":
        return True
    if tt in ("illegal", "rank_gated"):
        return user_rank(user) >= int(tag.min_rank or 1)
    if tt == "18plus":
        return _user_can_view_18plus(session, user)
    if tt == "badge_gated":
        if tag.required_badge_id is None:
            return False
        return (
            session.exec(
                select(UserBadge).where(
                    UserBadge.user_id == user.id,
                    UserBadge.badge_id == tag.required_badge_id,
                )
            ).first()
            is not None
        )
    return True


def _tags_visible_for_user(session: Session, user: User, tags: list[Tag]) -> bool:
    return all(_tag_visible_for_user(session, user, t) for t in tags)


def _filter_tags_visible_for_user(session: Session, user: User, tags: list[Tag]) -> list[Tag]:
    return [t for t in tags if _tag_visible_for_user(session, user, t)]


def _sanitize_tag_query_for_user(
    session: Session, user: User, tag: str | None, corner_tags: list[Tag]
) -> str | None:
    """Drop tag filter if it names a corner tag the user is not allowed to view."""
    if not tag or not str(tag).strip():
        return None
    raw = str(tag).strip()
    for t in corner_tags:
        if t.slug == raw or t.name == raw:
            if not _tag_visible_for_user(session, user, t):
                return None
            break
    return raw


def _tag_selectable_for_user(session: Session, user: User, tag: Tag, is_admin: bool) -> bool:
    if is_admin:
        return True
    tt = tag.tag_type
    if tt == "normal":
        return True
    if tt in ("illegal", "rank_gated"):
        return user_rank(user) >= int(tag.min_rank or 1)
    if tt == "18plus":
        return _user_can_view_18plus(session, user)
    if tt == "badge_gated":
        if tag.required_badge_id is None:
            return False
        return (
            session.exec(
                select(UserBadge).where(
                    UserBadge.user_id == user.id,
                    UserBadge.badge_id == tag.required_badge_id,
                )
            ).first()
            is not None
        )
    return True


def _load_tags_for_posts(session: Session, post_ids: list[int]) -> dict[int, list[Tag]]:
    if not post_ids:
        return {}
    rows = session.exec(select(PostTag).where(PostTag.post_id.in_(post_ids))).all()  # type: ignore[attr-defined]
    tag_ids = {r.tag_id for r in rows if r.tag_id is not None}
    tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all() if tag_ids else []
    tag_map = {t.id: t for t in tags if t.id is not None}
    by_post: dict[int, list[Tag]] = {pid: [] for pid in post_ids}
    for r in rows:
        if r.post_id is not None and r.tag_id in tag_map:
            by_post[r.post_id].append(tag_map[r.tag_id])
    return by_post


def _load_tags_for_books(session: Session, book_ids: list[int]) -> dict[int, list[Tag]]:
    if not book_ids:
        return {}
    rows = session.exec(select(BookTag).where(BookTag.book_id.in_(book_ids))).all()  # type: ignore[attr-defined]
    tag_ids = {r.tag_id for r in rows if r.tag_id is not None}
    tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all() if tag_ids else []
    tag_map = {t.id: t for t in tags if t.id is not None}
    by_book: dict[int, list[Tag]] = {bid: [] for bid in book_ids}
    for r in rows:
        if r.book_id is not None and r.tag_id in tag_map:
            by_book[r.book_id].append(tag_map[r.tag_id])
    return by_book


def _load_tags_for_suggestions(session: Session, suggestion_ids: list[int]) -> dict[int, list[Tag]]:
    if not suggestion_ids:
        return {}
    rows = session.exec(select(SuggestionTag).where(SuggestionTag.suggestion_id.in_(suggestion_ids))).all()  # type: ignore[attr-defined]
    tag_ids = {r.tag_id for r in rows if r.tag_id is not None}
    tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all() if tag_ids else []
    tag_map = {t.id: t for t in tags if t.id is not None}
    by_s: dict[int, list[Tag]] = {sid: [] for sid in suggestion_ids}
    for r in rows:
        if r.suggestion_id is not None and r.tag_id in tag_map:
            by_s[r.suggestion_id].append(tag_map[r.tag_id])
    return by_s


def _tag_allowed_in_corner(session: Session, tag_id: int, corner_id: int | None) -> bool:
    if corner_id is None:
        return True
    accesses = session.exec(select(TagCornerAccess).where(TagCornerAccess.tag_id == tag_id)).all()
    if not accesses:
        return True
    return any(a.corner_id == corner_id for a in accesses)


def _available_tags_for_corner(session: Session, corner_id: int | None) -> list[Tag]:
    tags = session.exec(select(Tag).order_by(Tag.created_at)).all()
    return [t for t in tags if t.id is not None and _tag_allowed_in_corner(session, t.id, corner_id)]


def _user_has_any_privilege(user: User) -> bool:
    return (
        has_privilege(user, "comment")
        or has_privilege(user, "reply")
        or has_privilege(user, "admin")
        or has_privilege(user, "architect")
    )


def _log_privilege_use(
    session: Session,
    actor: User,
    privilege: str,
    action: str,
    location: str = "",
    details: str = "",
):
    session.add(PrivilegeLog(
        actor_id=actor.id,
        privilege=privilege,
        action=action[:120],
        location=location[:300],
        details=details[:1000],
    ))


def _get_comments(session: Session, target_type: str, target_id: int):
    raw = session.exec(select(Comment).where(
        Comment.target_type == target_type, Comment.target_id == target_id,
        Comment.parent_id == None,
    ).order_by(Comment.created_at)).all()
    cache: dict[int, User] = {}
    result = []
    for c in raw:
        if c.author_id not in cache:
            cache[c.author_id] = session.get(User, c.author_id)
        replies_raw = session.exec(
            select(Comment).where(Comment.parent_id == c.id).order_by(Comment.created_at)
        ).all()
        replies = []
        for r in replies_raw:
            if r.author_id not in cache:
                cache[r.author_id] = session.get(User, r.author_id)
            replies.append({"c": r, "author": cache[r.author_id]})
        result.append({"c": c, "author": cache[c.author_id], "replies": replies})
    return result


def _can_comment(session: Session, user, target_type: str, target_id: int) -> bool:
    if not has_privilege(user, "comment"):
        return False
    if has_privilege(user, "unlimited_comments"):
        return True
    return session.exec(select(Comment).where(
        Comment.author_id == user.id, Comment.target_type == target_type,
        Comment.target_id == target_id, Comment.parent_id == None,
    )).first() is None


def _exec_approve_user(session, p):
    u = session.get(User, p["user_id"])
    if u:
        u.is_active = True
        session.add(u)
        if p.get("title"):
            session.add(Title(user_id=u.id, text=p["title"]))


def _exec_decline_user(session, p):
    u = session.get(User, p["user_id"])
    if u:
        session.delete(u)


def _exec_ban_user(session, p):
    u = session.get(User, p["user_id"])
    if u:
        u.is_banned = True
        u.ban_reason = p.get("reason", "") or ""
        session.add(u)


def _exec_unban_user(session, p):
    u = session.get(User, p["user_id"])
    if u:
        u.is_banned = False
        u.ban_reason = ""
        session.add(u)


def _exec_delete_user(session, p):
    u = session.get(User, p["user_id"])
    if not u:
        return
    for sub in session.exec(select(UserCornerSubscription).where(
        UserCornerSubscription.user_id == u.id
    )).all():
        session.delete(sub)

    u.is_deleted = True
    u.is_active = False
    u.is_banned = False
    u.ban_reason = ""
    u.deleted_reason = p.get("reason", "") or ""
    u.deleted_at = datetime.now(timezone.utc)
    session.add(u)


def _exec_delete_corner(session, p):
    corner = session.get(Corner, p["corner_id"])
    if not corner:
        return

    for sub in session.exec(select(UserCornerSubscription).where(
        UserCornerSubscription.corner_id == corner.id
    )).all():
        session.delete(sub)

    if corner.slug == "library":
        for b in session.exec(select(Book)).all():
            _exec_delete_book(session, {"book_id": b.id})
    elif corner.slug == "suggestions":
        for s in session.exec(select(Suggestion)).all():
            _exec_delete_suggestion(session, {"suggestion_id": s.id})
    else:
        for post in session.exec(select(Post).where(Post.corner_id == corner.id)).all():
            _exec_delete_post(session, {"post_id": post.id})

    session.delete(corner)


def _exec_approve_suggestion(session, p):
    s = session.get(Suggestion, p["suggestion_id"])
    if s:
        s.is_approved = True
        session.add(s)


def _exec_delete_book(session, p):
    b = session.get(Book, p["book_id"])
    if b:
        for row in session.exec(select(BookTag).where(BookTag.book_id == b.id)).all():
            session.delete(row)
        for row in session.exec(select(BookPage).where(BookPage.book_id == b.id)).all():
            session.delete(row)
        for c in session.exec(select(Comment).where(Comment.target_type == "book", Comment.target_id == b.id)).all():
            for r in session.exec(select(Comment).where(Comment.parent_id == c.id)).all():
                session.delete(r)
            session.delete(c)
        session.delete(b)


def _split_book_pages(raw: str) -> list[str]:
    text = (raw or "").strip()
    if not text:
        return []
    parts = re.split(r"\r?\n\s*---\s*page\s*---\s*\r?\n", text, flags=re.IGNORECASE)
    chunks = [p.strip() for p in parts if p.strip()]
    return chunks if chunks else [text]


def _library_tags_for_write(session: Session) -> list[Tag]:
    library_corner = session.exec(select(Corner).where(Corner.slug == "library")).first()
    return _available_tags_for_corner(session, library_corner.id if library_corner else None)


def _book_editor_content(session: Session, book: Book) -> str:
    rows = session.exec(select(BookPage).where(BookPage.book_id == book.id).order_by(BookPage.page_number)).all()
    if rows:
        return "\n---page---\n".join(r.content for r in rows)
    return book.content or ""


def _exec_delete_suggestion(session, p):
    s = session.get(Suggestion, p["suggestion_id"])
    if s:
        for v in session.exec(select(Vote).where(Vote.suggestion_id == s.id)).all():
            session.delete(v)
        for c in session.exec(select(Comment).where(Comment.target_type == "suggestion", Comment.target_id == s.id)).all():
            for r in session.exec(select(Comment).where(Comment.parent_id == c.id)).all():
                session.delete(r)
            session.delete(c)
        session.delete(s)


def _exec_add_title(session, p):
    session.add(Title(user_id=p["user_id"], text=p["text"], description=p.get("description", "")))


def _exec_remove_title(session, p):
    t = session.get(Title, p["title_id"])
    if t:
        session.delete(t)


def _exec_assign_badge(session, p):
    existing = session.exec(select(UserBadge).where(
        UserBadge.user_id == p["user_id"], UserBadge.badge_id == p["badge_id"]
    )).first()
    if not existing:
        session.add(UserBadge(user_id=p["user_id"], badge_id=p["badge_id"], assigned_by=p["assigned_by"]))


def _exec_remove_badge(session, p):
    existing = session.exec(select(UserBadge).where(
        UserBadge.user_id == p["user_id"], UserBadge.badge_id == p["badge_id"]
    )).first()
    if existing:
        session.delete(existing)


def _exec_set_rank(session, p):
    u = session.get(User, p["user_id"])
    if u:
        u.rank = p["rank"]
        session.add(u)


def _exec_send_notification(session, p):
    session.add(Notification(user_id=p["user_id"], message=p["message"]))


def _notify_followers(session: Session, author_id: int, message: str):
    followers = session.exec(select(Follow).where(Follow.following_id == author_id)).all()
    for f in followers:
        session.add(Notification(user_id=f.follower_id, message=message))


def _exec_delete_comment(session, p):
    c = session.get(Comment, p["comment_id"])
    if c:
        for r in session.exec(select(Comment).where(Comment.parent_id == c.id)).all():
            session.delete(r)
        session.delete(c)


def _exec_create_corner(session, p):
    session.add(Corner(
        name=p["name"], slug=p["slug"], icon=p.get("icon", ""),
        description=p.get("description", ""), template_type=p["template_type"],
        is_external=bool(p.get("is_external", False)),
    ))


def _exec_delete_post(session, p):
    post = session.get(Post, p["post_id"])
    if post:
        for v in session.exec(select(PostVote).where(PostVote.post_id == post.id)).all():
            session.delete(v)
        for c in session.exec(select(Comment).where(Comment.target_type == "post", Comment.target_id == post.id)).all():
            for r in session.exec(select(Comment).where(Comment.parent_id == c.id)).all():
                session.delete(r)
            session.delete(c)
        session.delete(post)


def _exec_approve_post(session, p):
    post = session.get(Post, p["post_id"])
    if post:
        post.is_approved = True
        session.add(post)


def _ensure_architect_exists(session: Session):
    has_architect = session.exec(select(User).where(User.rank >= 12, User.is_deleted == False)).first()
    if has_architect:
        return
    nyx = session.exec(select(User).where(sa_func.lower(User.username) == "nyx")).first()
    if nyx:
        nyx.rank = 12
        nyx.is_active = True
        nyx.is_banned = False
        nyx.is_deleted = False
        session.add(nyx)


ACTION_DISPATCH = {
    "approve_user": _exec_approve_user, "decline_user": _exec_decline_user,
    "ban_user": _exec_ban_user, "unban_user": _exec_unban_user,
    "delete_user": _exec_delete_user,
    "approve_suggestion": _exec_approve_suggestion,
    "delete_book": _exec_delete_book, "delete_suggestion": _exec_delete_suggestion,
    "add_title": _exec_add_title, "remove_title": _exec_remove_title, "set_rank": _exec_set_rank,
    "send_notification": _exec_send_notification, "delete_comment": _exec_delete_comment,
    "create_corner": _exec_create_corner,
    "delete_corner": _exec_delete_corner,
    "delete_post": _exec_delete_post, "approve_post": _exec_approve_post,
    "assign_badge": _exec_assign_badge, "remove_badge": _exec_remove_badge,
}


def _action_summary(action_type: str, payload: dict, session: Session | None = None) -> str:
    def _user_name(uid):
        if session is None or uid is None:
            return f"#{uid}"
        u = session.get(User, uid)
        return u.username if u else f"#{uid}"

    def _badge_name(bid):
        if session is None or bid is None:
            return f"#{bid}"
        b = session.get(Badge, bid)
        return b.name if b else f"#{bid}"

    def _safe_quote(s: str, max_len: int = 120) -> str:
        t = (s or "").replace('"', "'").strip()
        return t if len(t) <= max_len else t[: max_len - 1] + "…"

    def _remove_title_line(tid):
        if session is None or tid is None:
            return f"Remove title #{tid}"
        t = session.get(Title, tid)
        if not t:
            return f"Remove title #{tid}"
        return f'Remove title "{_safe_quote(t.text)}" from {_user_name(t.user_id)}'

    def _delete_book_line(bid):
        if session is None or bid is None:
            return f"Delete book #{bid}"
        b = session.get(Book, bid)
        if not b:
            return f"Delete book #{bid}"
        return f'Delete book "{_safe_quote(b.title, 100)}" by {_user_name(b.author_id)}'

    fns = {
        "approve_user": lambda p: f"Approve {_user_name(p.get('user_id'))}",
        "decline_user": lambda p: f"Decline {_user_name(p.get('user_id'))}",
        "ban_user": lambda p: f"Ban {_user_name(p.get('user_id'))}",
        "unban_user": lambda p: f"Unban {_user_name(p.get('user_id'))}",
        "delete_user": lambda p: f"Delete {_user_name(p.get('user_id'))}",
        "approve_suggestion": lambda p: f"Approve suggestion #{p.get('suggestion_id')}",
        "delete_book": lambda p: _delete_book_line(p.get("book_id")),
        "delete_suggestion": lambda p: f"Delete suggestion #{p.get('suggestion_id')}",
        "add_title": lambda p: f"Add title \"{p.get('text')}\" -> {_user_name(p.get('user_id'))}",
        "remove_title": lambda p: _remove_title_line(p.get("title_id")),
        "set_rank": lambda p: f"Set rank {get_rank_name(int(p.get('rank', 0)))} for {_user_name(p.get('user_id'))}",
        "send_notification": lambda p: f"Send notification -> {_user_name(p.get('user_id'))}",
        "delete_comment": lambda p: f"Delete comment #{p.get('comment_id')}",
        "create_corner": lambda p: f"Create corner \"{p.get('name')}\"",
        "delete_corner": lambda p: f"Delete corner #{p.get('corner_id')}",
        "delete_post": lambda p: f"Delete post #{p.get('post_id')}",
        "approve_post": lambda p: f"Approve post #{p.get('post_id')}",
        "assign_badge": lambda p: f"Assign badge {_badge_name(p.get('badge_id'))} \u2192 {_user_name(p.get('user_id'))}",
        "remove_badge": lambda p: f"Remove badge {_badge_name(p.get('badge_id'))} from {_user_name(p.get('user_id'))}",
    }
    return fns.get(action_type, lambda p: action_type)(payload)


def do_or_queue(admin, action_type, payload, session, execute_fn, redirect_url):
    _log_privilege_use(
        session,
        admin,
        "architect" if has_privilege(admin, "architect") else "admin",
        f"admin_action:{action_type}",
        location=redirect_url,
        details=json.dumps(payload, ensure_ascii=True),
    )
    if user_rank(admin) >= 12:
        execute_fn(session, payload)
        if action_type == "set_rank":
            _ensure_architect_exists(session)
        session.commit()
        return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    action = AdminAction(admin_id=admin.id, action_type=action_type, payload=json.dumps(payload))
    session.add(action)
    session.commit()
    return RedirectResponse(url="/admin?queued=1", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/", response_class=HTMLResponse)
def landing(request: Request):
    return _tpl("landing.html", request)


@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    return _tpl("register.html", request)


@app.post("/register", response_class=HTMLResponse)
def register(
    request: Request,
    username: str = Form(..., min_length=3, max_length=64),
    password: str = Form(..., min_length=6),
    bio: str = Form("", max_length=500),
    session: Session = Depends(get_session),
):
    if session.exec(select(User).where(User.username == username)).first():
        return _tpl("register.html", request, status_code=409, error="Username already taken.")
    session.add(User(username=username, password_hash=hash_password(password), bio=bio))
    session.commit()
    return _tpl("register.html", request, pending=True)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return _tpl("login.html", request)


@app.post("/login", response_class=HTMLResponse)
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    session: Session = Depends(get_session),
):
    user = session.exec(select(User).where(User.username == username)).first()
    if not user or not verify_password(password, user.password_hash):
        return _tpl("login.html", request, status_code=401, error="Invalid username or password.")
    if user.is_banned:
        return _tpl("login.html", request, status_code=403, error="This account has been banned.")
    if not user.is_active:
        return _tpl("login.html", request, status_code=403, error="Your account is pending admin approval.")
    token = create_access_token(data={"sub": user.username})
    resp = RedirectResponse(url="/corners", status_code=status.HTTP_303_SEE_OTHER)
    resp.set_cookie(value=token, **COOKIE)
    return resp


@app.get("/logout")
def logout():
    resp = RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    resp.delete_cookie("access_token")
    return resp


@app.get("/corners", response_class=HTMLResponse)
def corners(
    request: Request, q: str | None = Query(default=None),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    if has_privilege(user, "admin"):
        channels = session.exec(
            select(Corner).where(Corner.is_external == False).order_by(Corner.created_at)
        ).all()
        externals = session.exec(select(Corner).where(Corner.is_external == True).order_by(Corner.created_at)).all()
    else:
        channels = session.exec(
            select(Corner).where(Corner.is_hidden == False, Corner.is_external == False).order_by(Corner.created_at)
        ).all()
        externals = session.exec(
            select(Corner).where(Corner.is_external == True, Corner.is_hidden == False).order_by(Corner.created_at)
        ).all()
    saved_external_ids = {
        sub.corner_id for sub in session.exec(
            select(UserCornerSubscription).where(UserCornerSubscription.user_id == user.id)
        ).all()
    }
    search_result = search_error = None
    if q:
        found = session.exec(
            select(User).where(User.username == q, User.is_active == True)
        ).first()
        if found and not found.is_banned:
            search_result = found
        else:
            search_error = f'No user found with exact name "{q}"'
    return _tpl("corners.html", request, session, user,
                channels=channels,
                externals=externals,
                saved_external_ids=saved_external_ids,
                q=q or "",
                search_result=search_result,
                search_error=search_error)


@app.post("/corners/external/{slug}/toggle")
def toggle_external_corner(
    slug: str,
    user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    corner = session.exec(select(Corner).where(Corner.slug == slug)).first()
    if not corner or not corner.is_external:
        raise HTTPException(status_code=404)

    existing = session.exec(
        select(UserCornerSubscription).where(
            UserCornerSubscription.user_id == user.id,
            UserCornerSubscription.corner_id == corner.id,
        )
    ).first()

    if existing:
        session.delete(existing)
    else:
        session.add(UserCornerSubscription(user_id=user.id, corner_id=corner.id))
    session.commit()

    return RedirectResponse(url="/corners", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/corners/library", response_class=HTMLResponse)
def library(
    request: Request,
    tag: str | None = Query(default=None),
    user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    books = session.exec(select(Book).order_by(Book.created_at.desc())).all()
    library_corner = session.exec(select(Corner).where(Corner.slug == "library")).first()
    if library_corner and library_corner.is_hidden and not has_privilege(user, "admin"):
        raise HTTPException(status_code=404)
    all_corner_tags = _available_tags_for_corner(session, library_corner.id if library_corner else None)
    tag = _sanitize_tag_query_for_user(session, user, tag, all_corner_tags)
    available_tags = _filter_tags_visible_for_user(session, user, all_corner_tags)
    book_ids = [b.id for b in books if b.id is not None]
    tags_by_book = _load_tags_for_books(session, book_ids)
    visible_books = []
    for b in books:
        if b.id is None:
            continue
        book_tags = tags_by_book.get(b.id, [])
        if tag and not any((t.slug == tag or t.name == tag) for t in book_tags):
            continue
        if _tags_visible_for_user(session, user, book_tags):
            visible_books.append(b)
    authors: dict[int, User] = {}
    for b in visible_books:
        if b.author_id not in authors:
            a = session.get(User, b.author_id)
            if a:
                authors[b.author_id] = a
    return _tpl("library.html", request, session, user, books=visible_books, authors=authors, available_tags=available_tags, current_tag=tag or "")


@app.get("/corners/library/write", response_class=HTMLResponse)
def book_write_page(request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    return _tpl("book_write.html", request, session, user, tags=_library_tags_for_write(session))


@app.post("/corners/library/write", response_class=HTMLResponse)
def book_write(
    request: Request,
    title: str = Form(..., min_length=1, max_length=200),
    content: str = Form(..., min_length=1),
    tag_ids: list[int] | None = Form(default=None),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    title, content = title.strip(), content.strip()
    parts = _split_book_pages(content)
    normalized_content = parts[0] if parts else ""
    tags_for_form = _library_tags_for_write(session)
    if not title or not normalized_content:
        return _tpl(
            "book_write.html", request, session, user,
            tags=tags_for_form, error="Title and content are required.",
        )
    book = Book(title=title, content=normalized_content, author_id=user.id)
    session.add(book)
    session.commit()
    if len(parts) > 1:
        for idx, page_content in enumerate(parts, start=1):
            session.add(BookPage(book_id=book.id, page_number=idx, content=page_content))
        session.commit()
    if tag_ids:
        library_corner = session.exec(select(Corner).where(Corner.slug == "library")).first()
        library_corner_id = library_corner.id if library_corner else None
        is_admin = has_privilege(user, "admin")
        tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all()  # type: ignore[attr-defined]
        for t in tags:
            if _tag_selectable_for_user(session, user, t, is_admin) and _tag_allowed_in_corner(session, t.id, library_corner_id):
                session.add(BookTag(book_id=book.id, tag_id=t.id))
        session.commit()
    _notify_followers(session, user.id, f"{user.username} published a new book: {book.title}")
    session.commit()
    return RedirectResponse(url=f"/corners/library/{book.id}", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/corners/library/{book_id}", response_class=HTMLResponse)
def book_detail(
    book_id: int,
    request: Request,
    page: int = Query(1, ge=1),
    user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    book = session.get(Book, book_id)
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    tags_by_book = _load_tags_for_books(session, [book_id])
    tags = tags_by_book.get(book_id, [])
    if not _tags_visible_for_user(session, user, tags):
        raise HTTPException(status_code=403, detail="You cannot view this post.")
    if book.author_id != user.id:
        book.views += 1
        session.add(book)
        session.commit()
        session.refresh(book)
    author = session.get(User, book.author_id)
    library_corner = session.exec(select(Corner).where(Corner.slug == "library")).first()
    detail_tags = _available_tags_for_corner(session, library_corner.id if library_corner else None)
    pages = session.exec(select(BookPage).where(BookPage.book_id == book.id).order_by(BookPage.page_number)).all()
    total_pages = len(pages) if pages else 1
    current_page = min(page, total_pages)
    page_content = pages[current_page - 1].content if pages else book.content
    comments = _get_comments(session, "book", book_id)
    can_cmt = _can_comment(session, user, "book", book_id)
    book_tag_ids = [t.id for t in tags if t.id is not None]
    return _tpl("book_detail.html", request, session, user,
                book=book, author=author, comments=comments, can_comment=can_cmt,
                all_tags=detail_tags,
                book_tags=tags,
                book_tag_ids=book_tag_ids,
                page_content=page_content,
                current_page=current_page,
                total_pages=total_pages,
                can_edit_tags=(book.author_id == user.id or has_privilege(user, "admin")),
                can_edit_book=(book.author_id == user.id))


@app.get("/corners/library/{book_id}/edit", response_class=HTMLResponse)
def book_edit_page(
    book_id: int,
    request: Request,
    user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    book = session.get(Book, book_id)
    if not book:
        raise HTTPException(status_code=404)
    if book.author_id != user.id:
        raise HTTPException(status_code=403, detail="Only the author can edit this book")
    tags_by_book = _load_tags_for_books(session, [book_id])
    tags = tags_by_book.get(book_id, [])
    if not _tags_visible_for_user(session, user, tags):
        raise HTTPException(status_code=403, detail="You cannot view this post.")
    library_corner = session.exec(select(Corner).where(Corner.slug == "library")).first()
    all_tags = _available_tags_for_corner(session, library_corner.id if library_corner else None)
    body = _book_editor_content(session, book)
    selected_ids = [t.id for t in tags if t.id is not None]
    return _tpl(
        "book_edit.html",
        request,
        session,
        user,
        book=book,
        body=body,
        tags=all_tags,
        selected_tag_ids=selected_ids,
    )


@app.post("/corners/library/{book_id}/edit", response_class=HTMLResponse)
def book_edit(
    book_id: int,
    request: Request,
    title: str = Form(..., min_length=1, max_length=200),
    content: str = Form(..., min_length=1),
    tag_ids: list[int] | None = Form(default=None),
    user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    book = session.get(Book, book_id)
    if not book:
        raise HTTPException(status_code=404)
    if book.author_id != user.id:
        raise HTTPException(status_code=403, detail="Only the author can edit this book")
    title, content = title.strip(), content.strip()
    parts = _split_book_pages(content)
    normalized_content = parts[0] if parts else ""
    if not title or not normalized_content:
        library_corner = session.exec(select(Corner).where(Corner.slug == "library")).first()
        all_tags = _available_tags_for_corner(session, library_corner.id if library_corner else None)
        return _tpl(
            "book_edit.html",
            request,
            session,
            user,
            book=book,
            body=content,
            tags=all_tags,
            selected_tag_ids=tag_ids or [],
            error="Title and content are required.",
        )
    book.title = title
    book.content = normalized_content
    book.updated_at = datetime.now(timezone.utc)
    session.add(book)
    for row in session.exec(select(BookPage).where(BookPage.book_id == book_id)).all():
        session.delete(row)
    if len(parts) > 1:
        for idx, page_content in enumerate(parts, start=1):
            session.add(BookPage(book_id=book.id, page_number=idx, content=page_content))
    session.commit()
    library_corner = session.exec(select(Corner).where(Corner.slug == "library")).first()
    library_corner_id = library_corner.id if library_corner else None
    is_admin = has_privilege(user, "admin")
    for row in session.exec(select(BookTag).where(BookTag.book_id == book_id)).all():
        session.delete(row)
    if tag_ids:
        tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all()  # type: ignore[attr-defined]
        for t in tags:
            if _tag_selectable_for_user(session, user, t, is_admin) and t.id is not None and _tag_allowed_in_corner(session, t.id, library_corner_id):
                session.add(BookTag(book_id=book.id, tag_id=t.id))
    session.commit()
    return RedirectResponse(url=f"/corners/library/{book.id}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/corners/library/{book_id}/tags")
def book_set_tags(
    book_id: int,
    tag_ids: list[int] | None = Form(default=None),
    user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    book = session.get(Book, book_id)
    if not book:
        raise HTTPException(status_code=404)
    is_admin = has_privilege(user, "admin")
    if not (is_admin or book.author_id == user.id):
        raise HTTPException(status_code=403, detail="Not allowed")

    tag_ids = tag_ids or []
    tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all() if tag_ids else []
    library_corner = session.exec(select(Corner).where(Corner.slug == "library")).first()
    library_corner_id = library_corner.id if library_corner else None
    allowed = [
        t for t in tags
        if _tag_selectable_for_user(session, user, t, is_admin)
        and t.id is not None
        and _tag_allowed_in_corner(session, t.id, library_corner_id)
    ]

    for row in session.exec(select(BookTag).where(BookTag.book_id == book_id)).all():
        session.delete(row)
    for t in allowed:
        session.add(BookTag(book_id=book_id, tag_id=t.id))
    session.commit()
    return RedirectResponse(url=f"/corners/library/{book_id}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/corners/library/{book_id}/delete")
def book_delete(book_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    if not session.get(Book, book_id):
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "delete_book", {"book_id": book_id}, session, _exec_delete_book, "/corners/library")


@app.get("/corners/suggestions", response_class=HTMLResponse)
def suggestions_list(
    request: Request, sort: str = Query("new"), tab: str = Query("open"), tag: str | None = Query(default=None),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    suggestion_corner = session.exec(select(Corner).where(Corner.slug == "suggestions")).first()
    if suggestion_corner and suggestion_corner.is_hidden and not has_privilege(user, "admin"):
        raise HTTPException(status_code=404)
    is_approved = tab == "approved"
    items = session.exec(select(Suggestion).where(Suggestion.is_approved == is_approved)).all()
    all_corner_tags = _available_tags_for_corner(session, suggestion_corner.id if suggestion_corner else None)
    tag = _sanitize_tag_query_for_user(session, user, tag, all_corner_tags)
    available_tags = _filter_tags_visible_for_user(session, user, all_corner_tags)
    suggestion_ids = [s.id for s in items if s.id is not None]
    tags_by_s = _load_tags_for_suggestions(session, suggestion_ids)
    items = [
        s for s in items
        if s.id is not None
        and (not tag or any((t.slug == tag or t.name == tag) for t in tags_by_s.get(s.id, [])))
        and _tags_visible_for_user(session, user, tags_by_s.get(s.id, []))
    ]
    scored = []
    authors: dict[int, User] = {}
    for s in items:
        votes = session.exec(select(Vote).where(Vote.suggestion_id == s.id)).all()
        up = sum(1 for v in votes if v.value == 1)
        down = sum(1 for v in votes if v.value == -1)
        my_vote = next((v.value for v in votes if v.user_id == user.id), 0)
        scored.append({"s": s, "up": up, "down": down, "score": up - down, "my_vote": my_vote})
        if s.author_id not in authors:
            a = session.get(User, s.author_id)
            if a:
                authors[s.author_id] = a
    if sort == "top":
        scored.sort(key=lambda x: x["score"], reverse=True)
    else:
        scored.sort(key=lambda x: x["s"].created_at, reverse=True)
    return _tpl("suggestions.html", request, session, user,
                items=scored, authors=authors, sort=sort, tab=tab, available_tags=available_tags, current_tag=tag or "")


@app.get("/corners/suggestions/write", response_class=HTMLResponse)
def suggestion_write_page(request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    suggestion_corner = session.exec(select(Corner).where(Corner.slug == "suggestions")).first()
    tags = _available_tags_for_corner(session, suggestion_corner.id if suggestion_corner else None)
    return _tpl("suggestion_write.html", request, session, user, tags=tags)


@app.post("/corners/suggestions/write", response_class=HTMLResponse)
def suggestion_write(
    request: Request,
    title: str = Form(..., min_length=1, max_length=200),
    body: str = Form(..., min_length=1),
    tag_ids: list[int] | None = Form(default=None),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    title, body = title.strip(), body.strip()
    if not title or not body:
        return _tpl("suggestion_write.html", request, session, user, error="Title and description are required.")
    s = Suggestion(title=title, body=body, author_id=user.id)
    session.add(s)
    session.commit()
    if tag_ids:
        suggestion_corner = session.exec(select(Corner).where(Corner.slug == "suggestions")).first()
        suggestion_corner_id = suggestion_corner.id if suggestion_corner else None
        is_admin = has_privilege(user, "admin")
        tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all()  # type: ignore[attr-defined]
        for t in tags:
            if _tag_selectable_for_user(session, user, t, is_admin) and _tag_allowed_in_corner(session, t.id, suggestion_corner_id):
                session.add(SuggestionTag(suggestion_id=s.id, tag_id=t.id))
        session.commit()
    _notify_followers(session, user.id, f"{user.username} published a new suggestion: {s.title}")
    session.commit()
    return RedirectResponse(url=f"/corners/suggestions/{s.id}", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/corners/suggestions/{sid}", response_class=HTMLResponse)
def suggestion_detail(sid: int, request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    s = session.get(Suggestion, sid)
    if not s:
        raise HTTPException(status_code=404)
    tags_by_s = _load_tags_for_suggestions(session, [sid])
    s_tags = tags_by_s.get(sid, [])
    if not _tags_visible_for_user(session, user, s_tags):
        raise HTTPException(status_code=403, detail="You cannot view this post.")
    author = session.get(User, s.author_id)
    votes = session.exec(select(Vote).where(Vote.suggestion_id == s.id)).all()
    up = sum(1 for v in votes if v.value == 1)
    down = sum(1 for v in votes if v.value == -1)
    my_vote = next((v.value for v in votes if v.user_id == user.id), 0)
    comments = _get_comments(session, "suggestion", sid)
    can_cmt = _can_comment(session, user, "suggestion", sid)
    suggestion_corner = session.exec(select(Corner).where(Corner.slug == "suggestions")).first()
    detail_tags = _available_tags_for_corner(session, suggestion_corner.id if suggestion_corner else None)
    return _tpl("suggestion_detail.html", request, session, user,
                s=s, author=author, up=up, down=down, my_vote=my_vote,
                comments=comments, can_comment=can_cmt,
                all_tags=detail_tags,
                s_tags=s_tags,
                s_tag_ids=[t.id for t in s_tags if t.id is not None],
                can_edit_tags=(s.author_id == user.id or has_privilege(user, "admin")))


@app.post("/corners/suggestions/{sid}/tags")
def suggestion_set_tags(
    sid: int,
    tag_ids: list[int] | None = Form(default=None),
    user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    s = session.get(Suggestion, sid)
    if not s:
        raise HTTPException(status_code=404)
    is_admin = has_privilege(user, "admin")
    if not (is_admin or s.author_id == user.id):
        raise HTTPException(status_code=403, detail="Not allowed")

    tag_ids = tag_ids or []
    tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all() if tag_ids else []
    suggestion_corner = session.exec(select(Corner).where(Corner.slug == "suggestions")).first()
    suggestion_corner_id = suggestion_corner.id if suggestion_corner else None
    allowed = [
        t for t in tags
        if _tag_selectable_for_user(session, user, t, is_admin)
        and t.id is not None
        and _tag_allowed_in_corner(session, t.id, suggestion_corner_id)
    ]

    for row in session.exec(select(SuggestionTag).where(SuggestionTag.suggestion_id == sid)).all():
        session.delete(row)
    for t in allowed:
        session.add(SuggestionTag(suggestion_id=sid, tag_id=t.id))
    session.commit()
    return RedirectResponse(url=f"/corners/suggestions/{sid}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/corners/suggestions/{sid}/vote")
def suggestion_vote(sid: int, value: int = Form(...), user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    if not session.get(Suggestion, sid):
        raise HTTPException(status_code=404)
    if value not in (1, -1):
        raise HTTPException(status_code=400)
    existing = session.exec(select(Vote).where(Vote.suggestion_id == sid, Vote.user_id == user.id)).first()
    if existing:
        if existing.value == value:
            session.delete(existing)
        else:
            existing.value = value
            session.add(existing)
    else:
        session.add(Vote(suggestion_id=sid, user_id=user.id, value=value))
    session.commit()
    return RedirectResponse(url=f"/corners/suggestions/{sid}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/corners/suggestions/{sid}/delete")
def suggestion_delete(sid: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    if not session.get(Suggestion, sid):
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "delete_suggestion", {"suggestion_id": sid}, session, _exec_delete_suggestion, "/corners/suggestions")


@app.post("/admin/suggestion/approve/{sid}")
def suggestion_approve(sid: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    if not session.get(Suggestion, sid):
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "approve_suggestion", {"suggestion_id": sid}, session, _exec_approve_suggestion, "/corners/suggestions")


@app.get("/corners/{slug}", response_class=HTMLResponse)
def generic_corner_list(
    slug: str, request: Request,
    sort: str = Query("new"), tab: str = Query("open"), tag: str | None = Query(default=None),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    corner = session.exec(select(Corner).where(Corner.slug == slug)).first()
    if not corner:
        raise HTTPException(status_code=404)
    if corner.is_hidden and not has_privilege(user, "admin"):
        raise HTTPException(status_code=404)

    if corner.template_type == "suggestions":
        is_approved = tab == "approved"
        posts = session.exec(
            select(Post).where(Post.corner_id == corner.id, Post.is_approved == is_approved)
        ).all()
        all_corner_tags = _available_tags_for_corner(session, corner.id)
        tag = _sanitize_tag_query_for_user(session, user, tag, all_corner_tags)
        available_tags = _filter_tags_visible_for_user(session, user, all_corner_tags)
        post_ids = [p.id for p in posts if p.id is not None]
        tags_by_post = _load_tags_for_posts(session, post_ids)
        posts = [
            p for p in posts
            if p.id is not None
            and (not tag or any((t.slug == tag or t.name == tag) for t in tags_by_post.get(p.id, [])))
            and _tags_visible_for_user(session, user, tags_by_post.get(p.id, []))
        ]
        scored = []
        authors: dict[int, User] = {}
        for p in posts:
            votes = session.exec(select(PostVote).where(PostVote.post_id == p.id)).all()
            up = sum(1 for v in votes if v.value == 1)
            down = sum(1 for v in votes if v.value == -1)
            my_vote = next((v.value for v in votes if v.user_id == user.id), 0)
            scored.append({"p": p, "up": up, "down": down, "score": up - down, "my_vote": my_vote})
            if p.author_id not in authors:
                a = session.get(User, p.author_id)
                if a:
                    authors[p.author_id] = a
        if sort == "top":
            scored.sort(key=lambda x: x["score"], reverse=True)
        else:
            scored.sort(key=lambda x: x["p"].created_at, reverse=True)
        return _tpl("generic_suggestions.html", request, session, user,
                     corner=corner, items=scored, authors=authors, sort=sort, tab=tab, available_tags=available_tags, current_tag=tag or "")
    else:
        posts = session.exec(
            select(Post).where(Post.corner_id == corner.id).order_by(Post.created_at.desc())
        ).all()
        all_corner_tags = _available_tags_for_corner(session, corner.id)
        tag = _sanitize_tag_query_for_user(session, user, tag, all_corner_tags)
        available_tags = _filter_tags_visible_for_user(session, user, all_corner_tags)
        post_ids = [p.id for p in posts if p.id is not None]
        tags_by_post = _load_tags_for_posts(session, post_ids)
        posts = [
            p for p in posts
            if p.id is not None
            and (not tag or any((t.slug == tag or t.name == tag) for t in tags_by_post.get(p.id, [])))
            and _tags_visible_for_user(session, user, tags_by_post.get(p.id, []))
        ]
        authors: dict[int, User] = {}
        for p in posts:
            if p.author_id not in authors:
                a = session.get(User, p.author_id)
                if a:
                    authors[p.author_id] = a
        return _tpl("generic_list.html", request, session, user,
                     corner=corner, posts=posts, authors=authors, available_tags=available_tags, current_tag=tag or "")


@app.get("/corners/{slug}/write", response_class=HTMLResponse)
def generic_write_page(slug: str, request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    corner = session.exec(select(Corner).where(Corner.slug == slug)).first()
    if not corner:
        raise HTTPException(status_code=404)
    tags = _available_tags_for_corner(session, corner.id)
    return _tpl("generic_write.html", request, session, user, corner=corner, tags=tags)


@app.post("/corners/{slug}/write", response_class=HTMLResponse)
def generic_write(
    slug: str, request: Request,
    title: str = Form(..., min_length=1, max_length=200),
    content: str = Form(..., min_length=1),
    tag_ids: list[int] | None = Form(default=None),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    corner = session.exec(select(Corner).where(Corner.slug == slug)).first()
    if not corner:
        raise HTTPException(status_code=404)
    title, content = title.strip(), content.strip()
    if not title or not content:
        return _tpl("generic_write.html", request, session, user, corner=corner, error="Title and content required.")
    post = Post(corner_id=corner.id, title=title, content=content, author_id=user.id)
    session.add(post)
    session.commit()
    if tag_ids:
        is_admin = has_privilege(user, "admin")
        tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all()  # type: ignore[attr-defined]
        for t in tags:
            if _tag_selectable_for_user(session, user, t, is_admin) and _tag_allowed_in_corner(session, t.id, corner.id):
                session.add(PostTag(post_id=post.id, tag_id=t.id))
        session.commit()
    _notify_followers(session, user.id, f"{user.username} published in {corner.name}: {post.title}")
    session.commit()
    return RedirectResponse(url=f"/corners/{slug}/{post.id}", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/corners/{slug}/{post_id}", response_class=HTMLResponse)
def generic_detail(
    slug: str, post_id: int, request: Request,
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    corner = session.exec(select(Corner).where(Corner.slug == slug)).first()
    if not corner:
        raise HTTPException(status_code=404)
    post = session.get(Post, post_id)
    if not post or post.corner_id != corner.id:
        raise HTTPException(status_code=404)

    tags_by_post = _load_tags_for_posts(session, [post.id]) if post.id is not None else {}
    post_tags = tags_by_post.get(post.id, []) if post.id is not None else []
    if not _tags_visible_for_user(session, user, post_tags):
        raise HTTPException(status_code=403, detail="You cannot view this post.")

    if corner.template_type == "library" and post.author_id != user.id:
        post.views += 1
        session.add(post)
        session.commit()
        session.refresh(post)

    author = session.get(User, post.author_id)
    comments = _get_comments(session, "post", post_id)
    can_cmt = _can_comment(session, user, "post", post_id)

    extra = {}
    if corner.template_type == "suggestions":
        votes = session.exec(select(PostVote).where(PostVote.post_id == post.id)).all()
        extra["up"] = sum(1 for v in votes if v.value == 1)
        extra["down"] = sum(1 for v in votes if v.value == -1)
        extra["my_vote"] = next((v.value for v in votes if v.user_id == user.id), 0)

    all_tags = _available_tags_for_corner(session, corner.id)
    return _tpl("generic_detail.html", request, session, user,
                corner=corner, post=post, author=author,
                comments=comments, can_comment=can_cmt,
                all_tags=all_tags, post_tags=post_tags,
                post_tag_ids=[t.id for t in post_tags if t.id is not None],
                can_edit_tags=(post.author_id == user.id or has_privilege(user, "admin")),
                **extra)


@app.post("/corners/{slug}/{post_id}/tags")
def post_set_tags(
    slug: str,
    post_id: int,
    tag_ids: list[int] | None = Form(default=None),
    user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    corner = session.exec(select(Corner).where(Corner.slug == slug)).first()
    if not corner:
        raise HTTPException(status_code=404)
    post = session.get(Post, post_id)
    if not post or post.corner_id != corner.id:
        raise HTTPException(status_code=404)

    is_admin = has_privilege(user, "admin")
    if not (is_admin or post.author_id == user.id):
        raise HTTPException(status_code=403, detail="Not allowed")

    tag_ids = tag_ids or []
    tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all() if tag_ids else []
    allowed = [
        t for t in tags
        if _tag_selectable_for_user(session, user, t, is_admin)
        and t.id is not None
        and _tag_allowed_in_corner(session, t.id, corner.id)
    ]

    for row in session.exec(select(PostTag).where(PostTag.post_id == post_id)).all():
        session.delete(row)
    for t in allowed:
        session.add(PostTag(post_id=post_id, tag_id=t.id))
    session.commit()
    return RedirectResponse(url=f"/corners/{slug}/{post_id}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/corners/{slug}/{post_id}/vote")
def generic_post_vote(
    slug: str, post_id: int, value: int = Form(...),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    corner = session.exec(select(Corner).where(Corner.slug == slug)).first()
    if not corner or corner.template_type != "suggestions":
        raise HTTPException(status_code=404)
    post = session.get(Post, post_id)
    if not post or post.corner_id != corner.id:
        raise HTTPException(status_code=404)
    if value not in (1, -1):
        raise HTTPException(status_code=400)
    existing = session.exec(select(PostVote).where(PostVote.post_id == post_id, PostVote.user_id == user.id)).first()
    if existing:
        if existing.value == value:
            session.delete(existing)
        else:
            existing.value = value
            session.add(existing)
    else:
        session.add(PostVote(post_id=post_id, user_id=user.id, value=value))
    session.commit()
    return RedirectResponse(url=f"/corners/{slug}/{post_id}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/corners/{slug}/{post_id}/delete")
def generic_post_delete(slug: str, post_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    corner = session.exec(select(Corner).where(Corner.slug == slug)).first()
    if not corner:
        raise HTTPException(status_code=404)
    if not session.get(Post, post_id):
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "delete_post", {"post_id": post_id}, session, _exec_delete_post, f"/corners/{slug}")


@app.post("/admin/post/approve/{post_id}")
def post_approve(post_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    if not session.get(Post, post_id):
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "approve_post", {"post_id": post_id}, session, _exec_approve_post, "/admin")


def _comment_redirect(session, target_type, target_id):
    if target_type == "book":
        return f"/corners/library/{target_id}"
    if target_type == "suggestion":
        return f"/corners/suggestions/{target_id}"
    if target_type == "post":
        post = session.get(Post, target_id)
        if post:
            corner = session.get(Corner, post.corner_id)
            if corner:
                return f"/corners/{corner.slug}/{target_id}"
    return "/corners"


@app.post("/comment/{target_type}/{target_id}")
def add_comment(
    target_type: str, target_id: int,
    content: str = Form(..., min_length=1, max_length=2000),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    if not has_privilege(user, "comment"):
        raise HTTPException(status_code=403, detail="Insufficient rank to comment")
    if target_type == "book" and not session.get(Book, target_id):
        raise HTTPException(status_code=404)
    elif target_type == "suggestion" and not session.get(Suggestion, target_id):
        raise HTTPException(status_code=404)
    elif target_type == "post" and not session.get(Post, target_id):
        raise HTTPException(status_code=404)
    elif target_type not in ("book", "suggestion", "post"):
        raise HTTPException(status_code=400)

    if not has_privilege(user, "unlimited_comments"):
        existing = session.exec(select(Comment).where(
            Comment.author_id == user.id, Comment.target_type == target_type,
            Comment.target_id == target_id, Comment.parent_id == None,
        )).first()
        if existing:
            raise HTTPException(status_code=400, detail="Already commented on this item")

    _log_privilege_use(
        session,
        user,
        "comment",
        "add_comment",
        location=f"{target_type}:{target_id}",
        details=content.strip(),
    )
    session.add(Comment(content=content.strip(), author_id=user.id, target_type=target_type, target_id=target_id))
    session.commit()
    return RedirectResponse(url=_comment_redirect(session, target_type, target_id), status_code=status.HTTP_303_SEE_OTHER)


@app.post("/comment/{comment_id}/reply")
def reply_comment(
    comment_id: int,
    content: str = Form(..., min_length=1, max_length=2000),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    if not has_privilege(user, "reply"):
        raise HTTPException(status_code=403, detail="Insufficient rank to reply")
    parent = session.get(Comment, comment_id)
    if not parent or parent.parent_id is not None:
        raise HTTPException(status_code=404)
    _log_privilege_use(
        session,
        user,
        "reply",
        "reply_comment",
        location=f"{parent.target_type}:{parent.target_id}",
        details=content.strip(),
    )
    session.add(Comment(
        content=content.strip(), author_id=user.id,
        target_type=parent.target_type, target_id=parent.target_id, parent_id=parent.id,
    ))
    session.commit()
    return RedirectResponse(url=_comment_redirect(session, parent.target_type, parent.target_id), status_code=status.HTTP_303_SEE_OTHER)


@app.post("/comment/{comment_id}/delete")
def delete_comment(comment_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    c = session.get(Comment, comment_id)
    if not c:
        raise HTTPException(status_code=404)
    redirect = _comment_redirect(session, c.target_type, c.target_id)
    return do_or_queue(admin, "delete_comment", {"comment_id": comment_id}, session, _exec_delete_comment, redirect)


@app.get("/user/{username}", response_class=HTMLResponse)
def user_profile(username: str, request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    profile = session.exec(select(User).where(User.username == username)).first()
    if not profile:
        raise HTTPException(status_code=404)
    books = session.exec(select(Book).where(Book.author_id == profile.id).order_by(Book.created_at.desc())).all()
    book_ids = [b.id for b in books if b.id is not None]
    tags_by_book = _load_tags_for_books(session, book_ids)
    books = [b for b in books if b.id is not None and _tags_visible_for_user(session, user, tags_by_book.get(b.id, []))]
    total_views = sum(b.views for b in books)
    ss = _suggestion_stats(session, profile.id, viewer=user)
    titles = session.exec(select(Title).where(Title.user_id == profile.id).order_by(Title.created_at)).all()
    cstats = _corner_stats(session, profile.id)
    all_raw = session.exec(select(Title)).all()
    seen = {}
    for t in all_raw:
        if t.text not in seen:
            seen[t.text] = t.description or ""
    all_saved_titles = [{"text": k, "description": v} for k, v in sorted(seen.items())]
    all_badges = session.exec(select(Badge).order_by(Badge.name)).all()
    ub_rows = session.exec(select(UserBadge).where(UserBadge.user_id == profile.id)).all()
    profile_badges = []
    profile_badge_ids = set()
    for ub in ub_rows:
        b = session.get(Badge, ub.badge_id)
        if b:
            profile_badges.append(b)
            profile_badge_ids.add(b.id)
    profile_has_privileges = _user_has_any_privilege(profile)
    followers_count = session.exec(
        select(sa_func.count()).select_from(Follow).where(Follow.following_id == profile.id)
    ).one()
    following_count = session.exec(
        select(sa_func.count()).select_from(Follow).where(Follow.follower_id == profile.id)
    ).one()
    is_following = session.exec(
        select(Follow).where(Follow.follower_id == user.id, Follow.following_id == profile.id)
    ).first() is not None
    profile_externals: list[Corner] = []
    admin_viewer = has_privilege(user, "admin")
    for sub in session.exec(
        select(UserCornerSubscription).where(UserCornerSubscription.user_id == profile.id)
    ).all():
        c = session.get(Corner, sub.corner_id)
        if not c or not c.is_external:
            continue
        if c.is_hidden and not admin_viewer:
            continue
        profile_externals.append(c)
    profile_externals.sort(key=lambda x: (x.name or "").lower())
    can_edit_bio = profile.id == user.id or has_privilege(user, "admin")
    can_view_privilege_log = (
        profile_has_privileges
        and (user_rank(user) >= 3 or has_privilege(user, "admin"))
        and (profile.id == user.id or has_privilege(user, "admin"))
    )
    return _tpl("user_profile.html", request, session, user,
                profile=profile, books=books, total_views=total_views, titles=titles,
                corner_stats=cstats, all_saved_titles=all_saved_titles,
                all_badges=all_badges, profile_badges=profile_badges,
                profile_badge_ids=profile_badge_ids, profile_has_privileges=profile_has_privileges,
                followers_count=followers_count, following_count=following_count, is_following=is_following,
                profile_externals=profile_externals, can_edit_bio=can_edit_bio,
                can_view_privilege_log=can_view_privilege_log, **ss)


@app.post("/user/{username}/bio", response_class=HTMLResponse)
def update_profile_bio(
    username: str,
    bio: str = Form("", max_length=500),
    user: User = Depends(get_current_user),
    session: Session = Depends(get_session),
):
    profile = session.exec(select(User).where(User.username == username)).first()
    if not profile:
        raise HTTPException(status_code=404)
    if profile.id != user.id and not has_privilege(user, "admin"):
        raise HTTPException(status_code=403, detail="Not allowed")
    old_bio = profile.bio or ""
    new_bio = (bio or "").strip()[:500]
    profile.bio = new_bio
    session.add(profile)
    if user.id != profile.id:
        _log_privilege_use(
            session,
            user,
            "admin" if has_privilege(user, "admin") else "comment",
            "update_user_bio",
            location=f"/user/{profile.username}",
            details=f"actor={user.username}; target={profile.username}; old={old_bio}; new={new_bio}",
        )
    session.commit()
    return RedirectResponse(url=f"/user/{profile.username}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/user/{username}/follow")
def follow_user(username: str, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    profile = session.exec(select(User).where(User.username == username)).first()
    if not profile:
        raise HTTPException(status_code=404)
    if profile.id == user.id:
        return RedirectResponse(url=f"/user/{username}", status_code=status.HTTP_303_SEE_OTHER)
    existing = session.exec(select(Follow).where(Follow.follower_id == user.id, Follow.following_id == profile.id)).first()
    if not existing:
        session.add(Follow(follower_id=user.id, following_id=profile.id))
        session.commit()
    return RedirectResponse(url=f"/user/{username}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/user/{username}/unfollow")
def unfollow_user(username: str, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    profile = session.exec(select(User).where(User.username == username)).first()
    if not profile:
        raise HTTPException(status_code=404)
    existing = session.exec(select(Follow).where(Follow.follower_id == user.id, Follow.following_id == profile.id)).first()
    if existing:
        session.delete(existing)
        session.commit()
    return RedirectResponse(url=f"/user/{username}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/user/follow/{user_id}")
def follow_user_by_id(user_id: int, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    profile = session.get(User, user_id)
    if not profile:
        raise HTTPException(status_code=404, detail="User not found")
    if profile.id == user.id:
        return RedirectResponse(url=f"/user/{profile.username}", status_code=status.HTTP_303_SEE_OTHER)
    existing = session.exec(select(Follow).where(Follow.follower_id == user.id, Follow.following_id == profile.id)).first()
    if not existing:
        session.add(Follow(follower_id=user.id, following_id=profile.id))
        session.commit()
    return RedirectResponse(url=f"/user/{profile.username}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/user/unfollow/{user_id}")
def unfollow_user_by_id(user_id: int, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    profile = session.get(User, user_id)
    if not profile:
        raise HTTPException(status_code=404, detail="User not found")
    existing = session.exec(select(Follow).where(Follow.follower_id == user.id, Follow.following_id == profile.id)).first()
    if existing:
        session.delete(existing)
        session.commit()
    return RedirectResponse(url=f"/user/{profile.username}", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/user/{username}/logs", response_class=HTMLResponse)
def user_logs(username: str, request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    profile = session.exec(select(User).where(User.username == username)).first()
    if not profile:
        raise HTTPException(status_code=404)
    is_admin = has_privilege(user, "admin")
    if user_rank(user) < 3 and not is_admin:
        raise HTTPException(status_code=403, detail="Rank 3+ required")
    if not is_admin and profile.id != user.id:
        raise HTTPException(status_code=403, detail="Not allowed")
    if not _user_has_any_privilege(profile):
        raise HTTPException(status_code=404)
    logs = session.exec(
        select(PrivilegeLog).where(PrivilegeLog.actor_id == profile.id).order_by(PrivilegeLog.created_at.desc())
    ).all()
    readable_logs = [
        {
            "when": log.created_at.strftime("%Y-%m-%d %H:%M"),
            "what": log.action.replace("_", " ").strip().capitalize(),
            "where": log.location or "General action",
            "details": log.details,
            "privilege": log.privilege.capitalize(),
        }
        for log in logs
    ]
    return _tpl(
        "user_logs.html",
        request,
        session,
        user,
        profile=profile,
        logs=readable_logs,
        can_see_details=is_admin,
        can_clear_logs=has_privilege(user, "architect"),
    )


@app.post("/user/{username}/logs/clear")
def clear_user_logs(
    username: str,
    architect: User = Depends(get_current_architect),
    session: Session = Depends(get_session),
):
    profile = session.exec(select(User).where(User.username == username)).first()
    if not profile:
        raise HTTPException(status_code=404)
    for row in session.exec(select(PrivilegeLog).where(PrivilegeLog.actor_id == profile.id)).all():
        session.delete(row)
    _log_privilege_use(
        session,
        architect,
        "architect",
        "clear_user_logs",
        location=f"/user/{profile.username}/logs",
        details=f"cleared_actor={profile.username}",
    )
    session.commit()
    return RedirectResponse(url=f"/user/{profile.username}/logs", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    books = session.exec(select(Book).where(Book.author_id == user.id).order_by(Book.created_at.desc())).all()
    book_ids = [b.id for b in books if b.id is not None]
    tags_by_book = _load_tags_for_books(session, book_ids)
    books = [b for b in books if b.id is not None and _tags_visible_for_user(session, user, tags_by_book.get(b.id, []))]
    total_views = sum(b.views for b in books)
    ss = _suggestion_stats(session, user.id, viewer=user)
    titles = session.exec(select(Title).where(Title.user_id == user.id).order_by(Title.created_at)).all()
    cstats = _corner_stats(session, user.id)
    ext_subs = session.exec(select(UserCornerSubscription).where(UserCornerSubscription.user_id == user.id)).all()
    ext_ids = [s.corner_id for s in ext_subs]
    saved_externals = (
        session.exec(select(Corner).where(Corner.id.in_(ext_ids), Corner.is_external == True)).all()
        if ext_ids
        else []
    )
    ub_rows = session.exec(select(UserBadge).where(UserBadge.user_id == user.id)).all()
    my_badges = []
    for ub in ub_rows:
        b = session.get(Badge, ub.badge_id)
        if b:
            my_badges.append(b)
    followers_count = session.exec(
        select(sa_func.count()).select_from(Follow).where(Follow.following_id == user.id)
    ).one()
    following_count = session.exec(
        select(sa_func.count()).select_from(Follow).where(Follow.follower_id == user.id)
    ).one()
    return _tpl("dashboard.html", request, session, user,
                books=books, total_views=total_views, titles=titles,
                corner_stats=cstats, saved_externals=saved_externals, my_badges=my_badges,
                followers_count=followers_count, following_count=following_count, **ss)


@app.get("/notifications", response_class=HTMLResponse)
def notifications_page(request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    notes = session.exec(
        select(Notification).where(Notification.user_id == user.id).order_by(Notification.created_at.desc())
    ).all()
    return _tpl("notifications.html", request, session, user, notifications=notes)


@app.post("/notifications/read")
def notifications_mark_read(user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    for n in session.exec(select(Notification).where(
        Notification.user_id == user.id, Notification.is_read == False
    )).all():
        n.is_read = True
        session.add(n)
    session.commit()
    return RedirectResponse(url="/notifications", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/notifications/clear")
def notifications_clear(user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    for n in session.exec(select(Notification).where(Notification.user_id == user.id)).all():
        session.delete(n)
    session.commit()
    return RedirectResponse(url="/notifications", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/admin", response_class=HTMLResponse)
def admin_panel(
    request: Request, queued: int = Query(0),
    admin: User = Depends(get_current_admin), session: Session = Depends(get_session),
):
    pending = session.exec(select(User).where(User.is_active == False, User.rank < 11, User.is_deleted == False)).all()
    pending_suggestions = session.exec(select(Suggestion).where(Suggestion.is_approved == False)).all()
    s_authors: dict[int, User] = {}
    for s in pending_suggestions:
        if s.author_id not in s_authors:
            a = session.get(User, s.author_id)
            if a:
                s_authors[s.author_id] = a

    pending_actions = []
    a_admins: dict[int, User] = {}
    action_summaries: dict[int, str] = {}
    if user_rank(admin) >= 12:
        raw = session.exec(select(AdminAction).where(AdminAction.status == "pending").order_by(AdminAction.created_at)).all()
        for act in raw:
            pending_actions.append(act)
            try:
                payload = json.loads(act.payload)
            except Exception:
                payload = {}
            action_summaries[act.id] = _action_summary(act.action_type, payload, session)
            if act.admin_id not in a_admins:
                adm = session.get(User, act.admin_id)
                if adm:
                    a_admins[act.admin_id] = adm

    all_corners = session.exec(select(Corner).order_by(Corner.created_at)).all()
    external_ids = [c.id for c in all_corners if c.is_external]
    external_counts: dict[int, int] = {cid: 0 for cid in external_ids if cid is not None}
    if external_ids:
        for sub in session.exec(select(UserCornerSubscription).where(UserCornerSubscription.corner_id.in_(external_ids))).all():  # type: ignore[attr-defined]
            if sub.corner_id in external_counts:
                external_counts[sub.corner_id] += 1

    return _tpl("admin.html", request, session, admin, queued=bool(queued),
                pending_users=pending,
                pending_suggestions=pending_suggestions, s_authors=s_authors,
                pending_actions=pending_actions, a_admins=a_admins,
                action_summaries=action_summaries, corners=all_corners,
                external_counts=external_counts)


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users(
    request: Request,
    tab: str = Query("active"),
    admin: User = Depends(get_current_admin),
    session: Session = Depends(get_session),
):
    active_users = session.exec(
        select(User).where(User.is_active == True, User.is_banned == False, User.is_deleted == False).order_by(User.created_at.desc())
    ).all()
    banned_users = session.exec(
        select(User).where(User.is_active == True, User.is_banned == True, User.is_deleted == False).order_by(User.created_at.desc())
    ).all()
    deleted_users = session.exec(
        select(User).where(User.is_deleted == True).order_by(User.deleted_at.desc())
    ).all()

    if tab not in ("active", "banned", "deleted"):
        tab = "active"
    if tab == "banned":
        users = banned_users
    elif tab == "deleted":
        users = deleted_users
    else:
        users = active_users
    return _tpl(
        "admin_users.html",
        request,
        session,
        admin,
        users=users,
        tab=tab,
        active_count=len(active_users),
        banned_count=len(banned_users),
        deleted_count=len(deleted_users),
    )


@app.post("/admin/approve/{user_id}")
def approve_user(user_id: int, title: str = Form(""), admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    if not session.get(User, user_id):
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "approve_user", {"user_id": user_id, "title": title.strip()}, session, _exec_approve_user, "/admin")


@app.post("/admin/decline/{user_id}")
def decline_user(user_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    if not session.get(User, user_id):
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "decline_user", {"user_id": user_id}, session, _exec_decline_user, "/admin")


@app.post("/admin/ban/{user_id}")
def ban_user(
    user_id: int,
    reason: str = Form(..., min_length=1, max_length=500),
    admin: User = Depends(get_current_admin),
    session: Session = Depends(get_session),
):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404)
    if user_rank(u) >= 11:
        raise HTTPException(status_code=400, detail="Cannot ban an admin")
    return do_or_queue(
        admin,
        "ban_user",
        {"user_id": user_id, "reason": reason.strip()},
        session,
        _exec_ban_user,
        f"/user/{u.username}",
    )


@app.post("/admin/unban/{user_id}")
def unban_user(user_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "unban_user", {"user_id": user_id}, session, _exec_unban_user, "/admin")


@app.post("/admin/user/{user_id}/delete")
def delete_user(
    user_id: int,
    reason: str = Form(..., min_length=1, max_length=500),
    admin: User = Depends(get_current_admin),
    session: Session = Depends(get_session),
):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404)
    if u.id == admin.id:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")
    if user_rank(u) >= 11:
        raise HTTPException(status_code=400, detail="Cannot delete an admin")
    if user_rank(u) >= user_rank(admin):
        raise HTTPException(status_code=403, detail="Insufficient rank to delete this user")
    return do_or_queue(
        admin,
        "delete_user",
        {"user_id": user_id, "reason": reason.strip()},
        session,
        _exec_delete_user,
        f"/user/{u.username}",
    )


@app.post("/admin/title/{user_id}")
def add_title(user_id: int, text: str = Form(..., min_length=1, max_length=100), description: str = Form("", max_length=300), admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "add_title", {"user_id": user_id, "text": text.strip(), "description": description.strip()}, session, _exec_add_title, f"/user/{u.username}")


@app.post("/admin/title/{title_id}/delete")
def delete_title(title_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    t = session.get(Title, title_id)
    if not t:
        raise HTTPException(status_code=404)
    u = session.get(User, t.user_id)
    redirect = f"/user/{u.username}" if u else "/admin"
    return do_or_queue(admin, "remove_title", {"title_id": title_id}, session, _exec_remove_title, redirect)


@app.post("/admin/rank/{user_id}")
def set_rank(user_id: int, rank: int = Form(...), admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404)
    if rank < 1 or rank > 12:
        raise HTTPException(status_code=400, detail="Invalid rank")
    if rank >= 12 and user_rank(admin) < 12:
        raise HTTPException(status_code=403, detail="Only Architect can assign rank 12")
    return do_or_queue(admin, "set_rank", {"user_id": user_id, "rank": rank}, session, _exec_set_rank, f"/user/{u.username}")


@app.post("/admin/notify/user/{user_id}")
def send_notification(user_id: int, message: str = Form(..., min_length=1, max_length=500), admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404)
    clean = message.strip()
    _log_privilege_use(
        session,
        admin,
        "admin",
        "send_notification",
        location=f"/user/{u.username}",
        details=f"to={u.username}; message={clean}",
    )
    _exec_send_notification(session, {"user_id": user_id, "message": clean})
    session.commit()
    return RedirectResponse(url=f"/user/{u.username}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/notify/broadcast")
def send_notification_broadcast(
    message: str = Form(..., min_length=1, max_length=500),
    target: str = Form("all"),
    rank: int = Form(1),
    admin: User = Depends(get_current_admin),
    session: Session = Depends(get_session),
):
    clean = message.strip()
    recipients = session.exec(select(User).where(User.is_active == True, User.is_deleted == False)).all()
    if target == "rank":
        recipients = [u for u in recipients if u.rank == rank]
    for recipient in recipients:
        session.add(Notification(user_id=recipient.id, message=clean))
    _log_privilege_use(
        session,
        admin,
        "admin",
        "broadcast_notification",
        location="/admin",
        details=f"target={target}; rank={rank if target == 'rank' else '-'}; recipients={len(recipients)}; message={clean}",
    )
    session.commit()
    return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/corner")
def create_corner(
    name: str = Form(..., min_length=1, max_length=100),
    slug: str = Form(..., min_length=1, max_length=60),
    icon: str = Form("", max_length=8),
    description: str = Form("", max_length=300),
    template_type: str = Form("library"),
    is_external: bool = Form(False),
    admin: User = Depends(get_current_admin), session: Session = Depends(get_session),
):
    slug = slug.strip().lower().replace(" ", "-")
    if slug in RESERVED_SLUGS:
        raise HTTPException(status_code=400, detail="This slug is reserved")
    if session.exec(select(Corner).where(Corner.slug == slug)).first():
        raise HTTPException(status_code=400, detail="A corner with this slug already exists")
    if template_type not in ("library", "suggestions"):
        raise HTTPException(status_code=400)
    payload = {"name": name.strip(), "slug": slug, "icon": icon.strip(),
               "description": description.strip(), "template_type": template_type, "is_external": is_external}
    _log_privilege_use(
        session,
        admin,
        "admin",
        "create_corner",
        location="/admin",
        details=f"name={payload['name']}; slug={payload['slug']}; type={payload['template_type']}; external={payload['is_external']}",
    )
    _exec_create_corner(session, payload)
    session.commit()
    return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/corner/{corner_id}/delete")
def delete_corner(
    corner_id: int,
    admin: User = Depends(get_current_admin),
    session: Session = Depends(get_session),
):
    corner = session.get(Corner, corner_id)
    if not corner:
        raise HTTPException(status_code=404)
    _log_privilege_use(
        session,
        admin,
        "admin",
        "delete_corner",
        location="/admin",
        details=f"corner={corner.name}; slug={corner.slug}",
    )
    _exec_delete_corner(session, {"corner_id": corner_id})
    session.commit()
    return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/corner/{corner_id}/toggle-hidden")
def toggle_corner_hidden(
    corner_id: int,
    admin: User = Depends(get_current_admin),
    session: Session = Depends(get_session),
):
    corner = session.get(Corner, corner_id)
    if not corner:
        raise HTTPException(status_code=404)
    corner.is_hidden = not corner.is_hidden
    session.add(corner)
    _log_privilege_use(
        session,
        admin,
        "admin",
        "toggle_corner_hidden",
        location="/admin",
        details=f"corner={corner.name}; slug={corner.slug}; hidden={corner.is_hidden}",
    )
    session.commit()
    return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/actions/{action_id}/approve")
def approve_action(action_id: int, architect: User = Depends(get_current_architect), session: Session = Depends(get_session)):
    action = session.get(AdminAction, action_id)
    if not action or action.status != "pending":
        raise HTTPException(status_code=404)
    handler = ACTION_DISPATCH.get(action.action_type)
    if handler:
        handler(session, json.loads(action.payload))
    _log_privilege_use(
        session,
        architect,
        "architect",
        "approve_admin_action",
        location="/admin",
        details=f"action_id={action.id}; type={action.action_type}",
    )
    action.status = "approved"
    action.reviewed_at = datetime.now(timezone.utc)
    session.add(action)
    _ensure_architect_exists(session)
    session.commit()
    return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/actions/{action_id}/reject")
def reject_action(action_id: int, architect: User = Depends(get_current_architect), session: Session = Depends(get_session)):
    action = session.get(AdminAction, action_id)
    if not action or action.status != "pending":
        raise HTTPException(status_code=404)
    _log_privilege_use(
        session,
        architect,
        "architect",
        "reject_admin_action",
        location="/admin",
        details=f"action_id={action.id}; type={action.action_type}",
    )
    action.status = "rejected"
    action.reviewed_at = datetime.now(timezone.utc)
    session.add(action)
    session.commit()
    return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/admin/badges", response_class=HTMLResponse)
def admin_badges_page(request: Request, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    badges = session.exec(select(Badge).order_by(Badge.created_at)).all()
    return _tpl("admin_badges.html", request, session, admin, badges=badges)


@app.post("/admin/badges/create")
def create_badge(
    name: str = Form(..., min_length=1, max_length=100),
    description: str = Form("", max_length=300),
    icon_svg: str = Form(""),
    can_view_18plus: bool = Form(False),
    admin: User = Depends(get_current_admin), session: Session = Depends(get_session),
):
    badge = Badge(
        name=name.strip(),
        description=description.strip(),
        icon_svg=icon_svg.strip(),
        can_view_18plus=can_view_18plus,
    )
    session.add(badge)
    session.commit()
    return RedirectResponse(url="/admin/badges", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/admin/badges/{badge_id}/edit", response_class=HTMLResponse)
def edit_badge_page(badge_id: int, request: Request, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    badge = session.get(Badge, badge_id)
    if not badge:
        raise HTTPException(status_code=404)
    return _tpl("badge_edit.html", request, session, admin, badge=badge)


@app.post("/admin/badges/{badge_id}/edit")
def edit_badge(
    badge_id: int,
    name: str = Form(..., min_length=1, max_length=100),
    description: str = Form("", max_length=300),
    icon_svg: str = Form(""),
    can_view_18plus: bool = Form(False),
    admin: User = Depends(get_current_admin), session: Session = Depends(get_session),
):
    badge = session.get(Badge, badge_id)
    if not badge:
        raise HTTPException(status_code=404)
    badge.name = name.strip()
    badge.description = description.strip()
    badge.icon_svg = icon_svg.strip()
    badge.can_view_18plus = can_view_18plus
    badge.updated_at = datetime.now(timezone.utc)
    session.add(badge)
    session.commit()
    return RedirectResponse(url="/admin/badges", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/badges/{badge_id}/delete")
def delete_badge(badge_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    badge = session.get(Badge, badge_id)
    if not badge:
        raise HTTPException(status_code=404)
    for ub in session.exec(select(UserBadge).where(UserBadge.badge_id == badge_id)).all():
        session.delete(ub)
    session.delete(badge)
    session.commit()
    return RedirectResponse(url="/admin/badges", status_code=status.HTTP_303_SEE_OTHER)


def _validated_tag_fields(
    session: Session, tag_type: str, min_rank: int, required_badge_id_str: str | None
) -> tuple[str, int, int | None]:
    tt = (tag_type or "normal").strip().lower().replace("-", "_")
    legacy = {"illegal": "rank_gated", "18plus": "badge_gated"}
    tt = legacy.get(tt, tt)
    if tt not in ("normal", "rank_gated", "badge_gated"):
        raise HTTPException(status_code=400, detail="Invalid tag type")
    mr = max(1, min(12, int(min_rank))) if tt == "rank_gated" else 1
    req_badge: int | None = None
    if tt == "badge_gated":
        raw = (required_badge_id_str or "").strip()
        if not raw:
            raise HTTPException(status_code=400, detail="Badge-gated tags require a badge")
        req_badge = int(raw)
        if not session.get(Badge, req_badge):
            raise HTTPException(status_code=400, detail="Invalid badge")
    return tt, mr, req_badge


@app.get("/admin/tags", response_class=HTMLResponse)
def admin_tags_page(request: Request, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    tags = session.exec(select(Tag).order_by(Tag.created_at.desc())).all()
    corners = session.exec(select(Corner).order_by(Corner.created_at)).all()
    badges = session.exec(select(Badge).order_by(Badge.name)).all()
    tag_corner_map: dict[int, list[Corner]] = {}
    accesses = session.exec(select(TagCornerAccess)).all()
    for tag in tags:
        if tag.id is None:
            continue
        cids = [a.corner_id for a in accesses if a.tag_id == tag.id]
        tag_corner_map[tag.id] = [c for c in corners if c.id in cids]
    return _tpl(
        "admin_tags.html", request, session, admin,
        tags=tags, corners=corners, badges=badges, tag_corner_map=tag_corner_map,
    )


@app.post("/admin/tags/create")
def create_tag(
    name: str = Form(..., min_length=1, max_length=100),
    slug: str = Form(..., min_length=1, max_length=60),
    tag_type: str = Form("normal"),
    min_rank: int = Form(5),
    required_badge_id: str | None = Form(default=None),
    corner_ids: list[int] | None = Form(default=None),
    admin: User = Depends(get_current_admin),
    session: Session = Depends(get_session),
):
    slug = slug.strip().lower()
    if session.exec(select(Tag).where(Tag.slug == slug)).first():
        raise HTTPException(status_code=400, detail="Tag slug already exists")
    tt, mr, rb = _validated_tag_fields(session, tag_type, min_rank, required_badge_id)
    tag = Tag(name=name.strip(), slug=slug, tag_type=tt, min_rank=mr, required_badge_id=rb)
    session.add(tag)
    session.commit()
    if corner_ids:
        for cid in corner_ids:
            if session.get(Corner, cid):
                session.add(TagCornerAccess(tag_id=tag.id, corner_id=cid))
        session.commit()
    return RedirectResponse(url="/admin/tags", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/tags/{tag_id}/update")
def admin_update_tag(
    tag_id: int,
    tag_type: str = Form(...),
    min_rank: int = Form(1),
    required_badge_id: str | None = Form(default=None),
    admin: User = Depends(get_current_admin),
    session: Session = Depends(get_session),
):
    tag = session.get(Tag, tag_id)
    if not tag:
        raise HTTPException(status_code=404)
    tt, mr, rb = _validated_tag_fields(session, tag_type, min_rank, required_badge_id)
    tag.tag_type = tt
    tag.min_rank = mr
    tag.required_badge_id = rb
    session.add(tag)
    session.commit()
    return RedirectResponse(url="/admin/tags", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/tags/{tag_id}/delete")
def delete_tag(tag_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    tag = session.get(Tag, tag_id)
    if not tag:
        raise HTTPException(status_code=404)
    for row in session.exec(select(PostTag).where(PostTag.tag_id == tag_id)).all():
        session.delete(row)
    for row in session.exec(select(BookTag).where(BookTag.tag_id == tag_id)).all():
        session.delete(row)
    for row in session.exec(select(SuggestionTag).where(SuggestionTag.tag_id == tag_id)).all():
        session.delete(row)
    for row in session.exec(select(TagCornerAccess).where(TagCornerAccess.tag_id == tag_id)).all():
        session.delete(row)
    session.delete(tag)
    session.commit()
    return RedirectResponse(url="/admin/tags", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/badge/toggle/{user_id}")
def toggle_badge(
    user_id: int, badge_id: int = Form(...),
    admin: User = Depends(get_current_admin), session: Session = Depends(get_session),
):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404)
    if not session.get(Badge, badge_id):
        raise HTTPException(status_code=404)
    existing = session.exec(
        select(UserBadge).where(UserBadge.user_id == user_id, UserBadge.badge_id == badge_id)
    ).first()
    if existing:
        return do_or_queue(admin, "remove_badge", {"user_id": user_id, "badge_id": badge_id},
                           session, _exec_remove_badge, f"/user/{u.username}")
    return do_or_queue(admin, "assign_badge",
                       {"user_id": user_id, "badge_id": badge_id, "assigned_by": admin.id},
                       session, _exec_assign_badge, f"/user/{u.username}")


@app.get("/api/users/{user_id}/badges")
def api_user_badges(user_id: int, session: Session = Depends(get_session)):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404)
    rows = session.exec(select(UserBadge).where(UserBadge.user_id == user_id)).all()
    result = []
    for ub in rows:
        badge = session.get(Badge, ub.badge_id)
        if badge:
            result.append({"id": badge.id, "name": badge.name, "description": badge.description,
                           "assigned_at": ub.assigned_at.isoformat()})
    return result


@app.post("/api/login")
def api_login(username: str = Form(...), password: str = Form(...), session: Session = Depends(get_session)):
    u = session.exec(select(User).where(User.username == username)).first()
    if not u or not verify_password(password, u.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    if u.is_banned:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account banned")
    if not u.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account pending approval")
    return {"access_token": create_access_token(data={"sub": u.username}), "token_type": "bearer"}


@app.post("/api/register")
def api_register(
    username: str = Form(..., min_length=3, max_length=64),
    password: str = Form(..., min_length=6),
    bio: str = Form("", max_length=500),
    session: Session = Depends(get_session),
):
    if session.exec(select(User).where(User.username == username)).first():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already taken")
    session.add(User(username=username, password_hash=hash_password(password), bio=bio))
    session.commit()
    return {"message": "Registration submitted, pending admin approval"}


@app.get("/api/me")
def api_me(user: User = Depends(get_current_user)):
    return {"username": user.username, "bio": user.bio, "rank": user.rank, "rank_name": get_rank_name(user.rank)}
