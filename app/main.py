import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
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
    AdminAction, Badge, Book, Comment, Corner, Notification, Post, PostVote,
    Suggestion, Title, User, UserBadge, Vote, create_db_and_tables, engine, get_session,
)
from app.ranks import RANKS, get_rank_name, has_privilege

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.globals["get_rank_name"] = get_rank_name
templates.env.globals["has_privilege"] = has_privilege
templates.env.globals["RANKS"] = RANKS

COOKIE = dict(key="access_token", httponly=True, samesite="none", secure=True)
RESERVED_SLUGS = {"library", "suggestions"}

BUILTIN_CORNERS = [
    {"slug": "library", "name": "Library", "icon": "\U0001f4da",
     "description": "Read and publish books of knowledge.", "template_type": "library"},
    {"slug": "suggestions", "name": "Suggestions", "icon": "\U0001f4ac",
     "description": "Suggest changes and features. Vote on what matters.", "template_type": "suggestions"},
]


@asynccontextmanager
async def lifespan(_app: FastAPI):
    create_db_and_tables()
    from sqlalchemy import text as sa_text
    with engine.connect() as conn:
        try:
            conn.execute(sa_text("ALTER TABLE title ADD COLUMN description VARCHAR(300) DEFAULT ''"))
            conn.commit()
        except Exception:
            pass
    with Session(engine) as s:
        for c in BUILTIN_CORNERS:
            if not s.exec(select(Corner).where(Corner.slug == c["slug"])).first():
                s.add(Corner(**c))
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
            Notification.user_id == user.id, Notification.is_read == False  # noqa: E712
        )).all())
    resp = templates.TemplateResponse(name, {"request": request, "user": user, "unread_count": uc, **ctx})
    if status_code != 200:
        resp.status_code = status_code
    return resp


def _suggestion_stats(session: Session, user_id: int) -> dict:
    subs = session.exec(select(Suggestion).where(Suggestion.author_id == user_id)).all()
    s_ids = [s.id for s in subs]
    up = down = 0
    if s_ids:
        votes = session.exec(select(Vote).where(Vote.suggestion_id.in_(s_ids))).all()  # type: ignore[attr-defined]
        up = sum(1 for v in votes if v.value == 1)
        down = sum(1 for v in votes if v.value == -1)
    approved = sum(1 for s in subs if s.is_approved)
    return {"suggestions": subs, "s_count": len(subs), "s_up": up, "s_down": down, "s_approved": approved}


def _corner_stats(session: Session, user_id: int) -> list[dict]:
    corners = session.exec(select(Corner).where(
        Corner.slug.notin_(["library", "suggestions"])  # type: ignore[attr-defined]
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


def _get_comments(session: Session, target_type: str, target_id: int):
    raw = session.exec(select(Comment).where(
        Comment.target_type == target_type, Comment.target_id == target_id,
        Comment.parent_id == None,  # noqa: E711
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
        Comment.target_id == target_id, Comment.parent_id == None,  # noqa: E711
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
        session.add(u)


def _exec_unban_user(session, p):
    u = session.get(User, p["user_id"])
    if u:
        u.is_banned = False
        session.add(u)


def _exec_approve_suggestion(session, p):
    s = session.get(Suggestion, p["suggestion_id"])
    if s:
        s.is_approved = True
        session.add(s)


def _exec_delete_book(session, p):
    b = session.get(Book, p["book_id"])
    if b:
        for c in session.exec(select(Comment).where(Comment.target_type == "book", Comment.target_id == b.id)).all():
            for r in session.exec(select(Comment).where(Comment.parent_id == c.id)).all():
                session.delete(r)
            session.delete(c)
        session.delete(b)


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


ACTION_DISPATCH = {
    "approve_user": _exec_approve_user, "decline_user": _exec_decline_user,
    "ban_user": _exec_ban_user, "unban_user": _exec_unban_user,
    "approve_suggestion": _exec_approve_suggestion,
    "delete_book": _exec_delete_book, "delete_suggestion": _exec_delete_suggestion,
    "add_title": _exec_add_title, "remove_title": _exec_remove_title, "set_rank": _exec_set_rank,
    "send_notification": _exec_send_notification, "delete_comment": _exec_delete_comment,
    "create_corner": _exec_create_corner,
    "delete_post": _exec_delete_post, "approve_post": _exec_approve_post,
    "assign_badge": _exec_assign_badge, "remove_badge": _exec_remove_badge,
}


def _action_summary(action_type: str, payload: dict) -> str:
    fns = {
        "approve_user": lambda p: f"Approve user #{p.get('user_id')}",
        "decline_user": lambda p: f"Decline user #{p.get('user_id')}",
        "ban_user": lambda p: f"Ban user #{p.get('user_id')}",
        "unban_user": lambda p: f"Unban user #{p.get('user_id')}",
        "approve_suggestion": lambda p: f"Approve suggestion #{p.get('suggestion_id')}",
        "delete_book": lambda p: f"Delete book #{p.get('book_id')}",
        "delete_suggestion": lambda p: f"Delete suggestion #{p.get('suggestion_id')}",
        "add_title": lambda p: f"Title \"{p.get('text')}\" \u2192 user #{p.get('user_id')}",
        "remove_title": lambda p: f"Remove title #{p.get('title_id')}",
        "set_rank": lambda p: f"Set rank {p.get('rank')} for user #{p.get('user_id')}",
        "send_notification": lambda p: f"Notify user #{p.get('user_id')}",
        "delete_comment": lambda p: f"Delete comment #{p.get('comment_id')}",
        "create_corner": lambda p: f"Create corner \"{p.get('name')}\"",
        "delete_post": lambda p: f"Delete post #{p.get('post_id')}",
        "approve_post": lambda p: f"Approve post #{p.get('post_id')}",
        "assign_badge": lambda p: f"Assign badge #{p.get('badge_id')} \u2192 user #{p.get('user_id')}",
        "remove_badge": lambda p: f"Remove badge #{p.get('badge_id')} from user #{p.get('user_id')}",
    }
    return fns.get(action_type, lambda p: action_type)(payload)


def do_or_queue(admin, action_type, payload, session, execute_fn, redirect_url):
    if admin.rank >= 12:
        execute_fn(session, payload)
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
    channels = session.exec(select(Corner).order_by(Corner.created_at)).all()
    search_result = search_error = None
    if q:
        found = session.exec(
            select(User).where(User.username == q, User.is_active == True)  # noqa: E712
        ).first()
        if found and not found.is_banned:
            search_result = found
        else:
            search_error = f'No user found with exact name "{q}"'
    return _tpl("corners.html", request, session, user,
                channels=channels, q=q or "", search_result=search_result, search_error=search_error)


@app.get("/corners/library", response_class=HTMLResponse)
def library(request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    books = session.exec(select(Book).order_by(Book.created_at.desc())).all()
    authors: dict[int, User] = {}
    for b in books:
        if b.author_id not in authors:
            a = session.get(User, b.author_id)
            if a:
                authors[b.author_id] = a
    return _tpl("library.html", request, session, user, books=books, authors=authors)


@app.get("/corners/library/write", response_class=HTMLResponse)
def book_write_page(request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    return _tpl("book_write.html", request, session, user)


@app.post("/corners/library/write", response_class=HTMLResponse)
def book_write(
    request: Request,
    title: str = Form(..., min_length=1, max_length=200),
    content: str = Form(..., min_length=1),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    title, content = title.strip(), content.strip()
    if not title or not content:
        return _tpl("book_write.html", request, session, user, error="Title and content are required.")
    book = Book(title=title, content=content, author_id=user.id)
    session.add(book)
    session.commit()
    return RedirectResponse(url=f"/corners/library/{book.id}", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/corners/library/{book_id}", response_class=HTMLResponse)
def book_detail(book_id: int, request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    book = session.get(Book, book_id)
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    if book.author_id != user.id:
        book.views += 1
        session.add(book)
        session.commit()
        session.refresh(book)
    author = session.get(User, book.author_id)
    comments = _get_comments(session, "book", book_id)
    can_cmt = _can_comment(session, user, "book", book_id)
    return _tpl("book_detail.html", request, session, user,
                book=book, author=author, comments=comments, can_comment=can_cmt)


@app.post("/corners/library/{book_id}/delete")
def book_delete(book_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    if not session.get(Book, book_id):
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "delete_book", {"book_id": book_id}, session, _exec_delete_book, "/corners/library")


@app.get("/corners/suggestions", response_class=HTMLResponse)
def suggestions_list(
    request: Request, sort: str = Query("new"), tab: str = Query("open"),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    is_approved = tab == "approved"
    items = session.exec(select(Suggestion).where(Suggestion.is_approved == is_approved)).all()
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
                items=scored, authors=authors, sort=sort, tab=tab)


@app.get("/corners/suggestions/write", response_class=HTMLResponse)
def suggestion_write_page(request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    return _tpl("suggestion_write.html", request, session, user)


@app.post("/corners/suggestions/write", response_class=HTMLResponse)
def suggestion_write(
    request: Request,
    title: str = Form(..., min_length=1, max_length=200),
    body: str = Form(..., min_length=1),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    title, body = title.strip(), body.strip()
    if not title or not body:
        return _tpl("suggestion_write.html", request, session, user, error="Title and description are required.")
    s = Suggestion(title=title, body=body, author_id=user.id)
    session.add(s)
    session.commit()
    return RedirectResponse(url=f"/corners/suggestions/{s.id}", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/corners/suggestions/{sid}", response_class=HTMLResponse)
def suggestion_detail(sid: int, request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    s = session.get(Suggestion, sid)
    if not s:
        raise HTTPException(status_code=404)
    author = session.get(User, s.author_id)
    votes = session.exec(select(Vote).where(Vote.suggestion_id == s.id)).all()
    up = sum(1 for v in votes if v.value == 1)
    down = sum(1 for v in votes if v.value == -1)
    my_vote = next((v.value for v in votes if v.user_id == user.id), 0)
    comments = _get_comments(session, "suggestion", sid)
    can_cmt = _can_comment(session, user, "suggestion", sid)
    return _tpl("suggestion_detail.html", request, session, user,
                s=s, author=author, up=up, down=down, my_vote=my_vote,
                comments=comments, can_comment=can_cmt)


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
    sort: str = Query("new"), tab: str = Query("open"),
    user: User = Depends(get_current_user), session: Session = Depends(get_session),
):
    corner = session.exec(select(Corner).where(Corner.slug == slug)).first()
    if not corner:
        raise HTTPException(status_code=404)

    if corner.template_type == "suggestions":
        is_approved = tab == "approved"
        posts = session.exec(
            select(Post).where(Post.corner_id == corner.id, Post.is_approved == is_approved)
        ).all()
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
                     corner=corner, items=scored, authors=authors, sort=sort, tab=tab)
    else:
        posts = session.exec(
            select(Post).where(Post.corner_id == corner.id).order_by(Post.created_at.desc())
        ).all()
        authors: dict[int, User] = {}
        for p in posts:
            if p.author_id not in authors:
                a = session.get(User, p.author_id)
                if a:
                    authors[p.author_id] = a
        return _tpl("generic_list.html", request, session, user,
                     corner=corner, posts=posts, authors=authors)


@app.get("/corners/{slug}/write", response_class=HTMLResponse)
def generic_write_page(slug: str, request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    corner = session.exec(select(Corner).where(Corner.slug == slug)).first()
    if not corner:
        raise HTTPException(status_code=404)
    return _tpl("generic_write.html", request, session, user, corner=corner)


@app.post("/corners/{slug}/write", response_class=HTMLResponse)
def generic_write(
    slug: str, request: Request,
    title: str = Form(..., min_length=1, max_length=200),
    content: str = Form(..., min_length=1),
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

    return _tpl("generic_detail.html", request, session, user,
                corner=corner, post=post, author=author,
                comments=comments, can_comment=can_cmt, **extra)


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
            Comment.target_id == target_id, Comment.parent_id == None,  # noqa: E711
        )).first()
        if existing:
            raise HTTPException(status_code=400, detail="Already commented on this item")

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
    total_views = sum(b.views for b in books)
    ss = _suggestion_stats(session, profile.id)
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
    user_badge_ids = set()
    for ub in ub_rows:
        b = session.get(Badge, ub.badge_id)
        if b:
            profile_badges.append(b)
            user_badge_ids.add(b.id)
    return _tpl("user_profile.html", request, session, user,
                profile=profile, books=books, total_views=total_views, titles=titles,
                corner_stats=cstats, all_saved_titles=all_saved_titles,
                all_badges=all_badges, profile_badges=profile_badges,
                user_badge_ids=user_badge_ids, **ss)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    books = session.exec(select(Book).where(Book.author_id == user.id).order_by(Book.created_at.desc())).all()
    total_views = sum(b.views for b in books)
    ss = _suggestion_stats(session, user.id)
    titles = session.exec(select(Title).where(Title.user_id == user.id).order_by(Title.created_at)).all()
    cstats = _corner_stats(session, user.id)
    ub_rows = session.exec(select(UserBadge).where(UserBadge.user_id == user.id)).all()
    my_badges = []
    for ub in ub_rows:
        b = session.get(Badge, ub.badge_id)
        if b:
            my_badges.append(b)
    return _tpl("dashboard.html", request, session, user,
                books=books, total_views=total_views, titles=titles,
                corner_stats=cstats, my_badges=my_badges, **ss)


@app.get("/notifications", response_class=HTMLResponse)
def notifications_page(request: Request, user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    notes = session.exec(
        select(Notification).where(Notification.user_id == user.id).order_by(Notification.created_at.desc())
    ).all()
    return _tpl("notifications.html", request, session, user, notifications=notes)


@app.post("/notifications/read")
def notifications_mark_read(user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    for n in session.exec(select(Notification).where(
        Notification.user_id == user.id, Notification.is_read == False  # noqa: E712
    )).all():
        n.is_read = True
        session.add(n)
    session.commit()
    return RedirectResponse(url="/notifications", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/admin", response_class=HTMLResponse)
def admin_panel(
    request: Request, queued: int = Query(0),
    admin: User = Depends(get_current_admin), session: Session = Depends(get_session),
):
    pending = session.exec(select(User).where(User.is_active == False, User.rank < 11)).all()  # noqa: E712
    banned = session.exec(select(User).where(User.is_banned == True)).all()  # noqa: E712
    pending_suggestions = session.exec(select(Suggestion).where(Suggestion.is_approved == False)).all()  # noqa: E712
    s_authors: dict[int, User] = {}
    for s in pending_suggestions:
        if s.author_id not in s_authors:
            a = session.get(User, s.author_id)
            if a:
                s_authors[s.author_id] = a

    pending_actions = []
    a_admins: dict[int, User] = {}
    action_summaries: dict[int, str] = {}
    if admin.rank >= 12:
        raw = session.exec(select(AdminAction).where(AdminAction.status == "pending").order_by(AdminAction.created_at)).all()
        for act in raw:
            pending_actions.append(act)
            try:
                payload = json.loads(act.payload)
            except Exception:
                payload = {}
            action_summaries[act.id] = _action_summary(act.action_type, payload)
            if act.admin_id not in a_admins:
                adm = session.get(User, act.admin_id)
                if adm:
                    a_admins[act.admin_id] = adm

    all_corners = session.exec(select(Corner).order_by(Corner.created_at)).all()

    return _tpl("admin.html", request, session, admin, queued=bool(queued),
                pending_users=pending, banned_users=banned,
                pending_suggestions=pending_suggestions, s_authors=s_authors,
                pending_actions=pending_actions, a_admins=a_admins,
                action_summaries=action_summaries, corners=all_corners)


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users(request: Request, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    users = session.exec(select(User).where(User.is_active == True).order_by(User.created_at.desc())).all()  # noqa: E712
    return _tpl("admin_users.html", request, session, admin, users=users)


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
def ban_user(user_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404)
    if u.rank >= 11:
        raise HTTPException(status_code=400, detail="Cannot ban an admin")
    return do_or_queue(admin, "ban_user", {"user_id": user_id}, session, _exec_ban_user, f"/user/{u.username}")


@app.post("/admin/unban/{user_id}")
def unban_user(user_id: int, admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "unban_user", {"user_id": user_id}, session, _exec_unban_user, "/admin")


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
    if rank >= 12 and admin.rank < 12:
        raise HTTPException(status_code=403, detail="Only Architect can assign rank 12")
    return do_or_queue(admin, "set_rank", {"user_id": user_id, "rank": rank}, session, _exec_set_rank, f"/user/{u.username}")


@app.post("/admin/notify/{user_id}")
def send_notification(user_id: int, message: str = Form(..., min_length=1, max_length=500), admin: User = Depends(get_current_admin), session: Session = Depends(get_session)):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404)
    return do_or_queue(admin, "send_notification", {"user_id": user_id, "message": message.strip()}, session, _exec_send_notification, f"/user/{u.username}")


@app.post("/admin/corner")
def create_corner(
    name: str = Form(..., min_length=1, max_length=100),
    slug: str = Form(..., min_length=1, max_length=60),
    icon: str = Form("", max_length=8),
    description: str = Form("", max_length=300),
    template_type: str = Form("library"),
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
               "description": description.strip(), "template_type": template_type}
    return do_or_queue(admin, "create_corner", payload, session, _exec_create_corner, "/admin")


@app.post("/admin/actions/{action_id}/approve")
def approve_action(action_id: int, architect: User = Depends(get_current_architect), session: Session = Depends(get_session)):
    action = session.get(AdminAction, action_id)
    if not action or action.status != "pending":
        raise HTTPException(status_code=404)
    handler = ACTION_DISPATCH.get(action.action_type)
    if handler:
        handler(session, json.loads(action.payload))
    action.status = "approved"
    action.reviewed_at = datetime.now(timezone.utc)
    session.add(action)
    session.commit()
    return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/actions/{action_id}/reject")
def reject_action(action_id: int, architect: User = Depends(get_current_architect), session: Session = Depends(get_session)):
    action = session.get(AdminAction, action_id)
    if not action or action.status != "pending":
        raise HTTPException(status_code=404)
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
    admin: User = Depends(get_current_admin), session: Session = Depends(get_session),
):
    badge = Badge(name=name.strip(), description=description.strip(), icon_svg=icon_svg.strip())
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
    admin: User = Depends(get_current_admin), session: Session = Depends(get_session),
):
    badge = session.get(Badge, badge_id)
    if not badge:
        raise HTTPException(status_code=404)
    badge.name = name.strip()
    badge.description = description.strip()
    badge.icon_svg = icon_svg.strip()
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
