from datetime import datetime, timezone

from sqlmodel import Field, SQLModel, Session, create_engine

from app.config import DATABASE_URL

engine = create_engine(DATABASE_URL, echo=False, connect_args={"check_same_thread": False})


class User(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True, max_length=64)
    password_hash: str
    bio: str = Field(default="", max_length=500)
    is_active: bool = Field(default=False)
    is_banned: bool = Field(default=False)
    ban_reason: str = Field(default="", max_length=500)
    is_deleted: bool = Field(default=False)
    deleted_reason: str = Field(default="", max_length=500)
    deleted_at: datetime | None = Field(default=None)
    rank: int = Field(default=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Book(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    title: str = Field(max_length=200)
    content: str
    author_id: int = Field(foreign_key="user.id")
    views: int = Field(default=0)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Suggestion(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    title: str = Field(max_length=200)
    body: str
    author_id: int = Field(foreign_key="user.id")
    is_approved: bool = Field(default=False)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Vote(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    suggestion_id: int = Field(foreign_key="suggestion.id")
    user_id: int = Field(foreign_key="user.id")
    value: int = Field(default=0)


class Title(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.id")
    text: str = Field(max_length=100)
    description: str = Field(default="", max_length=300)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Badge(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(max_length=100, unique=True)
    description: str = Field(default="", max_length=300)
    icon_svg: str = Field(default="")
    can_view_18plus: bool = Field(default=False)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class UserBadge(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.id")
    badge_id: int = Field(foreign_key="badge.id")
    assigned_by: int = Field(foreign_key="user.id")
    assigned_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Tag(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(max_length=100)
    slug: str = Field(unique=True, max_length=60)
    tag_type: str = Field(default="normal", max_length=20)
    min_rank: int = Field(default=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TagCornerAccess(SQLModel, table=True):
    tag_id: int = Field(foreign_key="tag.id", primary_key=True)
    corner_id: int = Field(foreign_key="corner.id", primary_key=True)


class PostTag(SQLModel, table=True):
    post_id: int = Field(foreign_key="post.id", primary_key=True)
    tag_id: int = Field(foreign_key="tag.id", primary_key=True)


class BookTag(SQLModel, table=True):
    book_id: int = Field(foreign_key="book.id", primary_key=True)
    tag_id: int = Field(foreign_key="tag.id", primary_key=True)


class SuggestionTag(SQLModel, table=True):
    suggestion_id: int = Field(foreign_key="suggestion.id", primary_key=True)
    tag_id: int = Field(foreign_key="tag.id", primary_key=True)


class Notification(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.id")
    message: str
    is_read: bool = Field(default=False)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Comment(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    content: str = Field(max_length=2000)
    author_id: int = Field(foreign_key="user.id")
    target_type: str = Field(max_length=20)
    target_id: int
    parent_id: int | None = Field(default=None, foreign_key="comment.id")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AdminAction(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    admin_id: int = Field(foreign_key="user.id")
    action_type: str = Field(max_length=50)
    payload: str = Field(default="{}")
    status: str = Field(default="pending", max_length=20)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    reviewed_at: datetime | None = Field(default=None)


class Corner(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(max_length=100)
    slug: str = Field(unique=True, max_length=60)
    icon: str = Field(default="", max_length=8)
    description: str = Field(default="", max_length=300)
    template_type: str = Field(default="library", max_length=20)
    is_external: bool = Field(default=False)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class UserCornerSubscription(SQLModel, table=True):
    user_id: int = Field(foreign_key="user.id", primary_key=True)
    corner_id: int = Field(foreign_key="corner.id", primary_key=True)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Follow(SQLModel, table=True):
    follower_id: int = Field(foreign_key="user.id", primary_key=True)
    following_id: int = Field(foreign_key="user.id", primary_key=True)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PrivilegeLog(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    actor_id: int = Field(foreign_key="user.id")
    privilege: str = Field(max_length=50)
    action: str = Field(max_length=120)
    location: str = Field(default="", max_length=300)
    details: str = Field(default="", max_length=1000)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class BookPage(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    book_id: int = Field(foreign_key="book.id")
    page_number: int = Field(default=1)
    content: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Post(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    corner_id: int = Field(foreign_key="corner.id")
    title: str = Field(max_length=200)
    content: str
    author_id: int = Field(foreign_key="user.id")
    views: int = Field(default=0)
    is_approved: bool = Field(default=False)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PostVote(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    post_id: int = Field(foreign_key="post.id")
    user_id: int = Field(foreign_key="user.id")
    value: int = Field(default=0)


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session
