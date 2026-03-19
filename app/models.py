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
    rank: int = Field(default=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Book(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    title: str = Field(max_length=200)
    content: str
    author_id: int = Field(foreign_key="user.id")
    views: int = Field(default=0)
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
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


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
