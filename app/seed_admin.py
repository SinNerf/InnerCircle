import sys

from sqlmodel import Session, select

from app.auth import hash_password
from app.config import ADMIN_USERNAME, ADMIN_PASSWORD
from app.models import Corner, Title, User, create_db_and_tables, engine

BUILTIN_CORNERS = [
    {"slug": "library", "name": "Library", "icon": "\U0001F4DA", "description": "Read and publish books of knowledge.", "template_type": "library"},
    {"slug": "suggestions", "name": "Suggestions", "icon": "\U0001F4AC", "description": "Suggest changes and features. Vote on what matters.", "template_type": "suggestions"},
]


def seed() -> None:
    create_db_and_tables()
    with Session(engine) as session:
        existing = session.exec(select(User).where(User.username == ADMIN_USERNAME)).first()
        if existing:
            print(f"Error: Admin user '{ADMIN_USERNAME}' already exists.", file=sys.stderr)
            sys.exit(1)
        admin = User(
            username=ADMIN_USERNAME,
            password_hash=hash_password(ADMIN_PASSWORD),
            bio="System administrator",
            is_active=True,
            rank=12,
        )
        session.add(admin)
        session.flush()
        session.add(Title(user_id=admin.id, text="System Architect"))
        for c in BUILTIN_CORNERS:
            if not session.exec(select(Corner).where(Corner.slug == c["slug"])).first():
                session.add(Corner(**c))
        session.commit()
        print(f"Admin user '{ADMIN_USERNAME}' created (rank 12).")


if __name__ == "__main__":
    seed()
