RANKS = {
    1: "Newcomer",
    2: "Initiate",
    3: "Insider",
    4: "Contributor",
    5: "Trusted",
    6: "Veteran",
    7: "Elite",
    8: "Sentinel",
    9: "Warden",
    10: "Council",
    11: "Admin",
    12: "Architect",
}

PRIVILEGES = {
    "comment": 3,
    "unlimited_comments": 7,
    "reply": 7,
    "admin": 11,
    "architect": 12,
}


def user_rank(user) -> int:
    try:
        r = int(getattr(user, "rank", 1))
    except (TypeError, ValueError):
        return 1
    return r


def get_rank_name(level) -> str:
    try:
        lvl = int(level)
    except (TypeError, ValueError):
        return "Unknown"
    return RANKS.get(lvl, "Unknown")


def has_privilege(user, privilege: str) -> bool:
    min_rank = PRIVILEGES.get(privilege)
    if min_rank is None:
        return False
    return user_rank(user) >= min_rank
