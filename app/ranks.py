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


def get_rank_name(level: int) -> str:
    return RANKS.get(level, "Unknown")


def has_privilege(user, privilege: str) -> bool:
    min_rank = PRIVILEGES.get(privilege)
    if min_rank is None:
        return False
    return user.rank >= min_rank
