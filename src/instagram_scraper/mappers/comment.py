"""Comment records in the Apify output shape."""

from __future__ import annotations

from typing import Any

from .common import as_int, as_str, dig, iso_timestamp

__all__ = ["map_comment", "map_reply"]


def map_comment(
    raw: dict[str, Any],
    *,
    post_url: str | None = None,
    input_url: str | None = None,
    replies: list[dict[str, Any]] | None = None,
    parent: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Reshape one comment from either the private API or the GraphQL shape."""
    if isinstance(raw.get("node"), dict):
        raw = raw["node"]
    owner = raw.get("user") or raw.get("owner") or {}
    comment_id = as_str(raw.get("pk") or raw.get("id"))
    created = raw.get("created_at") or raw.get("created_at_utc") or raw.get("taken_at")

    nested = replies
    if nested is None:
        nested = [
            map_reply(child, post_url=post_url, parent_id=comment_id)
            for child in (raw.get("preview_child_comments")
                          or dig(raw, "edge_threaded_comments", "edges") or [])
            if isinstance(child, dict)
        ]

    record: dict[str, Any] = {
        "inputUrl": input_url,
        "id": comment_id,
        "postUrl": post_url,
        "commentUrl": f"{post_url}c/{comment_id}/" if post_url and comment_id else None,
        "text": str(raw["text"]) if raw.get("text") is not None else None,
        "ownerUsername": as_str(owner.get("username")),
        "ownerProfilePicUrl": as_str(owner.get("profile_pic_url")),
        "timestamp": iso_timestamp(created),
        "repliesCount": as_int(
            _first(raw.get("child_comment_count"),
                   dig(raw, "edge_threaded_comments", "count"))),
        "replies": nested,
        "likesCount": as_int(
            _first(raw.get("comment_like_count"), raw.get("like_count"),
                   dig(raw, "edge_liked_by", "count"))),
        "owner": _owner_block(owner),
        "dataSource": raw.get("dataSource", "api"),
    }
    if raw.get("media") is not None:
        record["media"] = raw["media"]
    if raw.get("giphy_media_info") is not None:
        record["giphyMediaInfo"] = raw["giphy_media_info"]
    if "is_pinned" in raw:
        record["isPinned"] = bool(raw["is_pinned"])
    parent_id = raw.get("_parent_comment_id") or raw.get("parent_comment_id")
    if parent_id:
        record["parentCommentId"] = as_str(parent_id)
    if parent:
        record["postOwnerUsername"] = parent.get("ownerUsername")
        record["postShortCode"] = parent.get("shortCode")
    return record


def map_reply(
    raw: dict[str, Any],
    *,
    post_url: str | None = None,
    parent_id: str | None = None,
) -> dict[str, Any]:
    """A threaded reply, in the reduced shape nested under ``replies``."""
    node = raw.get("node") if isinstance(raw.get("node"), dict) else raw
    record = map_comment(node, post_url=post_url, replies=[])
    record.pop("inputUrl", None)
    record.pop("replies", None)
    record.pop("repliesCount", None)
    record["parentCommentId"] = parent_id
    return record


def _first(*values: Any) -> Any:
    return next((v for v in values if v is not None), None)


def _owner_block(owner: dict[str, Any]) -> dict[str, Any] | None:
    """The nested `owner` object Apify emits alongside the flat fields."""
    if not owner:
        return None
    return {
        "id": as_str(owner.get("pk") or owner.get("id")),
        "fbid_v2": as_str(owner.get("fbid_v2")),
        "username": as_str(owner.get("username")),
        "full_name": as_str(owner.get("full_name")),
        "profile_pic_url": as_str(owner.get("profile_pic_url")),
        "profile_pic_id": as_str(owner.get("profile_pic_id")),
        "is_verified": bool(owner["is_verified"]) if owner.get("is_verified") is not None else None,
        "is_private": bool(owner["is_private"]) if owner.get("is_private") is not None else None,
        "is_mentionable": bool(owner["is_mentionable"]) if owner.get("is_mentionable") is not None else None,
        "latest_reel_media": as_int(owner.get("latest_reel_media")),
    }
