"""Post / reel records in the `apify/instagram-scraper` output shape.

Instagram hands us the same post in two different shapes depending on which
endpoint answered:

*private API* (``/api/v1/feed/user/...``)
    snake_case, ``media_type`` as an int, ``image_versions2.candidates``,
    ``carousel_media`` children.

*GraphQL* (``web_profile_info`` edges)
    ``__typename``, ``edge_media_to_caption``, ``display_url``,
    ``edge_sidecar_to_children``.

:func:`map_post` sniffs the shape and produces one canonical record either way.
"""

from __future__ import annotations

from typing import Any

from ..urls import post_url
from .common import (
    MEDIA_TYPE_NAMES,
    as_int,
    as_str,
    best_candidate,
    dig,
    extract_hashtags,
    extract_mentions,
    iso_timestamp,
    typename_to_type,
)

__all__ = ["map_post", "map_child_post", "post_sort_key"]


def map_post(
    raw: dict[str, Any],
    *,
    input_url: str | None = None,
    parent: dict[str, Any] | None = None,
    include_children: bool = True,
    data_source: str | None = None,
    preserve_unknowns: bool = False,
) -> dict[str, Any]:
    """Reshape one post into the Apify record.

    Args:
        raw: post JSON in either the private-API or GraphQL shape.
        input_url: echoed back as ``inputUrl``.
        parent: profile / hashtag / place context used for ``addParentData``.
        include_children: expand carousel children into ``childPosts``.
        data_source: overrides the ``dataSource`` tag.
    """
    if _is_graphql(raw):
        record = _from_graphql(raw, include_children=include_children)
    else:
        record = _from_private(raw, include_children=include_children)

    record["inputUrl"] = input_url
    if preserve_unknowns and raw.get("media_type") is None and not typename_to_type(raw.get("__typename")) and raw.get("is_video") is None:
        record["type"] = None
    if data_source:
        record["dataSource"] = data_source
    if parent:
        record.update(_parent_fields(parent))
    return record


def map_child_post(raw: dict[str, Any]) -> dict[str, Any]:
    """A carousel child, in the reduced shape Apify nests under ``childPosts``."""
    if _is_graphql(raw):
        child = _from_graphql(raw, include_children=False)
    else:
        child = _from_private(raw, include_children=False)
    keep = (
        "id", "type", "shortCode", "caption", "hashtags", "mentions", "url",
        "commentsCount", "dimensionsHeight", "dimensionsWidth", "displayUrl",
        "images", "videoUrl", "alt", "likesCount", "timestamp",
        "ownerId", "ownerUsername", "ownerFullName", "productType",
        "videoDuration", "taggedUsers",
    )
    return {key: child.get(key) for key in keep}


def post_sort_key(record: dict[str, Any]) -> str:
    """Newest-first ordering key for a mapped post."""
    return str(record.get("timestamp") or "")


# --------------------------------------------------------------------------- #
# Private ("mobile") API shape
# --------------------------------------------------------------------------- #

def _is_graphql(raw: dict[str, Any]) -> bool:
    # Modern Relay nodes wrap the private API media shape in XDTMediaDict.
    # __typename alone must not discard code, user, image_versions2 or metrics.
    if raw.get("media_type") is not None and "code" in raw:
        return False
    return "__typename" in raw or "edge_media_to_caption" in raw or "shortcode" in raw


def _first_known(*values: Any) -> Any:
    return next((value for value in values if value is not None), None)


def _optional_bool(value: Any) -> bool | None:
    return bool(value) if value is not None else None


def _from_private(raw: dict[str, Any], *, include_children: bool) -> dict[str, Any]:
    media_type = as_int(raw.get("media_type"), 1)
    shortcode = as_str(raw.get("code"))
    caption_text = dig(raw, "caption", "text")
    owner = raw.get("user") or raw.get("owner") or {}
    carousel = raw.get("carousel_media") or []
    location = _location_from_private(raw)
    clips = raw.get("clips_metadata") or {}

    display_url = best_candidate(raw.get("image_versions2"))
    video_url = best_candidate(raw.get("video_versions"))
    # `images` is one entry per media in the post: a single URL for a plain
    # photo or video, one per slide for a carousel -- not every resolution
    # variant Instagram happens to publish.
    if carousel:
        images = [
            url for child in carousel
            if (url := best_candidate(child.get("image_versions2")))
        ]
    else:
        images = [display_url] if display_url else []

    view_count = as_int(raw.get("view_count"))
    play_count = as_int(raw.get("play_count"))
    ig_play_count = as_int(raw.get("ig_play_count"))

    record: dict[str, Any] = {
        "id": as_str(raw.get("pk") or raw.get("id")),
        "type": MEDIA_TYPE_NAMES.get(media_type, "Image"),
        "shortCode": shortcode,
        "caption": caption_text,
        "hashtags": extract_hashtags(caption_text),
        "mentions": extract_mentions(caption_text),
        "url": post_url(shortcode) if shortcode else None,
        "commentsCount": as_int(raw.get("comment_count")),
        "firstComment": None,
        "latestComments": [],
        "dimensionsHeight": as_int(raw.get("original_height")),
        "dimensionsWidth": as_int(raw.get("original_width")),
        "displayUrl": display_url,
        "images": images,
        "videoUrl": video_url,
        "alt": as_str(raw.get("accessibility_caption")),
        "likesCount": as_int(raw.get("like_count")),
        "videoViewCount": view_count if view_count is not None else ig_play_count,
        "videoPlayCount": play_count if play_count is not None else ig_play_count,
        "timestamp": iso_timestamp(raw.get("taken_at")),
        "childPosts": [],
        "ownerFullName": as_str(owner.get("full_name")),
        "ownerUsername": as_str(owner.get("username")),
        "ownerId": as_str(owner.get("pk") or owner.get("id")),
        "productType": as_str(raw.get("product_type")),
        "videoDuration": raw.get("video_duration"),
        "isSponsored": _optional_bool(raw.get("is_paid_partnership")),
        "isPinned": _optional_bool(raw.get("timeline_pinned_user_ids")),
        "isCommentsDisabled": _optional_bool(raw.get("comments_disabled")),
        "taggedUsers": _tagged_from_private(raw),
        "coauthorProducers": [
            {"id": as_str(c.get("pk") or c.get("id")),
             "username": as_str(c.get("username")),
             "is_verified": bool(c.get("is_verified"))}
            for c in raw.get("coauthor_producers") or []
            if isinstance(c, dict)
        ],
        "musicInfo": _music_from_private(clips, raw.get("music_metadata")),
        "locationName": location.get("name"),
        "locationId": location.get("id"),
        "dataSource": "api",
    }

    preview = raw.get("preview_comments") or raw.get("comments") or []
    if preview:
        record["latestComments"] = [_preview_comment(c) for c in preview if isinstance(c, dict)]
        record["firstComment"] = dig(preview[0], "text")

    if include_children and carousel:
        record["childPosts"] = [map_child_post(child) for child in carousel
                                if isinstance(child, dict)]
    return record


def _location_from_private(raw: dict[str, Any]) -> dict[str, Any]:
    location = raw.get("location")
    if not location:
        locations = raw.get("locations") or []
        location = locations[0] if locations else None
    if not isinstance(location, dict):
        return {}
    return {
        "id": as_str(location.get("pk") or location.get("id")),
        "name": as_str(location.get("name")),
    }


def _tagged_from_private(raw: dict[str, Any]) -> list[dict[str, Any]]:
    usertags = dig(raw, "usertags", "in") or []
    out: list[dict[str, Any]] = []
    for tag in usertags:
        user = (tag or {}).get("user") or {}
        if not user:
            continue
        out.append({
            "id": as_str(user.get("pk") or user.get("id")),
            "username": as_str(user.get("username")),
            "full_name": as_str(user.get("full_name")),
            "is_verified": bool(user.get("is_verified")),
            "profile_pic_url": as_str(user.get("profile_pic_url")),
        })
    return out


def _music_from_private(clips: dict[str, Any], metadata: Any) -> dict[str, Any] | None:
    """Apify's ``musicInfo`` block (``artist_name``, ``song_name``,
    ``uses_original_audio``, ``should_mute_audio``,
    ``should_mute_audio_reason``, ``audio_id`` -- verified against a live
    Apify reels run on 17 September 2026) plus one extension: ``audio_url``,
    the track's own ``progressive_download_url`` (licensed music or original
    sound) which the private API carries next to the title; ``None`` when
    Instagram withholds it (muted or region-locked audio)."""
    info = (clips or {}).get("music_info") or {}
    asset = info.get("music_asset_info") or {}
    consumption = info.get("music_consumption_info") or {}
    original = (clips or {}).get("original_sound_info") or {}
    if not asset and not original and not metadata:
        return None
    if asset:
        return {
            "artist_name": as_str(asset.get("display_artist")),
            "song_name": as_str(asset.get("title")),
            "uses_original_audio": False,
            "should_mute_audio": bool(info.get("should_mute_audio")
                                      or consumption.get("should_mute_audio")),
            "should_mute_audio_reason": as_str(consumption.get("should_mute_audio_reason")
                                               or info.get("should_mute_audio_reason")) or "",
            "audio_id": as_str(asset.get("audio_cluster_id")),
            "audio_url": as_str(asset.get("progressive_download_url")
                                or asset.get("fast_start_progressive_download_url")),
        }
    if original:
        return {
            "artist_name": as_str(dig(original, "ig_artist", "username")),
            "song_name": as_str(original.get("original_audio_title")),
            "uses_original_audio": True,
            "should_mute_audio": bool(original.get("should_mute_audio")),
            "should_mute_audio_reason": as_str(original.get("should_mute_audio_reason")) or "",
            "audio_id": as_str(original.get("audio_asset_id")),
            "audio_url": as_str(original.get("progressive_download_url")
                                or original.get("fast_start_progressive_download_url")),
        }
    return None


def _preview_comment(raw: dict[str, Any]) -> dict[str, Any]:
    user = raw.get("user") or {}
    return {
        "id": as_str(raw.get("pk") or raw.get("id")),
        "text": as_str(raw.get("text")),
        "ownerUsername": as_str(user.get("username")),
        "ownerProfilePicUrl": as_str(user.get("profile_pic_url")),
        "timestamp": iso_timestamp(raw.get("created_at")),
        "likesCount": as_int(raw.get("comment_like_count")),
        "repliesCount": as_int(raw.get("child_comment_count"), 0),
    }


# --------------------------------------------------------------------------- #
# GraphQL shape
# --------------------------------------------------------------------------- #

def _from_graphql(raw: dict[str, Any], *, include_children: bool) -> dict[str, Any]:
    shortcode = as_str(raw.get("shortcode") or raw.get("code"))
    caption_text = _graphql_caption(raw)
    owner = raw.get("owner") or {}
    is_video = bool(raw.get("is_video"))
    type_name = typename_to_type(raw.get("__typename")) or ("Video" if is_video else "Image")

    children_edges = dig(raw, "edge_sidecar_to_children", "edges") or []
    children = [edge.get("node") for edge in children_edges
                if isinstance(edge, dict) and isinstance(edge.get("node"), dict)]
    if children:
        type_name = "Sidecar"

    images = [raw.get("display_url")] if raw.get("display_url") else []
    if children:
        images = [child.get("display_url") for child in children if child.get("display_url")]

    record: dict[str, Any] = {
        "id": as_str(raw.get("id")),
        "type": type_name,
        "shortCode": shortcode,
        "caption": caption_text,
        "hashtags": extract_hashtags(caption_text),
        "mentions": extract_mentions(caption_text),
        "url": post_url(shortcode) if shortcode else None,
        "commentsCount": as_int(_first_known(
            dig(raw, "edge_media_to_comment", "count"),
            dig(raw, "edge_media_preview_comment", "count"),
            dig(raw, "edge_media_to_parent_comment", "count"))),
        "firstComment": None,
        "latestComments": [],
        "dimensionsHeight": as_int(dig(raw, "dimensions", "height")),
        "dimensionsWidth": as_int(dig(raw, "dimensions", "width")),
        "displayUrl": as_str(raw.get("display_url")),
        "images": [url for url in images if url],
        "videoUrl": as_str(raw.get("video_url")),
        "alt": as_str(raw.get("accessibility_caption")),
        "likesCount": as_int(_first_known(
            dig(raw, "edge_media_preview_like", "count"),
            dig(raw, "edge_liked_by", "count"))),
        "videoViewCount": as_int(raw.get("video_view_count")),
        "videoPlayCount": as_int(raw.get("video_play_count")),
        "timestamp": iso_timestamp(raw.get("taken_at_timestamp")),
        "childPosts": [],
        "ownerFullName": as_str(owner.get("full_name")),
        "ownerUsername": as_str(owner.get("username")),
        "ownerId": as_str(owner.get("id")),
        "productType": as_str(raw.get("product_type")),
        "videoDuration": raw.get("video_duration"),
        "isSponsored": _optional_bool(raw.get("is_ad")),
        "isPinned": _optional_bool(raw.get("pinned_for_users")),
        "isCommentsDisabled": _optional_bool(raw.get("comments_disabled")),
        "taggedUsers": _tagged_from_graphql(raw),
        "coauthorProducers": [
            {"id": as_str(c.get("id")), "username": as_str(c.get("username")),
             "is_verified": bool(c.get("is_verified"))}
            for c in raw.get("coauthor_producers") or [] if isinstance(c, dict)
        ],
        "musicInfo": None,
        "locationName": as_str(dig(raw, "location", "name")),
        "locationId": as_str(dig(raw, "location", "id")),
        "dataSource": "api",
    }

    comment_edges = (dig(raw, "edge_media_to_parent_comment", "edges")
                     or dig(raw, "edge_media_to_comment", "edges") or [])
    latest = []
    for edge in comment_edges:
        node = (edge or {}).get("node") or {}
        if not node:
            continue
        latest.append({
            "id": as_str(node.get("id")),
            "text": as_str(node.get("text")),
            "ownerUsername": as_str(dig(node, "owner", "username")),
            "ownerProfilePicUrl": as_str(dig(node, "owner", "profile_pic_url")),
            "timestamp": iso_timestamp(node.get("created_at")),
            "likesCount": as_int(dig(node, "edge_liked_by", "count")),
            "repliesCount": as_int(dig(node, "edge_threaded_comments", "count"), 0),
        })
    if latest:
        record["latestComments"] = latest
        record["firstComment"] = latest[0]["text"]

    if include_children and children:
        record["childPosts"] = [map_child_post(child) for child in children]
    return record


def _graphql_caption(raw: dict[str, Any]) -> str | None:
    edges = dig(raw, "edge_media_to_caption", "edges") or []
    for edge in edges:
        text = dig(edge, "node", "text")
        if text:
            return str(text)
    return as_str(raw.get("caption")) if isinstance(raw.get("caption"), str) else None


def _tagged_from_graphql(raw: dict[str, Any]) -> list[dict[str, Any]]:
    edges = dig(raw, "edge_media_to_tagged_user", "edges") or []
    out: list[dict[str, Any]] = []
    for edge in edges:
        user = dig(edge, "node", "user") or {}
        if not user:
            continue
        out.append({
            "id": as_str(user.get("id")),
            "username": as_str(user.get("username")),
            "full_name": as_str(user.get("full_name")),
            "is_verified": bool(user.get("is_verified")),
            "profile_pic_url": as_str(user.get("profile_pic_url")),
        })
    return out


# --------------------------------------------------------------------------- #
# addParentData
# --------------------------------------------------------------------------- #

def _parent_fields(parent: dict[str, Any]) -> dict[str, Any]:
    """The `addParentData` block describing where a post came from."""
    kind = parent.get("type")
    if kind == "hashtag":
        return {"fromHashtag": parent.get("name"), "dataSourceType": "hashtag"}
    if kind == "place":
        return {"fromPlace": parent.get("name"), "fromPlaceId": parent.get("id"),
                "dataSourceType": "place"}
    if kind == "profile":
        return {"fromProfile": parent.get("username"), "dataSourceType": "profile"}
    return {}
