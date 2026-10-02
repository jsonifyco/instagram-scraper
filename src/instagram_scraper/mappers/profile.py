"""Profile / hashtag / place `details` records in the Apify output shape."""

from __future__ import annotations

from typing import Any

from ..urls import profile_url
from .common import as_int, as_str, dig, first
from .post import map_post

__all__ = ["map_profile", "map_hashtag", "map_place", "map_search_hit"]


def map_profile(
    raw: dict[str, Any],
    *,
    input_url: str | None = None,
    latest_posts: list[dict[str, Any]] | None = None,
    add_statistics: bool = False,
) -> dict[str, Any]:
    """Reshape a ``web_profile_info`` user object.

    Args:
        raw: the ``data.user`` object from `web_profile_info`.
        input_url: echoed back as ``inputUrl``.
        latest_posts: already-mapped posts to nest under ``latestPosts``.
        add_statistics: include the extended `addProfileStatistics` block.
    """
    username = as_str(raw.get("username"))
    bio_links = raw.get("bio_links") or []
    external_urls = [
        link.get("url") for link in bio_links
        if isinstance(link, dict) and link.get("url")
    ]
    external_url = as_str(raw.get("external_url")) or (external_urls[0] if external_urls else None)

    record: dict[str, Any] = {
        "inputUrl": input_url,
        "id": as_str(raw.get("id") or raw.get("pk")),
        "username": username,
        "url": profile_url(username) if username else None,
        "fullName": as_str(raw.get("full_name")),
        "biography": as_str(raw.get("biography")),
        "externalUrls": external_urls,
        "externalUrl": external_url,
        "externalUrlShimmed": as_str(raw.get("external_url_linkshimmed")),
        # Counts default to None, not 0: sparse by-id and fallback profile
        # responses can omit them. Reporting "0 posts" when unknown would
        # silently lose the distinction from a genuine empty profile.
        # `first` picks the first non-None value, so a genuine 0 is kept
        # rather than falling through the way `or` would.
        "followersCount": as_int(first([dig(raw, "edge_followed_by", "count"),
                                        raw.get("follower_count")])),
        "followsCount": as_int(first([dig(raw, "edge_follow", "count"),
                                      raw.get("following_count")])),
        "hasChannel": bool(raw.get("has_channel")),
        "highlightReelCount": as_int(raw.get("highlight_reel_count")),
        # the web app's profile response names it `is_business` (2026-09-29)
        "isBusinessAccount": bool(raw.get("is_business_account", raw.get("is_business"))),
        "joinedRecently": bool(raw.get("is_joined_recently")),
        "businessCategoryName": as_str(raw.get("business_category_name")),
        "private": bool(raw.get("is_private")),
        "verified": bool(raw.get("is_verified")),
        "profilePicUrl": as_str(raw.get("profile_pic_url")),
        "profilePicUrlHD": as_str(raw.get("profile_pic_url_hd")
                                  or dig(raw, "hd_profile_pic_url_info", "url")
                                  or raw.get("profile_pic_url")),
        "igtvVideoCount": as_int(dig(raw, "edge_felix_video_timeline", "count")),
        "postsCount": as_int(first([dig(raw, "edge_owner_to_timeline_media", "count"),
                                    raw.get("media_count")])),
        "fbid": as_str(raw.get("fbid") or raw.get("eimu_id") or raw.get("fbid_v2")),
        "latestPosts": latest_posts or [],
        "dataSource": raw.get("dataSource", "api"),
    }

    related = dig(raw, "edge_related_profiles", "edges") or []
    if related:
        record["relatedProfiles"] = [
            {
                "id": as_str(dig(edge, "node", "id")),
                "username": as_str(dig(edge, "node", "username")),
                "full_name": as_str(dig(edge, "node", "full_name")),
                "is_private": bool(dig(edge, "node", "is_private")),
                "is_verified": bool(dig(edge, "node", "is_verified")),
                "profile_pic_url": as_str(dig(edge, "node", "profile_pic_url")),
            }
            for edge in related
        ]

    if add_statistics:
        record.update({
            "isProfessionalAccount": bool(raw.get("is_professional_account")),
            "accountType": as_str(raw.get("category_enum") or raw.get("account_type")),
            "categoryName": as_str(raw.get("category_name")
                                   or raw.get("overall_category_name")),
            "businessEmail": as_str(raw.get("business_email")),
            "businessPhoneNumber": as_str(raw.get("business_phone_number")),
            "businessAddress": raw.get("business_address_json"),
            "pronouns": raw.get("pronouns") or [],
            "hasClips": bool(raw.get("has_clips")),
            "hasGuides": bool(raw.get("has_guides")),
            "hasArEffects": bool(raw.get("has_ar_effects")),
            "hideLikeAndViewCounts": bool(raw.get("hide_like_and_view_counts")),
            "isVerifiedByMv4b": bool(raw.get("is_verified_by_mv4b")),
            "isEmbedsDisabled": bool(raw.get("is_embeds_disabled")),
            "shouldShowPublicContacts": bool(raw.get("should_show_public_contacts")),
            "transparencyLabel": as_str(raw.get("transparency_label")),
            "aiAgentType": as_str(raw.get("ai_agent_type")),
        })

    return record


def map_profile_with_timeline(
    raw: dict[str, Any],
    *,
    input_url: str | None = None,
    max_posts: int = 12,
    add_statistics: bool = False,
) -> dict[str, Any]:
    """Map a profile and inline the timeline edges `web_profile_info` returned."""
    edges = dig(raw, "edge_owner_to_timeline_media", "edges") or []
    latest = []
    for edge in edges[:max_posts]:
        node = (edge or {}).get("node")
        if isinstance(node, dict):
            latest.append(map_post(node, include_children=False))
    return map_profile(
        raw, input_url=input_url, latest_posts=latest, add_statistics=add_statistics
    )


def map_hashtag(
    raw: dict[str, Any],
    *,
    input_url: str | None = None,
    name: str | None = None,
    top_posts: list[dict[str, Any]] | None = None,
    latest_posts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Reshape a hashtag header record."""
    tag_name = as_str(raw.get("name")) or name
    count = as_int(
        raw.get("media_count")
        or dig(raw, "edge_hashtag_to_media", "count")
        or raw.get("postsCount"),
        0,
    )
    return {
        "inputUrl": input_url,
        "id": as_str(raw.get("id")),
        "name": tag_name,
        "url": f"https://www.instagram.com/explore/tags/{tag_name}/" if tag_name else None,
        "postsCount": count,
        # `profile_pic_url` is the API spelling; `profilePicUrl` is what the
        # page-header and AI fallbacks produce.
        "profilePicUrl": as_str(raw.get("profile_pic_url") or raw.get("profilePicUrl")),
        "topPostsOnly": bool(raw.get("is_top_media_only")),
        "topPosts": top_posts or [],
        "latestPosts": latest_posts or [],
        "dataSource": raw.get("dataSource", "api"),
    }


def map_place(
    raw: dict[str, Any],
    *,
    input_url: str | None = None,
    location_id: str | None = None,
    top_posts: list[dict[str, Any]] | None = None,
    latest_posts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Reshape a place / location header record."""
    location = raw.get("location") if isinstance(raw.get("location"), dict) else raw
    place_id = as_str(location.get("id") or location.get("pk")) or location_id
    return {
        "inputUrl": input_url,
        "id": place_id,
        "name": as_str(location.get("name")),
        "url": f"https://www.instagram.com/explore/locations/{place_id}/" if place_id else None,
        "slug": as_str(location.get("slug")),
        "postsCount": as_int(location.get("media_count")
                             or dig(raw, "edge_location_to_media", "count")
                             or raw.get("postsCount"), 0),
        "lat": location.get("lat"),
        "lng": location.get("lng"),
        "address": as_str(location.get("address_json") or location.get("address")
                          or location.get("location_address")),
        "city": as_str(location.get("city") or location.get("location_city")),
        "phone": as_str(location.get("phone")),
        "website": as_str(location.get("website")),
        # `profile_pic_url` is the API spelling; `profilePicUrl` is what the
        # page-header and AI fallbacks produce.
        "profilePicUrl": as_str(location.get("profile_pic_url")
                                or location.get("profilePicUrl")),
        "topPosts": top_posts or [],
        "latestPosts": latest_posts or [],
        "dataSource": raw.get("dataSource", "api"),
    }


def map_search_hit(raw: dict[str, Any], kind: str) -> dict[str, Any]:
    """Reshape one entry from a top-search response."""
    if kind == "user":
        user = raw.get("user") or raw
        username = as_str(user.get("username"))
        return {
            "type": "user",
            "id": as_str(user.get("pk") or user.get("id")),
            "username": username,
            "fullName": as_str(user.get("full_name")),
            "url": profile_url(username) if username else None,
            "verified": bool(user.get("is_verified")),
            "private": bool(user.get("is_private")),
            "profilePicUrl": as_str(user.get("profile_pic_url")),
            "followersCount": as_int(user.get("follower_count")),
        }
    if kind == "hashtag":
        tag = raw.get("hashtag") or raw
        name = as_str(tag.get("name"))
        return {
            "type": "hashtag",
            "id": as_str(tag.get("id")),
            "name": name,
            "url": f"https://www.instagram.com/explore/tags/{name}/" if name else None,
            "postsCount": as_int(tag.get("media_count"), 0),
        }
    place = raw.get("place") or raw
    location = place.get("location") or place
    place_id = as_str(location.get("pk") or location.get("id"))
    return {
        "type": "place",
        "id": place_id,
        "name": as_str(location.get("name") or place.get("title")),
        "url": f"https://www.instagram.com/explore/locations/{place_id}/" if place_id else None,
        "slug": as_str(place.get("slug")),
        "address": as_str(location.get("address")),
        "city": as_str(location.get("city")),
        "lat": location.get("lat"),
        "lng": location.get("lng"),
        "subtitle": as_str(place.get("subtitle")),
    }
