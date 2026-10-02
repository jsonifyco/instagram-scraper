"""Instagram shortcode <-> media id conversion.

An Instagram media has two identifiers that mean the same thing:

* the numeric primary key (``pk``), e.g. ``3967213292204992434``
* the shortcode used in URLs (``code``), e.g. ``DcOX3hWFiey``

The shortcode is the base-64 representation of the pk using Instagram's own
alphabet.  The mapping is deterministic and offline, which lets us hit
``/api/v1/media/{pk}/...`` endpoints when all we were given is a ``/p/{code}/``
URL -- no extra request needed.

Verified against live data: ``DcOX3hWFiey`` <-> ``3967213292204992434``.
"""

from __future__ import annotations

__all__ = ["ALPHABET", "shortcode_to_media_id", "media_id_to_shortcode", "split_media_id"]

ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
_INDEX = {ch: i for i, ch in enumerate(ALPHABET)}


def shortcode_to_media_id(shortcode: str) -> int:
    """Convert ``DcOX3hWFiey`` to ``3967213292204992434``.

    Raises:
        ValueError: if the shortcode contains characters outside the alphabet.
    """
    if not shortcode:
        raise ValueError("empty shortcode")
    value = 0
    for ch in shortcode:
        try:
            value = value * 64 + _INDEX[ch]
        except KeyError:
            raise ValueError(f"invalid character {ch!r} in shortcode {shortcode!r}") from None
    return value


def media_id_to_shortcode(media_id: int | str) -> str:
    """Convert ``3967213292204992434`` to ``DcOX3hWFiey``.

    Accepts the composite ``"<pk>_<owner_id>"`` form returned by the private
    API as well as a bare pk.
    """
    pk = split_media_id(media_id)
    if pk < 0:
        raise ValueError(f"negative media id: {media_id!r}")
    if pk == 0:
        return ALPHABET[0]
    out: list[str] = []
    while pk > 0:
        pk, rem = divmod(pk, 64)
        out.append(ALPHABET[rem])
    return "".join(reversed(out))


def split_media_id(media_id: int | str) -> int:
    """Return the numeric pk from ``"<pk>_<owner_id>"`` or from a plain value."""
    if isinstance(media_id, int):
        return media_id
    text = str(media_id).strip()
    if "_" in text:
        text = text.split("_", 1)[0]
    try:
        return int(text)
    except ValueError:
        raise ValueError(f"not a media id: {media_id!r}") from None
