"""Registry of the MTA's eight NYC subway GTFS-Realtime feeds.

No API key is required for these (bus feeds are different).
Each URL returns a protobuf-encoded snapshot of every train currently
running on those lines, refreshed roughly every 30 seconds.
"""

BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"

# feed_id -> (url suffix, routes carried by that feed)
FEEDS = {
    "numbered": ("",      ["1", "2", "3", "4", "5", "6", "7", "GS"]),
    "ace":      ("-ace",  ["A", "C", "E", "H", "FS"]),
    "bdfm":     ("-bdfm", ["B", "D", "F", "M"]),
    "g":        ("-g",    ["G"]),
    "jz":       ("-jz",   ["J", "Z"]),
    "nqrw":     ("-nqrw", ["N", "Q", "R", "W"]),
    "l":        ("-l",    ["L"]),
    "sir":      ("-si",   ["SI"]),
}


def feed_url(feed_id: str) -> str:
    """Return the full URL for a feed id, e.g. feed_url('ace')."""
    if feed_id not in FEEDS:
        raise KeyError(f"unknown feed {feed_id!r}; known: {sorted(FEEDS)}")
    suffix, _routes = FEEDS[feed_id]
    return BASE + suffix


if __name__ == "__main__":
    for fid in FEEDS:
        print(f"{fid:10s} {feed_url(fid)}")
