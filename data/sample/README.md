# A sample of real observations

Three of the eight feeds — `ace` (A, C, E, H, FS), `g` and `l` — over one
continuous 150-minute run, at the full thirty-second cadence. 903 snapshots,
about 5 MB compressed.

It is committed so that `python run.py demo` works on a clone with no cloud
account, no credentials and no waiting, and so CI can rebuild the whole
pipeline and check it on every push. A repository whose central claim is a
measurement, and which cannot demonstrate that measurement without you first
running a collector for two hours, is a repository nobody will check.

**Why a subset of feeds rather than a subset of time.** The inference reads an
arrival out of the difference between consecutive snapshots, so thinning the
snapshots degrades the thing being demonstrated: at a 90-second cadence the
last sighting of a train can be a minute and a half before it actually arrived.
Dropping whole feeds instead leaves every remaining observation exactly as it
was collected. Three feeds rather than one because the watermark rule in
`int_inferred_arrivals` is per feed, and a single-feed sample would never
exercise it.

**Why these three.** `ace` is a busy feed carrying five routes, so it produces
enough arrivals at one platform for an excess wait to mean something. `g` and
`l` are the two quietest, and between them they add two more routes for about
a megabyte.

The run was overnight — 00:43 to 03:13 New York time — so headways are twenty
minutes rather than the four you would see at rush hour. That is real service,
not thin data.
