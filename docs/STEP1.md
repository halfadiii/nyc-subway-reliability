# Step 1 — Local setup and first contact with the feed

Goal: fetch one MTA realtime feed on your own machine, decode it, and watch a
prediction vanish. ~30 minutes. No cloud account needed yet, no money spent.

---

## 1. Make the folder and the virtual environment

```bash
mkdir mta-reliability && cd mta-reliability
git init
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
```

**What a virtualenv is:** a private box of Python packages for this project only.
Without it, every project shares one global set of packages, and two projects that
need different versions of the same library will fight. Your terminal prompt shows
`(.venv)` when it's active. You re-activate it every time you open a new terminal.

```bash
pip install requests protobuf grpcio-tools
```

- `requests` — makes HTTP calls (visits the URL and gets bytes back)
- `protobuf` — the runtime for decoding the MTA's binary format
- `grpcio-tools` — ships `protoc`, the compiler that turns a `.proto` file into
  Python code. You need it once, in step 3.

Freeze what you installed so the versions are reproducible:

```bash
pip freeze > requirements.txt
```

---

## 2. Create the ingest folder

```bash
mkdir -p ingest/proto/com/google/transit/realtime
cd ingest
```

That nested path looks absurd. It's not optional: the MTA's extension file contains
the line `import "com/google/transit/realtime/gtfs-realtime.proto"`, and the compiler
resolves that literally against your folder structure. Wrong folders, no compile.

---

## 3. Download and compile the protobuf definitions

**What's going on here.** The MTA doesn't send you text. It sends a compact binary
blob — smaller and faster than JSON, but unreadable without a decoder. The decoder is
described by a `.proto` file: a schema listing every field and its type. You run a
compiler over that schema and it generates Python classes that know how to unpack the
blob.

Two files are needed. The **base** GTFS-Realtime spec (a standard used by transit
agencies worldwide), and the **NYCT extension** — extra fields the MTA bolted on for
things the standard doesn't cover, like which track a train will use and whether it's
northbound. Skip the extension and you silently lose those fields.

```bash
curl -o proto/com/google/transit/realtime/gtfs-realtime.proto \
  https://raw.githubusercontent.com/google/transit/master/gtfs-realtime/proto/gtfs-realtime.proto

curl -o proto/com/google/transit/realtime/gtfs-realtime-NYCT.proto \
  https://raw.githubusercontent.com/OneBusAway/onebusaway-gtfs-realtime-api/master/src/main/proto/com/google/transit/realtime/gtfs-realtime-NYCT.proto
```

Open the NYCT one in a text editor and skim it. It's 142 lines of well-commented
English, and it explains the direction codes and track numbering better than any
tutorial. Five minutes well spent.

Now compile both:

```bash
python -m grpc_tools.protoc -I=proto --python_out=. \
  proto/com/google/transit/realtime/gtfs-realtime.proto \
  proto/com/google/transit/realtime/gtfs-realtime-NYCT.proto

# make the generated folders importable as Python packages
touch com/__init__.py com/google/__init__.py \
      com/google/transit/__init__.py com/google/transit/realtime/__init__.py
```

You now have `com/google/transit/realtime/gtfs_realtime_pb2.py` and
`..._NYCT_pb2.py`. The `_pb2` suffix means "generated from protobuf." Never edit
these by hand — they're build output. Add a note in your README saying how to
regenerate them.

> **Trap I hit so you don't:** there's a PyPI package called
> `gtfs-realtime-bindings` that gives you the base bindings pre-compiled. Don't
> install it. It registers the same protobuf schema under a *different* module path,
> and importing both causes a duplicate-registration crash. Compile both files
> yourself, as above, and use only those.

---

## 4. Add the two Python files

Drop `feeds.py` and `explore.py` into `ingest/` (both provided).

`feeds.py` is a lookup table: eight feed IDs → eight URLs. The MTA splits the subway
into eight feeds grouped by line colour, so a full picture means eight fetches.
No API key is needed for subway realtime.

---

## 5. Run it

```bash
python explore.py ace
```

Expect roughly this:

```
Got 84,231 bytes of protobuf.

==============================================================
FEED HEADER   generated 18:04:08 NY  (16s ago)
==============================================================

142 entities:
    71 TripUpdate      (a train + its predicted stops)
    71 VehiclePosition (where a train is right now)

==============================================================
ONE TRAIN, IN DETAIL
==============================================================
  trip_id      064750_A..N00R
  route_id     A
  train_id     0A 647+ FAR/207          <- NYCT extension
  direction    NORTH

  NEXT 5 STOPS (of 21 remaining):

     stop_id    arrive    depart   track
        A02N  18:05:20  18:05:50   4
        A03N  18:08:20  18:08:50   4
        ...
```

Try other feeds: `python explore.py numbered`, `python explore.py l`,
`python explore.py g`. The G is quiet — good for seeing a small feed. `numbered` is
the busiest.

### Reading the output

- **`(16s ago)`** — the feed is genuinely live. If that number climbs past a few
  minutes, the MTA is having a problem. Later, this becomes an automated freshness test.
- **TripUpdate ≈ VehiclePosition count** — every running train appears twice, once as
  "here's my plan" and once as "here's where I am." Two ways to measure the same
  reality, which is what makes the cross-check in Step 8 possible.
- **`stop_id: A02N`** — station `A02`, `N` for northbound. Every physical platform is
  its own stop. Strip the letter to get the station a human would name.
- **`trip_id: 064750_A..N00R`** — `064750` is the origin departure time in hundredths
  of a minute after midnight (÷100 = 647.5 minutes = 10:47am), `A` is the route, `N`
  is the direction. Three useful columns encoded in a string; you'll parse it in dbt.
- **`arrive` and `depart` on every row** — the gap is the dwell time at the platform.

---

## 6. The exercise that matters

```bash
python watch.py ace 20
```

Ten minutes. It picks one train and prints its predictions for the next six stops,
once every 30 seconds. Watch the table:

```
poll feed time      A02N      A03N      A05N      A06N
   1     05:36     06:29     08:31     10:32     12:19
   2     06:06      --       07:03     08:50     10:47
   3     06:36      --       07:27     09:19     11:16
   4     07:06      --        --       07:40     09:53
```

Two things are happening at once.

**Predictions get revised.** A05N was forecast for 10:32, then 08:50, then 09:19.
The MTA is continuously re-estimating, and the estimate wobbles as it converges.

**Then a column turns to `--`.** That's not missing data. That's the train having
*passed* the stop, so the MTA drops it from the remaining-stops list. The last real
value before the dashes is your observed arrival.

The script prints those derived arrivals at the end. Look at them and register what
just happened: you measured when trains arrived at stations, and **no column in the
feed ever contained that information.** You inferred it from a row disappearing.

That's the project. Step 8 is this same logic in SQL, running over every train in the
system at once, but the idea is what you just watched.

---

## 7. Commit

```bash
cd ..
cat > .gitignore <<'EOF'
.venv/
__pycache__/
*.pyc
.env
*.pb
*.pb.gz
EOF

git add .
git commit -m "Step 1: decode MTA GTFS-RT feed, observe prediction lifecycle"
```

Commit at the end of every step. Small commits with real messages are themselves
evidence — reviewers do read commit history, and a repo with one commit called
"initial commit" looks like it was assembled the night before.

---

## Done when

- [ ] `python explore.py ace` prints a real train with real predicted times
- [ ] You ran `watch.py` and personally saw a stop disappear
- [ ] You can explain, out loud, why the feed header timestamp matters more than your
      own clock
- [ ] It's committed to git

---

## If something breaks

**`ModuleNotFoundError: No module named 'com'`** — run from inside `ingest/`, and check
the four `__init__.py` files exist.

**`TypeError: Couldn't build proto file` / duplicate symbol** — you have
`gtfs-realtime-bindings` installed. `pip uninstall gtfs-realtime-bindings`.

**`DecodeError`** — you got an HTML error page instead of protobuf. Print
`resp.content[:200]` to see what actually came back.

**Empty feed / no trips** — some feeds genuinely go quiet at 3am. Try `numbered`.

**Connection errors** — the MTA endpoint has occasional hiccups. Retrying is normal,
and Step 4 will make the poller resilient to it.

---

## Where this is heading

Step 2 downloads the *scheduled* timetable, so you have something to compare reality
against. Step 3 sets up the cloud. Step 4 turns `explore.py` into a poller that saves
instead of prints. Step 5 puts it on a server so it runs while you sleep — and from
that moment, history starts accumulating.

`explore.py` and `watch.py` stay in the repo. They're not throwaway: reviewers who
want to understand your pipeline can run them and see the raw data for themselves.
