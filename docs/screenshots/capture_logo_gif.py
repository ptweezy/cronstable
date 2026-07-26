"""Capture the header brand (self-balancing pendulum + wordmark) as seamless loops.

Unlike the dashboard PNGs this needs NO running daemon: the page is served
straight off the working tree, its own rAF loop is unhooked, and the mark's
cart/double-pendulum simulation is stepped frame-by-frame at an exact 50 fps
(sim dt = 20 ms, the GIF decoder floor), so the recording is deterministic
and the replay is true-speed physics, not an approximation of it.

The loop tells the product story with the real controller, not keyframes:

  * the l stands balanced and DEAD STILL: the ambient breeze of the live page
    is switched off, so between events the LQR pins the glyph at exact
    upright and nothing moves at all;
  * the only disturbances are the theme hops: each one lands as before (the
    ink SWITCHES clean to the next theme and holds while the glyph splits
    into a chromatic misregistration plus a digital slice tear), and each one
    physically KNOCKS the pendulum (sim.poke) with a random direction and a
    random magnitude (the direction and the shoulder/elbow split come from
    the sim's own seeded RNG; the magnitude from a per-seed schedule drawn
    here), so every stumble in the loop is different and the controller
    rides each one out;
  * the fourth hop is the big one: it cuts the signal (sim.setConnected(false))
    and its knock topples the now-dead motor, so the l collapses out of the
    word and swings; seconds later the signal returns and the cross-entropy
    planner threads the swing-up into a verified catch; the word heals,
    exactly as the live dashboard does when the daemon drops and comes back;
  * the last hop returns to the base theme and the LQR settles the state to
    (sub-pixel) exact upright, which is also frame 0's state, so the loop
    seam is invisible.

Stillness is what buys the loop its length: whenever the sim is balanced,
calm, and the swing trail has drained, the capture stops screenshotting and
simply EXTENDS the previous frame's on-screen duration while the physics
steps on underneath (batched, render-free) until the next hop. A quiet
stretch therefore costs zero frames and zero bytes, which is how a 3-minute
loop stays the size of the old 40-second one. (The page's CRT flicker, a
100 ms opacity dip every 3.2 s, is pinned off during capture so the frozen
stretches are honest; opacity 1 is that animation's state 97% of the time.)

The pendulum is chaotic, so the choreography is SEED-SEARCHED: each candidate
seed gets its own poke schedule (same generator, seeded by the sim seed), and
the same event timeline is replayed headlessly (physics only, no screenshots)
through the page's own `CronstableLogo.Sim`. The first seed whose performance
survives every knock, keeps clear of the end stops, lands the recovery inside
CATCH_WINDOW, holds the catch, and converges to a sub-pixel-calm final state
is used for BOTH variants (physics is palette-independent, so the dark and
light loops show the identical performance).

Each variant is written twice: a 24-bit `<name>.webp` (the primary — no
256-palette banding on the glow, the glitch's saturated ghosts, or the swing
trail) and a 256-colour `<name>.gif` twin for clients that don't render
animated WebP, the same webp-primary / gif-fallback convention as
build_reel.py.

Needs playwright (+ its Chromium) and Pillow. Files land in shots/.
"""
import http.server
import math
import random
import threading
from functools import partial
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageChops, ImageOps
from playwright.sync_api import sync_playwright

WEB = Path(__file__).resolve().parents[2] / "cronstable" / "web"
OUT = Path(__file__).parent / "shots"
PORT = 8123

FRAME_MS = 20         # 50 fps: the ~20ms GIF decoder floor, and an exact sim dt
FRAMES = 9000         # 180.0 s loop (stillness is frame-free, see docstring)
PAD = 8               # css px around the brand box (the glow tails; the swing
                      # itself never leaves the '#mark svg' rect the clip spans)
SCALE = 3             # device pixels per css px. The README shows the loop at
                      # its intrinsic size, so this IS the zoom: 3 renders the
                      # wordmark half again larger than the dashboard PNGs' 2.

# WebP is the primary (24-bit colour -> no 256-palette banding on the glow or
# the glitch's saturated ghosts); the GIF is a 256-colour twin for clients that
# don't render animated WebP. Mirrors docs/screenshots/build_reel.py.
WEBP_LOSSLESS = True  # True = pixel-perfect (larger); False = high-q lossy
WEBP_QUALITY = 92     # lossy mode: fidelity. lossless mode: compression effort (->100)
WEBP_METHOD = 6       # libwebp effort (0-6): best ratio

# ---- choreography (frame numbers at 20 ms/frame) -----------------------------
# Ink hops (each one glitches AND knocks): base -> six glitched stops -> base.
# Deliberately off-beat spacing so the glitches don't read as a metronome; the
# first hop comes early so a fresh page load isn't 20 s of stillness.
HOP_FRAMES = [420, 1720, 2680, 4150, 6050, 7180, 8300]
# Every hop pokes the pendulum (rad/s on both joints). The sim's own seeded
# RNG picks the direction and the shoulder/elbow split; the magnitude is drawn
# per hop from the ranges below by events_map(seed). The glyph mount's track
# is sharply asymmetric (the cart has only ~0.5 m of run-out on the "e" side),
# so anything much past ~0.6 usually tips a BALANCED mark clean over (probed
# empirically; the seed search still verifies survival per seed). The
# signal-cut hop draws hotter: its motor is dead, toppling is the whole point,
# and the extra energy just varies the collapse.
POKE_SWAY = (0.28, 0.62)       # the six survivable stumbles
POKE_DROP = (0.40, 0.90)       # the knock that rides the disconnect
DISCONNECT_AT = HOP_FRAMES[3]  # fourth hop cuts the motor: collapse + offline swing
RECONNECT_AT = 4310            # 3.2 s later the signal returns: swing-up begins
CATCH_WINDOW = (4550, 5300)    # the verified catch must land in here (91-106 s)
CATCH_LOOSE = (RECONNECT_AT + 100, 5450)   # widened once before giving up
SIM_SEEDS = range(1, 97)       # candidate sim seeds for the search
SEARCH_CHUNK = 8               # seeds per in-page batch (progress + early exit)

# ---- stillness gate ----------------------------------------------------------
# A frame is only frozen (duration-extended, not screenshotted) once the sim
# is balanced, the swing trail has fully drained, and the state has stayed
# under these bounds for FREEZE_HOLD consecutive frames. At the mount's scale
# (~168 device px/m, ~0.6 m reach) the bounds keep any residual motion, and
# therefore the freeze cut itself and the loop seam, under ~0.5 device px.
FREEZE_CALM = 0.004   # |th1| + |th2| + 0.3*(|w1| + |w2|), wrapped rad
FREEZE_X = 0.0015     # cart offset from the l's cell (m)
FREEZE_XD = 0.008     # cart speed (m/s)
FREEZE_HOLD = 25      # consecutive calm frames (0.5 s) before freezing

# ---- glitch (tuned live in docs/screenshots/logo-glitch-tuner) --------------
# The ink SWITCHES clean to the target theme and holds (no dim); on top of it the
# glyph is torn into a chromatic misregistration — offset colour
# silhouettes that jitter per frame — plus a digital slice tear.
GLITCH_MS = 200       # length of each glitch/switch (clamped to whole frames, ~10f)
GLITCH_SPLIT = 4.5    # px of chromatic offset for the colour ghosts at peak
GLITCH_TEAR = 3       # horizontal slice shears at the peak (0 = none)
GLITCH_MOVE = 2.0     # css px the wordmark itself eases sideways & back (peak, *SCALE)
SEED = 20260708       # fixes the per-frame glitch jitter AND the poke schedule

# the saturated silhouette inks the glyph splits into during a glitch. Screened
# on over a dark theme (they glow) and multiplied under a paper theme (they read
# as CMYK misregistration), so each variant gets its own tuned set.
GHOSTS_DARK = [(255, 46, 104), (54, 214, 255), (60, 255, 150), (188, 108, 255)]
GHOSTS_PAPER = [(210, 24, 66), (0, 132, 194), (22, 150, 74), (124, 48, 196)]

# The colour JOURNEY each loop takes: the base theme, six glitched stops (the
# three-ink cast revisited, never the same ink twice in a row), and a final
# hop home so the base spans the loop seam. The fourth stop doubles as the
# signal-loss event (see the choreography block above): it wears "modern",
# the neutral beat, so the amber recovering bobs and the accent swing trail
# read against quiet ink.
# NB: "standard" is intentionally left out — for the logo it's a near-white,
# no-glow ink indistinguishable from "modern", so two adjacent white stops would
# look like a glitch that changes nothing. One neutral beat (modern) is plenty.
# (basename — each variant writes both <name>.webp and <name>.gif)
VARIANTS = [
    ("logo-balance", "carolina",
     ["amber", "green", "amber", "modern", "green", "amber"]),
    ("logo-balance-light", "carolina-light",
     ["amber-light", "green-light", "amber-light", "modern-light",
      "green-light", "amber-light"]),
]

# brand-box ink tokens, lifted verbatim from index.html. Setting these inline on
# <html> outranks the `html[data-theme=...]` rules, so the mark + wordmark +
# glow all flick to the target palette without disturbing the background.
# --pending inks the bobs while the mark is down/recovering, --border2 the rail:
# both are part of the pendulum's dress and must hop with the rest of the ink.
INK_KEYS = ("--fg", "--fg-dim", "--fg-faint", "--accent", "--glow",
            "--pending", "--border2")
THEME_INK = {
    "amber":          ("#ffb000", "#c98a2c", "#a37a38", "#ffd98a",
                       "rgba(255,176,0,.55)", "#ffbf47", "#523c1a"),
    "green":          ("#38ff7a", "#1fbf57", "#22a85c", "#b6ffce",
                       "rgba(56,255,122,.5)", "#ffbf47", "#1d6038"),
    "modern":         ("#d6dee8", "#9aa4b2", "#8b95a3", "#79c0ff",
                       "rgba(121,192,255,.0)", "#ffbf47", "#3a424f"),
    "standard":       ("#e6e9ed", "#a6aeb9", "#8f98a3", "#4c8dff",
                       "rgba(76,141,255,0)", "#ffbf47", "#48505a"),
    "amber-light":    ("#3f2d00", "#6d500b", "#755c20", "#855700",
                       "rgba(138,90,0,.28)", "#7d5300", "#a98f52"),
    "green-light":    ("#0b3a1e", "#1a6238", "#3a7050", "#0d7034",
                       "rgba(18,105,55,.25)", "#7d5300", "#7fb28c"),
    "modern-light":   ("#1f2328", "#57606a", "#656e79", "#0969da",
                       "rgba(9,105,218,0)", "#7d5300", "#a8b3bd"),
    "standard-light": ("#14181d", "#4b5563", "#5d6675", "#0b5ed7",
                       "rgba(11,94,215,0)", "#b45309", "#aab3bf"),
}

GLITCH_LEN = max(1, round(GLITCH_MS / FRAME_MS))


def events_map(seed):
    """frame -> sim events for one candidate seed, shared verbatim by the seed
    search and the capture (the two MUST consume the sim identically or the
    search lies). The poke magnitudes are the seed's own: drawn from a
    generator keyed on (SEED, seed) so every candidate performs a different
    set of stumbles and the search vets the schedule it will actually film."""
    rng = random.Random(SEED * 1_000_003 + seed)
    ev = {}
    for f in HOP_FRAMES:
        lo, hi = POKE_DROP if f == DISCONNECT_AT else POKE_SWAY
        ev[f] = {"poke": round(rng.uniform(lo, hi), 3)}
    ev[DISCONNECT_AT]["conn"] = False
    ev[RECONNECT_AT] = {"conn": True}
    return ev


def wrap_a(a):
    """Wrap an angle to (-pi, pi], mirroring the page's wrapA."""
    a = math.fmod(a, 2 * math.pi)
    if a > math.pi:
        a -= 2 * math.pi
    elif a <= -math.pi:
        a += 2 * math.pi
    return a


def is_calm(st):
    """True when the sim state JS_DRIVE returned is still enough to freeze:
    balanced, trail drained, and any residual motion sub-pixel at the mount's
    scale (see the stillness-gate block)."""
    s = st["s"]
    sway = abs(wrap_a(s[2])) + abs(wrap_a(s[4])) + 0.3 * (abs(s[3]) + abs(s[5]))
    return (st["mode"] == "balance" and st["trail"] == 0
            and sway < FREEZE_CALM
            and abs(s[0]) < FREEZE_X and abs(s[1]) < FREEZE_XD)


# Captures the header's mounted CronstableLogo instance the moment the page
# creates it: mountGlyph is wrapped as window.CronstableLogo is assigned, and
# the returned logo lands on window.__pendLogo. The dynamic right gate
# (railMax) is pinned OFF: the capture drives sim.step()+_render() directly
# (never _gateStep), the seed search is choreographed against the word-edge
# track, and a mid-page gate cap would balloon the '#mark svg' rect — and
# with it the clip box — to half the viewport. Otherwise noninvasive — the
# page's own code runs unmodified.
INIT_HOOK = (
    "(() => { let CL;"
    "Object.defineProperty(window, 'CronstableLogo', {"
    " configurable: true, get: () => CL,"
    " set: (v) => { const orig = v.mountGlyph;"
    "  v.mountGlyph = function (slot, opts) {"
    "   const logo = orig.call(v, slot, Object.assign({}, opts, { railMax: null }));"
    "   window.__pendLogo = logo; return logo; };"
    "  CL = v; } }); })();"
)

# Unhook the page's animation (its rAF loop + anything that could restart it)
# and rebuild the sim on the chosen seed: connected, balanced, exactly upright —
# frame 0 of the loop. breeze:false is the whole point of the choreography: the
# live page's ambient sway is off, so the only disturbances are the pokes.
JS_SETUP = """(seed) => {
  const L = window.__pendLogo;
  L.sync = () => {};                       // kickMark() etc. may not restart us
  if (L._raf) cancelAnimationFrame(L._raf);
  L._raf = 0;
  L.sim = new window.CronstableLogo.Sim(L.sim.p, { seed, breeze: false });
  // defensive: railMax is pinned off above, but if the dynamic gate is ever
  // re-enabled here, a pre-setup disconnect must not freeze it extended
  const gt = L._gate;
  if (gt) { gt.x = gt.rest; gt.settledAt = -1; gt.pending = null; L._gateDrawn = null; }
  L.trail.length = 0;
  L._render();
}"""

# Replay each candidate's event timeline through fresh sims (physics only) and
# report how the performance went. Runs inside the page so the physics is the
# page's own, byte for byte.
JS_SEARCH = """([frames, eventsBySeed, seeds, gate]) => {
  const params = window.__pendLogo.sim.p;
  const wrap = (a) => { const T = 2 * Math.PI;
    a = ((a % T) + T) % T; return a > Math.PI ? a - T : a; };
  const out = [];
  for (const seed of seeds) {
    const events = eventsBySeed[seed];
    const sim = new window.CronstableLogo.Sim(params, { seed, breeze: false });
    let catchAt = -1, fellEarly = false, clamped = false, lost = false;
    for (let k = 0; k < frames; k++) {
      const ev = events[k];
      if (ev) {
        if (ev.conn !== undefined) sim.setConnected(ev.conn);
        if (ev.poke) sim.poke(ev.poke);
      }
      sim.step(0.02);
      if (k < gate && sim.mode !== 'balance') fellEarly = true;
      if (sim.s[0] >= sim.xMax * 1.045 || sim.s[0] <= sim.xMin * 1.045) clamped = true;
      if (k > gate && catchAt < 0 && sim.mode === 'balance') catchAt = k;
      if (catchAt > 0 && k > catchAt && sim.mode !== 'balance') { lost = true; catchAt = -1; }
    }
    const s = sim.s;
    out.push({ seed, catchAt, fellEarly, clamped, lost,
      endCalm: Math.abs(wrap(s[2])) + Math.abs(wrap(s[4]))
             + 0.3 * (Math.abs(s[3]) + Math.abs(s[5])),
      endX: Math.abs(s[0]) });
  }
  return out;
}"""

# One captured frame: apply this frame's sim events and ink, step the sim by
# exactly one frame, render, let the compositor settle, and report the state
# so the capture can decide when the mark has gone still enough to freeze.
JS_DRIVE = """([ev, ink, dtMs]) => new Promise((res) => {
  const L = window.__pendLogo;
  if (ev) {
    if (ev.conn !== undefined) L.sim.setConnected(ev.conn);
    if (ev.poke) L.sim.poke(ev.poke);
  }
  const el = document.documentElement,
        keys = ['--fg','--fg-dim','--fg-faint','--accent','--glow',
                '--pending','--border2'];
  if (ink) keys.forEach((k, i) => el.style.setProperty(k, ink[k]));
  else keys.forEach((k) => el.style.removeProperty(k));
  L.sim.step(dtMs / 1000);
  L._render();
  requestAnimationFrame(() => requestAnimationFrame(() =>
    res({ mode: L.sim.mode, s: L.sim.s.slice(), trail: L.trail.length })));
})"""

# A frozen stretch: the screen holds the previous frame while the physics
# steps on underneath, render-free, in one round trip. No event ever lands
# inside one of these spans (the capture always stops at the next event
# frame), and a calm balanced sim with the breeze off cannot wake itself up.
JS_RUN = """([n, dtMs]) => {
  const L = window.__pendLogo, dt = dtMs / 1000;
  for (let i = 0; i < n; i++) L.sim.step(dt);
}"""


def glitch_env(i, span):
    """0..1 aberration strength across a glitch of `span` frames: one smooth
    rise-and-fall, ~0 at the ends so the base wordmark returns home cleanly. The
    colour *switch* itself is separate (held from the hop onward); this only
    shapes the chromatic-split / tear / base-slide intensity."""
    return math.sin(math.pi * (i + 0.5) / span)


def _silhouette(gray, color, paper):
    """A single-colour copy of the ink, neutral against its ground: bright on
    black (screen), white-backed on paper (multiply)."""
    if paper:
        return ImageOps.colorize(ImageOps.invert(gray), black=(255, 255, 255), white=color)
    return ImageOps.colorize(gray, black=(0, 0, 0), white=color)


def spiderverse(img, env, rng, paper):
    """Split the (already colour-switched) glyph into jittered, saturated
    silhouettes for a chromatic misregistration, then slice-tear. The whole
    wordmark also eases sideways (GLITCH_MOVE) so the foundation itself moves."""
    if env <= 0:
        return img
    img = ImageChops.offset(img, int(round(GLITCH_MOVE * SCALE * env)), 0)
    gray = img.convert("L")
    ghosts = GHOSTS_PAPER if paper else GHOSTS_DARK
    reach = GLITCH_SPLIT * SCALE * (0.55 + 0.75 * env)
    order = list(range(len(ghosts)))
    rng.shuffle(order)
    out = img
    for gi in order[:3]:                         # three colour ghosts per frame
        dx = int(round(rng.uniform(-reach, reach)))
        dy = int(round(rng.uniform(-reach, reach) * 0.55))
        layer = ImageChops.offset(_silhouette(gray, ghosts[gi], paper), dx, dy)
        if paper:
            out = ImageChops.darker(out, layer)   # colours print under the paper
        else:
            out = ImageChops.lighter(out, layer)  # colours glow over the black
    if GLITCH_TEAR:
        w, h = out.size
        torn = out.copy()
        max_shift = int(round((GLITCH_SPLIT + 3) * SCALE * env)) + 1
        for _ in range(GLITCH_TEAR):
            y = rng.randint(0, h - 1)
            band_h = rng.randint(max(2, h // 36), max(3, h // 10))
            band = out.crop((0, y, w, min(h, y + band_h)))
            torn.paste(ImageChops.offset(band, rng.randint(-max_shift, max_shift), 0), (0, y))
        out = torn
    return out


def new_page(browser, theme):
    ctx = browser.new_context(
        viewport={"width": 900, "height": 260},
        device_scale_factor=SCALE,
        bypass_csp=True,
        reduced_motion="no-preference",  # keep the CRT glow classes on
    )
    ctx.add_init_script(
        "try{localStorage.setItem('cronstable.boot','false');"
        "localStorage.setItem('cronstable.zen','false');"
        f"localStorage.setItem('cronstable.theme','\"{theme}\"');}}catch(e){{}}"
        + INIT_HOOK
    )
    page = ctx.new_page()
    page.goto(f"http://127.0.0.1:{PORT}/index.html")
    page.wait_for_selector("#mark svg")
    page.evaluate("document.fonts.ready")  # settle glyph/wordmark metrics
    # pin the CRT flicker (a 100 ms opacity dip every 3.2 s) at its 97%-of-the-
    # time state so frozen stretches and re-runs are deterministic. The header's
    # bottom border would cross the frame as a stray horizontal line (the clip
    # pads below the mark's swing box, past the header's edge), so it is dropped
    # and the header's own background is extended down over the padding band.
    page.add_style_tag(content=
        "#crt { animation: none !important; }"
        "header { border-bottom: none !important;"
        f" padding-bottom: {2 * PAD}px !important; }}")
    return ctx, page


def pick_seed(page):
    """Replay each candidate's choreography through the page's own sim and
    pick the first acceptable performance. Chunked with an early exit: a
    seed's poke schedule is its own, so candidates keep arriving until one
    chunk yields survivors (widening the catch window once before giving up)."""
    seeds = list(SIM_SEEDS)
    all_events = {s: {str(k): v for k, v in events_map(s).items()} for s in seeds}

    def survivors(rs, lo, hi):
        return [r for r in rs
                if not r["fellEarly"] and not r["clamped"] and not r["lost"]
                and lo <= r["catchAt"] <= hi
                and r["endCalm"] < 0.003 and r["endX"] < 0.0012]

    results = []
    for i in range(0, len(seeds), SEARCH_CHUNK):
        chunk = seeds[i:i + SEARCH_CHUNK]
        results += page.evaluate(
            JS_SEARCH,
            [FRAMES, {str(s): all_events[s] for s in chunk}, chunk, DISCONNECT_AT])
        ok = survivors(results, *CATCH_WINDOW)
        print(f"[seed] searched {min(i + SEARCH_CHUNK, len(seeds))}/{len(seeds)}"
              f" ({len(ok)} acceptable)")
        if ok:
            break
    for lo, hi in (CATCH_WINDOW, CATCH_LOOSE):
        ok = survivors(results, lo, hi)
        if ok:
            mid = (CATCH_WINDOW[0] + CATCH_WINDOW[1]) / 2
            best = min(ok, key=lambda r: abs(r["catchAt"] - mid))
            pokes = ", ".join(f"{f * FRAME_MS / 1000:.0f}s:{e['poke']}"
                              for f, e in sorted(events_map(best["seed"]).items())
                              if "poke" in e)
            print(f"[seed] chose {best['seed']}: catch at frame {best['catchAt']} "
                  f"({best['catchAt'] * FRAME_MS / 1000:.1f}s), "
                  f"endCalm {best['endCalm']:.4f}, pokes [{pokes}]")
            return best["seed"]
    raise SystemExit(f"no candidate seed produced a clean loop: {results}")


def capture(browser, base, theme, stops, seed):
    ctx, page = new_page(browser, theme)
    page.evaluate(JS_SETUP, seed)
    box = page.evaluate("""() => {
      const rs = ['#mark svg', '#brandName']
        .map((s) => document.querySelector(s).getBoundingClientRect());
      const x = Math.min(...rs.map((r) => r.left));
      const y = Math.min(...rs.map((r) => r.top));
      return { x, y,
               width: Math.max(...rs.map((r) => r.right)) - x,
               height: Math.max(...rs.map((r) => r.bottom)) - y };
    }""")
    clip = {
        "x": max(0, box["x"] - PAD),
        "y": max(0, box["y"] - PAD),
        "width": box["width"] + 2 * PAD,
        "height": box["height"] + 2 * PAD,
    }

    paper = theme.endswith("-light")   # the ground the colour ghosts blend on
    seq = [theme] + list(stops) + [theme]
    starts = list(HOP_FRAMES)
    inks = {
        c: (None if c == theme else dict(zip(INK_KEYS, THEME_INK[c])))
        for c in set(seq)
    }
    events = events_map(seed)
    stop_frames = sorted(events)       # a freeze may never skip an event

    # frames[] and durs[] run in parallel: a hot (screenshotted) frame arrives
    # as 20 ms and a frozen stretch extends the LAST hot frame's duration, so
    # sum(durs) is always exactly FRAMES * FRAME_MS of true-speed physics.
    frames, durs = [], []
    cap_of = {}                        # sim frame -> index in frames (hot only)
    seg_last = {}                      # ink segment -> its latest hot index
    glitch_reps = []
    calm_run = FREEZE_HOLD             # frame 0 is exact upright: pre-satisfied
    k = 0
    while k < FRAMES:
        in_glitch = any(s <= k < s + GLITCH_LEN for s in starts)
        if k and calm_run >= FREEZE_HOLD and not in_glitch and k not in events:
            nxt = min([f for f in stop_frames if f > k] or [FRAMES])
            page.evaluate(JS_RUN, [nxt - k, FRAME_MS])
            durs[-1] += (nxt - k) * FRAME_MS
            k = nxt
            continue
        seg = sum(1 for s in starts if k >= s)  # colour switches at each hop
        st = page.evaluate(JS_DRIVE, [events.get(k), inks[seq[seg]], FRAME_MS])
        img = Image.open(BytesIO(page.screenshot(clip=clip))).convert("RGB")
        for s in starts:
            if s <= k < s + GLITCH_LEN:
                img = spiderverse(img, glitch_env(k - s, GLITCH_LEN),
                                  random.Random(SEED + k), paper)
                if k == s + GLITCH_LEN // 2:
                    glitch_reps.append(len(frames))
                break
        frames.append(img)
        durs.append(FRAME_MS)
        cap_of[k] = seg_last[seg] = len(frames) - 1
        calm_run = calm_run + 1 if is_calm(st) else 0
        k += 1
    ctx.close()

    # one shared palette (no per-frame requantize -> no palette flicker), no
    # dither (a shifting dither pattern would shimmer between frames). The
    # palette source stacks each ink segment's settled still, the middle of
    # each glitch, AND a sweep through the collapse/swing-up (the amber
    # "recovering" bobs and the accent trail only exist there), so every ink
    # the loop wears gets real palette slots.
    w, h = frames[0].size
    swing_reps = [cap_of[f] for f in range(DISCONNECT_AT + 25, CATCH_WINDOW[1], 90)
                  if f in cap_of]      # post-catch frames may already be frozen
    picks = sorted(set(list(seg_last.values()) + glitch_reps + swing_reps)) or [0]
    src = Image.new("RGB", (w, h * len(picks)))
    for i, fk in enumerate(picks):
        src.paste(frames[fk], (0, i * h))
    pal = src.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
    mapped = [f.quantize(palette=pal, dither=Image.Dither.NONE) for f in frames]

    OUT.mkdir(exist_ok=True)
    # WebP primary: full 24-bit RGB frames, no palette (the quality win). In
    # lossless mode `quality` is the compression *effort* (100 = smallest, still
    # pixel-perfect); in lossy mode it's fidelity. Per-frame durations carry
    # the frozen stretches.
    webp = OUT / f"{base}.webp"
    frames[0].save(
        webp, format="WEBP", save_all=True, append_images=frames[1:],
        duration=durs, loop=0, method=WEBP_METHOD,
        lossless=WEBP_LOSSLESS,
        quality=100 if WEBP_LOSSLESS else WEBP_QUALITY,
    )
    # GIF twin: the 256-colour fallback (centisecond timing: every duration is
    # a multiple of FRAME_MS = 20 ms, so nothing rounds)
    gif = OUT / f"{base}.gif"
    mapped[0].save(
        gif, format="GIF", save_all=True, append_images=mapped[1:],
        duration=durs, loop=0, optimize=True,
    )
    seam = max(hi for _, hi in ImageChops.difference(frames[0], frames[-1]).getextrema())
    tour = " -> ".join(seq)
    print(f"[img] {base}: {len(frames)} hot frames / {FRAMES} sim frames "
          f"({sum(durs) / 1000:.1f}s loop), seed {seed}, {len(starts)} hops [{tour}], "
          f"seam max-channel delta {seam} "
          f"-> webp {webp.stat().st_size // 1024} KB, gif {gif.stat().st_size // 1024} KB")


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass


def main():
    handler = partial(Quiet, directory=str(WEB))
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with sync_playwright() as p:
        browser = p.chromium.launch()
        # the physics is palette-independent: search once, replay for both
        ctx, page = new_page(browser, VARIANTS[0][1])
        seed = pick_seed(page)
        ctx.close()
        for base, theme, stops in VARIANTS:
            capture(browser, base, theme, stops, seed)
        browser.close()
    srv.shutdown()


if __name__ == "__main__":
    main()
