/* =====================================================================
 *  logo engine — a real self-balancing double pendulum on a cart.
 *
 *  This is not a canned animation. The header mark integrates the full
 *  nonlinear cart/double-pendulum dynamics (RK4 at 240 Hz) and balances
 *  itself with an LQR controller whose gains are computed at page load
 *  from a numerical linearization (discrete Riccati iteration). While the
 *  daemon is live it stands upright, swaying in a little breeze; lose the
 *  signal and the motor cuts — it collapses and swings, pumping energy
 *  (dE/dt = -a*G exactly) but held just short of the top. When the signal
 *  returns, a receding-horizon cross-entropy planner threads the swing
 *  into the balance controller's basin — every catch is verified by a
 *  2-second closed-loop rollout before it is committed. A hard recovery
 *  gets running room: the right end of the track swings open toward
 *  mid-page — the physics bound leaps at once, while a phantom rail
 *  and a fleeing end gate keep the picture honest — and eases back
 *  to the end of the word once the letter stands at home. Stability isn't
 *  a metaphor here; it is recomputed 240 times a second.
 *
 *  State s = [x, xd, th1, w1, th2, w2]; angles from upright; control is
 *  cart acceleration. Tuned + Monte-Carlo-tested headlessly (recovery
 *  median ~15 s, ~100% by 90 s, zero unverified catches in 10-min soak).
 * ===================================================================== */
"use strict";
(function () {
  const DP = {
    g: 9.81,
    L1: 0.34, L2: 0.26,      // link lengths (m)
    m1: 0.14, m2: 0.09,      // point masses at link tips (kg)
    d1: 0.005, d2: 0.002,    // joint viscous damping
    trackHalf: 0.42,         // cart travel each side of center (m)
    aMaxBalance: 18,         // accel authority while balancing (m/s^2)
    aMaxCatch: 24,           // extra muscle in the first 1.2 s of a catch
    aMaxSwing: 26,           // swing-up authority (escalates to 34)
    dt: 1 / 240,             // physics step (s)
  };
  const LQR_Q = [4, 3.2, 120, 1.33, 162, 2], LQR_R = 0.12;
  const SEG_N = 8, SEG_DT = 0.12;                  // CEM plan: 0.96 s horizon
  const SEG_STEPS = Math.round(SEG_DT / (1 / 120));
  const ENGAGE_GATE = 12000;                       // z'Pz gate before verifying
  const VERIFY_COOLDOWN = 0.06;                    // s between catch rollouts

  const clamp = (v, lo, hi) => (v < lo ? lo : v > hi ? hi : v);
  const wrapA = (a) => {
    const T = 2 * Math.PI;
    a = ((a % T) + T) % T;
    return a > Math.PI ? a - T : a;
  };
  const mulberry32 = (seed) => {
    let t = seed >>> 0;
    return () => {
      t += 0x6D2B79F5;
      let r = Math.imul(t ^ (t >>> 15), 1 | t);
      r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
      return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
    };
  };

  // ---- dynamics: M(q) qdd = b, solved 2x2 per evaluation ----
  // derivInto writes into a caller-owned vector. A planning frame runs 8960
  // rk4 steps and each one used to allocate eight fresh 6-element arrays, so
  // the integrator alone churned ~4 M arrays/second during a recovery; the
  // scratch buffers below cut that to one per step (the returned state, which
  // callers keep). Every operation is the same double arithmetic in the same
  // order, so the trajectory is bit-identical.
  function derivInto(p, s, a, out) {
    const xd = s[1], th1 = s[2], w1 = s[3], th2 = s[4], w2 = s[5];
    const s1 = Math.sin(th1), c1 = Math.cos(th1);
    const s2 = Math.sin(th2), c2 = Math.cos(th2);
    const D = th1 - th2, sD = Math.sin(D), cD = Math.cos(D);
    const M = p.m1 + p.m2;
    const M11 = M * p.L1 * p.L1;
    const M12 = p.m2 * p.L1 * p.L2 * cD;
    const M22 = p.m2 * p.L2 * p.L2;
    const Q1 = -p.d1 * w1 + p.d2 * (w2 - w1);
    const Q2 = -p.d2 * (w2 - w1);
    const b1 = -p.m2 * p.L1 * p.L2 * w2 * w2 * sD + p.g * M * p.L1 * s1 - M * p.L1 * c1 * a + Q1;
    const b2 = p.m2 * p.L1 * p.L2 * w1 * w1 * sD + p.g * p.m2 * p.L2 * s2 - p.m2 * p.L2 * c2 * a + Q2;
    const det = M11 * M22 - M12 * M12;
    out[0] = xd; out[1] = a; out[2] = w1;
    out[3] = (M22 * b1 - M12 * b2) / det;
    out[4] = w2;
    out[5] = (M11 * b2 - M12 * b1) / det;
    return out;
  }
  // allocating twin, for the handful of linearization probes in computeLQR
  const deriv = (p, s, a) => derivInto(p, s, a, new Array(6));
  const _k1 = new Float64Array(6), _k2 = new Float64Array(6);
  const _k3 = new Float64Array(6), _k4 = new Float64Array(6);
  const _sMid = new Float64Array(6);
  function rk4(p, s, a, dt) {
    derivInto(p, s, a, _k1);
    for (let i = 0; i < 6; i++) _sMid[i] = s[i] + 0.5 * dt * _k1[i];
    derivInto(p, _sMid, a, _k2);
    for (let i = 0; i < 6; i++) _sMid[i] = s[i] + 0.5 * dt * _k2[i];
    derivInto(p, _sMid, a, _k3);
    for (let i = 0; i < 6; i++) _sMid[i] = s[i] + dt * _k3[i];
    derivInto(p, _sMid, a, _k4);
    const out = new Array(6);
    for (let i = 0; i < 6; i++) out[i] = s[i] + (dt / 6) * (_k1[i] + 2 * _k2[i] + 2 * _k3[i] + _k4[i]);
    return out;
  }
  // pendulum energy in the cart frame; upright = eUp
  function energy(p, s) {
    const th1 = s[2], w1 = s[3], th2 = s[4], w2 = s[5];
    const M = p.m1 + p.m2;
    return 0.5 * M * p.L1 * p.L1 * w1 * w1
      + 0.5 * p.m2 * p.L2 * p.L2 * w2 * w2
      + p.m2 * p.L1 * p.L2 * w1 * w2 * Math.cos(th1 - th2)
      + p.g * (M * p.L1 * Math.cos(th1) + p.m2 * p.L2 * Math.cos(th2));
  }
  const eUp = (p) => p.g * ((p.m1 + p.m2) * p.L1 + p.m2 * p.L2);
  // energy-flow coupling: dE/dt = -a * G(s); the pump pushes against G
  const Gterm = (p, s) =>
    (p.m1 + p.m2) * p.L1 * Math.cos(s[2]) * s[3] + p.m2 * p.L2 * Math.cos(s[4]) * s[5];

  // ---- LQR gains from a numerical linearization at the upright ----
  function computeLQR(p) {
    const n = 6, eps = 1e-6, dtc = 0.002;
    const mm = (A, B) => {
      const rn = A.length, cm = B[0].length, kk = B.length;
      const C = Array.from({ length: rn }, () => new Float64Array(cm));
      for (let i = 0; i < rn; i++)
        for (let t = 0; t < kk; t++) {
          const a = A[i][t];
          if (a === 0) continue;
          for (let j = 0; j < cm; j++) C[i][j] += a * B[t][j];
        }
      return C;
    };
    const eye = (k) => Array.from({ length: k }, (_, i) => Array.from({ length: k }, (_, j) => (i === j ? 1 : 0)));
    const s0 = [0, 0, 0, 0, 0, 0], f0 = deriv(p, s0, 0);
    const A = eye(n).map((r) => r.map(() => 0));
    for (let j = 0; j < n; j++) {
      const sp = s0.slice(); sp[j] += eps;
      const fp = deriv(p, sp, 0);
      for (let i = 0; i < n; i++) A[i][j] = (fp[i] - f0[i]) / eps;
    }
    const fB = deriv(p, s0, eps);
    const Bc = f0.map((v, i) => [(fB[i] - v) / eps]);
    let Ad = eye(n), term = eye(n);
    let Sint = eye(n).map((r) => r.map((v) => v * dtc)), termI = Sint.map((r) => r.slice());
    for (let k = 1; k <= 8; k++) {
      term = mm(term, A).map((r) => Array.from(r, (v) => v * dtc / k));
      Ad = Ad.map((r, i) => r.map((v, j) => v + term[i][j]));
      termI = mm(termI, A).map((r) => Array.from(r, (v) => v * dtc / (k + 1)));
      Sint = Sint.map((r, i) => r.map((v, j) => v + termI[i][j]));
    }
    const Bd = mm(Sint, Bc);
    const Q = eye(n).map((r, i) => r.map((v, j) => (i === j ? LQR_Q[i] : 0)));
    let P = Q.map((r) => r.slice());
    const T = (X) => X[0].map((_, j) => X.map((r) => r[j]));
    for (let it = 0; it < 120000; it++) {
      const PA = mm(P, Ad), PB = mm(P, Bd);
      const AtPA = mm(T(Ad), PA), AtPB = mm(T(Ad), PB);
      let BtPB = 0;
      for (let i = 0; i < n; i++) BtPB += Bd[i][0] * PB[i][0];
      const inv = 1 / (LQR_R + BtPB);
      // re-symmetrized each pass: the naive recursion sheds symmetry via
      // cancellation on this stiff system and diverges to NaN without it
      let Pn = eye(n).map((r, i) => r.map((_, j) => Q[i][j] + AtPA[i][j] - AtPB[i][0] * inv * AtPB[j][0]));
      Pn = Pn.map((r, i) => r.map((v, j) => 0.5 * (v + Pn[j][i])));
      let diff = 0, pmax = 0;
      for (let i = 0; i < n; i++)
        for (let j = 0; j < n; j++) {
          diff = Math.max(diff, Math.abs(Pn[i][j] - P[i][j]));
          pmax = Math.max(pmax, Math.abs(Pn[i][j]));
        }
      P = Pn;
      if (diff < 1e-12 * Math.max(1, pmax)) break;
    }
    const PB = mm(P, Bd), PA = mm(P, Ad);
    let BtPB = 0;
    for (let i = 0; i < n; i++) BtPB += Bd[i][0] * PB[i][0];
    const K = [];
    for (let j = 0; j < n; j++) {
      let BtPA = 0;
      for (let i = 0; i < n; i++) BtPA += Bd[i][0] * PA[i][j];
      K.push(BtPA / (LQR_R + BtPB));
    }
    return { K, P };
  }

  // computeLQR reads exactly the seven dynamics parameters deriv reads, and
  // takes ~6500 iterations of 6x6 matrix algebra (~30 ms, measured) to reach
  // them. Every mount of the shipped wordmark has the same physics, so that
  // was ~30 ms of blocking script on the critical path to first paint for an
  // answer that never changes: the default DP's gains are seeded below, and
  // any other parameter set is solved once and kept (docs/logo-lab.html and
  // docs/comparison.html build several sims from one set).
  //
  // Keyed on the exact parameter string, so editing DP misses the seed and
  // falls back to the solver instead of flying stale gains. K and P are read
  // only after construction (_lqrAccel, _vCost), so sims can share them.
  const lqrKey = (p) => [p.g, p.L1, p.L2, p.m1, p.m2, p.d1, p.d2].join("|");
  const LQR_CACHE = new Map([["9.81|0.34|0.26|0.14|0.09|0.005|0.002", {
    K: [5.556371740320117, 11.397213225555143, -274.7382625940304,
        -8.892561917718053, 357.79380628190506, 43.699138936449486],
    P: [
      [4102.394061876565, 3403.3068673379175, -5126.758442369487, 1039.3114482356623, 16980.81586321461, 2372.023278149255],
      [3403.3068673379175, 5770.684968269181, -11355.417120138944, 1719.9502990877688, 32280.640406622548, 4466.22709047167],
      [-5126.758442369487, -11355.417120138944, 194942.64902431052, 1897.4647951004993, -293403.1021144789, -36614.65971869856],
      [1039.3114482356623, 1719.9502990877688, 1897.4647951004993, 769.2702917944772, 3450.5683239105483, 598.9046117665653],
      [16980.81586321461, 32280.640406622548, -293403.1021144789, 3450.5683239105483, 529535.3713706646, 66204.51980950462],
      [2372.023278149255, 4466.22709047167, -36614.65971869856, 598.9046117665653, 66204.51980950462, 8461.20622167257],
    ],
  }]]);
  function lqrGains(p) {
    const key = lqrKey(p);
    let hit = LQR_CACHE.get(key);
    if (!hit) { hit = computeLQR(p); LQR_CACHE.set(key, hit); }
    return hit;
  }

  // ---- simulator + supervisor: balance -> limp -> swing -> balance ----
  class PendulumSim {
    constructor(params, opts) {
      this.p = Object.assign({}, DP, params || {});
      this.opts = Object.assign({
        catchWhileDisconnected: false,
        limpDuration: 0.55,
        disconnectedEnergyFrac: 0.94,  // strive, never top out, while offline
        seed: 1,
        breeze: true,
      }, opts || {});
      // track bounds (m, relative to the balance point x = 0). trackMin/
      // trackMax params make the track asymmetric — the wordmark mount
      // stretches it to the ends of "cronstable" while the equilibrium
      // stays at the l's cell.
      this.xMin = this.p.trackMin != null ? this.p.trackMin : -this.p.trackHalf;
      this.xMax = this.p.trackMax != null ? this.p.trackMax : this.p.trackHalf;
      this.rand = mulberry32(this.opts.seed);
      const lq = lqrGains(this.p);
      this.K = lq.K; this.P = lq.P;
      this.s = [0, 0, 0, 0, 0, 0];
      this.mode = "balance";
      this.connected = true;
      this.t = 0;
      this.a = 0;
      this.limpUntil = 0;
      this.swingStart = 0;
      this.catchT = -1;
      this.plan = null;
      this.planAge = 0;
      this.cem = null;
      this.lastVerify = -1;
      this.onMode = null;
      this.noise = { breeze: 0, flutter: 0 };
    }
    setConnected(c) {
      c = !!c;
      if (c === this.connected) return;
      this.connected = c;
      if (!c && this.mode === "balance") this._toMode("limp");
      // reconnect mid-swing: start the escalation clock warm — the user is
      // watching a recovery, skip the most patient settings
      if (c && this.mode === "swing") this.swingStart = this.t - 8;
    }
    poke(strength) {
      const k = strength == null ? 3.5 : strength;
      const dir = this.rand() < 0.5 ? -1 : 1;
      this.s[3] += dir * k;
      this.s[5] += dir * k * (0.6 + 0.8 * this.rand());
    }
    // a pointer sweeping through the linkage brushes it aside: quadratic
    // ("wind") drag at the closest approach between the cursor's swept path
    // (ax,ay)→(bx,by) and each link, projected through the point Jacobian
    // onto the joint velocities — the lever arm and the angle of attack both
    // shape the kick. Swept-path contact (not point sampling) so a fast
    // flick can't tunnel between mousemove events; a slow hover barely
    // stirs it. Args in sim units (m, s, m); per-event clamps keep
    // teleporting cursors from exploding the state.
    brush(ax, ay, bx, by, dt, R) {
      const p = this.p, s = this.s;
      const vx = (bx - ax) / dt, vy = (by - ay) / dt;
      const v2 = vx * vx + vy * vy;
      if (v2 < 1e-4) return;
      const sp = Math.sqrt(v2);
      const s1 = Math.sin(s[2]), c1 = Math.cos(s[2]);
      const s2 = Math.sin(s[4]), c2 = Math.cos(s[4]);
      const ex = s[0] + p.L1 * s1, ey = p.L1 * c1;
      const tx = ex + p.L2 * s2, ty = ey + p.L2 * c2;
      // closest approach between the path and a link segment (clamped
      // projections, one refinement pass — exact enough for a brush)
      const hit = (q0x, q0y, q1x, q1y) => {
        const ux = bx - ax, uy = by - ay;
        const wx = q1x - q0x, wy = q1y - q0y;
        const rx = ax - q0x, ry = ay - q0y;
        const A = ux * ux + uy * uy, B = ux * wx + uy * wy, C = wx * wx + wy * wy;
        const D = ux * rx + uy * ry, E = wx * rx + wy * ry;
        const den = A * C - B * B;
        let sc = den > 1e-12 ? clamp((B * E - C * D) / den, 0, 1) : 0;
        let tc = C > 1e-12 ? clamp((B * sc + E) / C, 0, 1) : 0;
        sc = A > 1e-12 ? clamp((B * tc - D) / A, 0, 1) : 0;
        tc = C > 1e-12 ? clamp((B * sc + E) / C, 0, 1) : 0;
        const dx = (ax + sc * ux) - (q0x + tc * wx), dy = (ay + sc * uy) - (q0y + tc * wy);
        return { t: tc, d: Math.hypot(dx, dy) };
      };
      const G1 = 0.09, G2 = 0.14;
      const av1 = (vx * c1 - vy * s1) * sp;        // drag along link 1's swing direction
      const h1 = hit(s[0], 0, ex, ey);
      if (h1.d < R) s[3] += clamp(G1 * (1 - h1.d / R) * av1 * h1.t * dt, -0.5, 0.5);
      const h2 = hit(ex, ey, tx, ty);
      if (h2.d < R) {
        const fall = 1 - h2.d / R;
        s[5] += clamp(G2 * fall * (vx * c2 - vy * s2) * sp * h2.t * dt, -0.7, 0.7);
        s[3] += clamp(0.6 * G1 * fall * av1 * dt, -0.4, 0.4);  // the elbow carries the blow too
      }
    }
    _toMode(m) {
      if (m === this.mode) return;
      this.mode = m;
      this.plan = null;
      this.cem = null;
      if (m === "limp") this.limpUntil = this.t + this.opts.limpDuration;
      if (m === "swing") this.swingStart = this.t;
      if (m === "balance") this.catchT = this.t;
      if (this.onMode) this.onMode(m);
    }
    _wrapped(s) {
      s = s || this.s;
      return [s[0], s[1], wrapA(s[2]), s[3], wrapA(s[4]), s[5]];
    }
    _lqrAccel(z, aMax) {
      let a = 0;
      for (let i = 0; i < 6; i++) a -= this.K[i] * z[i];
      return clamp(a, -aMax, aMax);
    }
    _vCost(s) {
      const z = this._wrapped(s);
      let c = 0;
      for (let i = 0; i < 6; i++) for (let j = 0; j < 6; j++) c += z[i] * this.P[i][j] * z[j];
      return c;
    }
    _balanceAuthority() {
      return this.catchT >= 0 && this.t - this.catchT < 1.2
        ? (this.catchAuth != null ? this.catchAuth : this.p.aMaxCatch)
        : this.p.aMaxBalance;
    }
    // patience buys muscle: swing + catch authority escalate over time
    _swingCap() {
      return Math.min(34, this.p.aMaxSwing + Math.floor((this.t - this.swingStart) / 8) * 2);
    }
    _catchAuthority() {
      const tSw = this.mode === "swing" ? this.t - this.swingStart : 0;
      return Math.min(32, this.p.aMaxCatch + Math.floor(tSw / 10) * 3);
    }
    _pumpTarget() {
      if (!this.connected) return this.opts.disconnectedEnergyFrac;
      const tSw = this.t - this.swingStart;
      // periodic calm-down dumps excess energy and restarts the approach
      // geometry, escaping unproductive limit cycles on stubborn recoveries
      if (tSw > 25 && (tSw % 15) < 1.5) return 0.75;
      return Math.min(1.35, 1.10 + Math.floor(tSw / 6) * 0.05);
    }
    _pumpAccel() {
      const p = this.p, s = this.s, EUP = eUp(p);
      const E = energy(p, s);
      const dE = EUP * this._pumpTarget() - E;
      let a;
      if (E > EUP * (this._pumpTarget() - 0.03)) {
        a = -30 * s[0] - 8 * s[1];                 // coast: recenter, wait
      } else {
        let pump = -10 * Math.tanh(2.5 * dE / EUP) * Math.tanh(Gterm(p, s) / 0.05);
        if (Math.abs(s[3]) + Math.abs(s[5]) < 0.08 && Math.cos(s[2]) < 0) {
          pump = 10 * Math.sin(2 * Math.PI * 0.9 * this.t); // bootstrap from dead hang
        }
        a = pump - 45 * s[0] - 6.5 * s[1];
      }
      return clamp(a, -p.aMaxSwing, p.aMaxSwing);
    }
    _rolloutCost(s0, segs, aCap) {
      const p = this.p;
      let s = s0.slice(), best = Infinity;
      for (let i = 0; i < SEG_N * SEG_STEPS; i++) {
        s = rk4(p, s, clamp(segs[(i / SEG_STEPS) | 0], -aCap, aCap), 1 / 120);
        if (s[0] > this.xMax * 0.95 || s[0] < this.xMin * 0.95) return Infinity;
        if ((i & 1) === 1) {
          const c = this._vCost(s);
          if (c < best) best = c;
        }
      }
      return best;
    }
    // one anytime-CEM iteration per animation frame: (mean, sigma) carry
    // across frames, mean shifted by the executed amount (fractional
    // segments, blended) so the plan keeps phase. ~80 rollouts per call.
    _cemFrame() {
      const aCap = this._swingCap();
      let c = this.cem;
      if (!c) {
        c = this.cem = { mean: new Array(SEG_N).fill(0), sigma: new Array(SEG_N).fill(aCap * 0.6) };
      } else {
        const off = this.planAge / 240 / SEG_DT;
        c.mean = c.mean.map((_, i) => {
          const q = i + off, i0 = Math.floor(q), f = q - i0;
          const v0 = i0 < SEG_N ? c.mean[i0] : 0;
          const v1 = i0 + 1 < SEG_N ? c.mean[i0 + 1] : 0;
          return v0 * (1 - f) + v1 * f;
        });
        for (let i = 0; i < SEG_N; i++) c.sigma[i] = Math.min(aCap * 0.6, c.sigma[i] * 1.25 + 0.3);
      }
      const pop = [];
      for (let k = 0; k < 80; k++) {
        const segs = c.mean.map((m, i) =>
          k === 0 ? clamp(m, -aCap, aCap)
                  : clamp(m + (this.rand() * 2 - 1) * c.sigma[i] * 1.7, -aCap, aCap));
        const cost = this._rolloutCost(this.s, segs, aCap);
        if (cost < Infinity) pop.push({ segs: segs, cost: cost });
      }
      if (!pop.length) { this.plan = null; this.planAge = 0; return; }
      pop.sort((a, b) => a.cost - b.cost);
      const elite = pop.slice(0, Math.max(6, Math.floor(pop.length * 0.12)));
      for (let i = 0; i < SEG_N; i++) {
        let m = 0;
        for (const e of elite) m += e.segs[i];
        m /= elite.length;
        let v = 0;
        for (const e of elite) v += (e.segs[i] - m) * (e.segs[i] - m);
        c.mean[i] = m;
        c.sigma[i] = Math.sqrt(v / elite.length) + 0.5;
      }
      this.plan = { segs: elite[0].segs };
      this.planAge = 0;
    }
    // candidate catch: near upright AND a full 2 s closed-loop rollout with
    // the balance controller stabilizes — no catch is taken on faith
    _tryCatch() {
      if (!this.connected && !this.opts.catchWhileDisconnected) return false;
      const p = this.p;
      const z = this._wrapped();
      if (Math.abs(z[2]) > 0.55 || Math.abs(z[4]) > 0.8) return false;
      if (Math.abs(z[3]) > 4 || Math.abs(z[5]) > 5) return false;
      const aCatch = this._catchAuthority(); // must match execution authority
      let s = this.s.slice();
      const dt = 1 / 240;
      for (let i = 0; i < Math.round(2.0 / dt); i++) {
        const zz = this._wrapped(s);
        if (Math.abs(zz[2]) > 1.0 || Math.abs(zz[4]) > 1.25 || zz[0] > this.xMax || zz[0] < this.xMin) return false;
        s = rk4(p, s, this._lqrAccel(zz, i * dt < 1.2 ? aCatch : p.aMaxBalance), dt);
      }
      const zf = this._wrapped(s);
      this.catchAuth = aCatch;
      return Math.abs(zf[2]) < 0.06 && Math.abs(zf[4]) < 0.08 &&
             Math.abs(zf[3]) < 0.5 && Math.abs(zf[5]) < 0.6;
    }
    _swingFrame() {
      const p = this.p, EUP = eUp(p);
      const E = energy(p, this.s);
      const z2 = wrapA(this.s[2]);
      const inRegion = this.connected &&
        E > EUP * 0.75 && E < EUP * 1.9 && Math.abs(z2) < 1.35 && Math.abs(this.s[3]) < 7;
      if (!inRegion) { this.plan = null; this.cem = null; return; }
      // one anytime-CEM iteration per frame, frame-budgeted: when the last
      // pass ran long (a weak machine, a throttled tab), the next frame
      // executes the standing plan instead of planning again. The
      // (mean, sigma) warm start and the planAge bookkeeping carry across
      // the gap untouched (the next pass shifts the mean by the WHOLE
      // executed amount), so a skipped frame costs one slightly stale
      // plan, never phase; the rollout-verified catch gate below still
      // runs every frame either way. opts.planBudgetMs: 0 disables the
      // budget, because it is the one place wall-clock feeds back into
      // the trajectory; a deterministic rig (the README loop capture, a
      // seeded MC) must pin it off or identical seeds can diverge on a
      // loaded machine. Validated by MC: forced alternate-frame planning
      // still caught and held 24/24 knockover seeds.
      const planBudget =
        this.opts.planBudgetMs === undefined ? 5 : this.opts.planBudgetMs;
      if (this._planDebt) {
        this._planDebt = false;
      } else {
        const planT0 = performance.now();
        this._cemFrame();
        this._planDebt =
          planBudget > 0 && performance.now() - planT0 > planBudget;
      }
      if (this.t - this.lastVerify > VERIFY_COOLDOWN && this._vCost(this.s) < ENGAGE_GATE) {
        this.lastVerify = this.t;
        if (this._tryCatch()) this._toMode("balance");
      }
    }
    // advance by real seconds; call once per animation frame (planning is
    // amortized per call so the worst-case frame stays cheap)
    step(elapsed) {
      const p = this.p;
      let remaining = clamp(elapsed, 0, 0.05);
      if (this.mode === "swing") this._swingFrame();
      while (remaining > 1e-9) {
        const dt = Math.min(p.dt, remaining);
        remaining -= dt;
        this.t += dt;
        let a = 0;
        if (this.mode === "balance") {
          const z = this._wrapped();
          if (Math.abs(z[2]) > 1.0 || Math.abs(z[4]) > 1.3 || z[0] > this.xMax * 1.02 || z[0] < this.xMin * 1.02) {
            this._toMode(this.connected ? "swing" : "limp");
          } else {
            a = this._lqrAccel(z, this._balanceAuthority());
            if (this.opts.breeze) {
              // a varying little breeze instead of random gusts. The push
              // itself is a fast Ornstein-Uhlenbeck flutter (0.8 s memory,
              // zero mean, so there is never a sustained lean for the cart
              // to walk away under); its strength is breathed by a slow
              // envelope (9 s memory, squared then saturated so a swell
              // has a hard ceiling) that swells, lulls, and leaves real
              // near-still spells. The noise gains are sigma*sqrt(12) for
              // uniform increments, holding each process near unit
              // variance, so `wind` reads directly in rad/s^2 on the
              // shoulder; the elbow takes a lighter share of the load.
              const n = this.noise, rt = Math.sqrt(dt);
              n.breeze += (-n.breeze / 9) * dt + 1.63 * rt * (this.rand() - 0.5);
              n.flutter += (-n.flutter / 0.8) * dt + 5.48 * rt * (this.rand() - 0.5);
              // room fades the wind if the cart strays from the l's cell
              // (the track has only ~0.48 m of run-out on the "e" side);
              // ease brings it back in after a catch, while the mark still
              // balances on reduced authority (_balanceAuthority).
              const room = 1 / (1 + 40 * this.s[0] * this.s[0]);
              const ease = this.catchT >= 0 ? Math.min(1, (this.t - this.catchT) / 1.5) : 1;
              const swell = n.breeze * n.breeze;
              const wind = 0.125 * (0.25 + 1.05 * swell / (1 + swell)) * n.flutter * room * ease;
              this.s[3] += wind * dt;
              this.s[5] += 0.6 * wind * dt;
            }
          }
        } else if (this.mode === "limp") {
          a = clamp(-2.0 * this.s[1], -3, 3); // motor dead; gentle rolling drag
          if (this.t >= this.limpUntil) this._toMode("swing");
        } else if (this.plan) {
          a = this.plan.segs[Math.min(SEG_N - 1, (this.planAge / (SEG_DT * 240)) | 0)];
          this.planAge++;
        } else {
          a = this._pumpAccel();
        }
        this.a = a;
        this.s = rk4(p, this.s, a, dt);
        if (this.s[0] > this.xMax * 1.05 || this.s[0] < this.xMin * 1.05) {  // hard end-stops
          this.s[0] = clamp(this.s[0], this.xMin * 1.05, this.xMax * 1.05);
          this.s[1] = 0;
          if (this.mode === "balance") this._toMode("swing");
        }
      }
    }
    pose() {
      const x = this.s[0], th1 = this.s[2], th2 = this.s[4];
      const j2 = { x: x + this.p.L1 * Math.sin(th1), y: this.p.L1 * Math.cos(th1) };
      return {
        cart: x,
        j2: j2,
        tip: { x: j2.x + this.p.L2 * Math.sin(th2), y: j2.y + this.p.L2 * Math.cos(th2) },
        mode: this.mode,
        a: this.a,
      };
    }
  }

  // ---- renderer: builds an svg into `el` and drives the sim via rAF ----
  class CronstableLogo {
    constructor(el, opts) {
      this.el = el;
      this.opts = Object.assign({
        connected: () => true,
        reducedMotion: () => false,
        scale: 28,                    // px per meter
        ui: 1,                        // cart/bobs/rail are drawn for the 28px/m mark; ×ui rescales that chrome
        railX: null,                  // rail gates override, signed [x1, x2] px (default: flush with the box edges)
        railMax: null,                // dynamic right gate: how far (px) it may open during a recovery (null = fixed)
        decorative: null,             // aria-hidden svg — for mounts where host text already names the logo
        seed: (Math.random() * 1e9) | 0,
      }, opts || {});
      this.sim = new PendulumSim(this.opts.params, { seed: this.opts.seed });
      this.trail = [];
      this._raf = 0;
      this._last = 0;
      this._mode = "";
      this._frame = this._frame.bind(this);
      this._build();
      this.sync();
    }
    _build() {
      const NS = "http://www.w3.org/2000/svg";
      const S = this.opts.scale, U = this.opts.ui, p = this.sim.p;
      const reach = (p.L1 + p.L2) * S;
      const wL = Math.ceil(-this.sim.xMin * S + reach + 4);  // box extent left of the balance point
      // … and right (symmetric by default; a dynamic gate needs room to its cap)
      const wR = Math.ceil(Math.max(this.sim.xMax * S, this.opts.railMax || 0) + reach + 4);
      const hh = Math.ceil(reach + 5);
      // by default the rail spans the full box so the end gates sit flush
      // with the logo's edges. the cart's travel is shorter, and the tip's
      // worst-case reach (|track bound| + L1 + L2, which sizes the box)
      // stays ~2px inside the gates — nothing ever clips. a railX override
      // ([x1, x2] px) places the gates explicitly instead: the glyph mount
      // puts them at the outer ends of the wordmark.
      const rx = this.opts.railX != null ? this.opts.railX : [2 - wL, wR - 2];
      this._geom = { S: S, U: U, railY: 4.2 * U, railX: rx };
      // dynamic right gate (opt-in via railMax): while the mark is recovering
      // the physics bound opens out and the drawn gate flees ahead of the
      // cart — state here, policy in _gateStep. gate travel is measured
      // RELATIVE to rest, so the physics bound extends relative to restM and
      // any railX/track pairing stays in register.
      this._gate = this.opts.railMax == null ? null : {
        x: rx[1], rest: rx[1],
        cap: Math.max(this.opts.railMax, rx[1]),
        min: 12 * U,            // drawn gate → cart-edge clearance
        restM: this.sim.xMax,   // the word-edge physics bound to come home to
        settledAt: -1,
        pending: null,          // resize shrink arriving mid-recovery, held (see setRailMax)
        v: 0,                   // low-passed rightward cart charge, px/s (see _gateStep)
        rv: 0,                  // retract speed slew, px/s — eases the ride home in from zero
      };
      const mk = (t, at, parent) => {
        const n = document.createElementNS(NS, t);
        for (const k in at) n.setAttribute(k, at[k]);
        (parent || this.svg).appendChild(n);
        return n;
      };
      this.svg = document.createElementNS(NS, "svg");
      const svgAt = {
        viewBox: (-wL) + " " + (-hh) + " " + (wL + wR) + " " + (2 * hh),
        width: wL + wR, height: 2 * hh,
      };
      if (this.opts.decorative) svgAt["aria-hidden"] = "true";
      else {
        svgAt.role = "img";
        svgAt["aria-label"] = "cronstable logo: a self-balancing double pendulum on a cart";
      }
      for (const k in svgAt) this.svg.setAttribute(k, svgAt[k]);
      const g = this._geom;
      this.eTrail = mk("polyline", { class: "p-trail", fill: "none", stroke: "var(--accent)", "stroke-width": 1, opacity: 0.35, points: "" });
      mk("line", { class: "p-rail", x1: g.railX[0], y1: g.railY, x2: g.railX[1], y2: g.railY, stroke: "var(--border2)", "stroke-width": 1.2 });
      // phantom track: the temporary rail that fills in behind the fleeing
      // right gate while the track is opened out (see _gateStep)
      this.eGhost = !this._gate ? null : mk("line", { class: "p-rail-ext", x1: g.railX[1], y1: g.railY, x2: g.railX[1], y2: g.railY, stroke: "var(--border2)", "stroke-width": 1.2 });
      mk("line", { class: "p-stop", x1: g.railX[0], y1: g.railY - 3 * U, x2: g.railX[0], y2: g.railY + 0.6 * U, stroke: "var(--fg-faint)", "stroke-width": 1.4 });
      this.eStopR = mk("line", { class: "p-stop", x1: g.railX[1], y1: g.railY - 3 * U, x2: g.railX[1], y2: g.railY + 0.6 * U, stroke: "var(--fg-faint)", "stroke-width": 1.4 });
      this.eL1 = mk("line", { class: "p-link", stroke: "var(--fg)", "stroke-width": 1.5, "stroke-linecap": "round" });
      this.eL2 = mk("line", { class: "p-link", stroke: "var(--fg)", "stroke-width": 1.5, "stroke-linecap": "round" });
      this.eCart = mk("rect", { class: "p-cart", width: 7 * U, height: 2.8 * U, rx: 0.9 * U, y: 0.7 * U, fill: "var(--fg)" });
      this.ePivot = mk("circle", { class: "p-pivot", r: 0.85 * U, cy: 0, fill: "var(--fg)" });
      this.eElbow = mk("circle", { class: "p-bob", r: 1.7 * U, fill: "var(--accent)" });
      this.eTip = mk("circle", { class: "p-bob", r: 2.1 * U, fill: "var(--accent)" });
      this.el.appendChild(this.svg);
      this._render();
    }
    _render() {
      const g = this._geom, S = g.S;
      const w = this.sim.pose();
      const cx = w.cart * S;
      const j2x = w.j2.x * S, j2y = -w.j2.y * S;
      const tx = w.tip.x * S, ty = -w.tip.y * S;
      this.eCart.setAttribute("x", cx - 3.5 * g.U);
      this.ePivot.setAttribute("cx", cx);
      this.eL1.setAttribute("x1", cx); this.eL1.setAttribute("y1", 0);
      this.eL1.setAttribute("x2", j2x); this.eL1.setAttribute("y2", j2y);
      this.eL2.setAttribute("x1", j2x); this.eL2.setAttribute("y1", j2y);
      this.eL2.setAttribute("x2", tx); this.eL2.setAttribute("y2", ty);
      this.eElbow.setAttribute("cx", j2x); this.eElbow.setAttribute("cy", j2y);
      this.eTip.setAttribute("cx", tx); this.eTip.setAttribute("cy", ty);
      if (w.mode !== "balance") {
        this.trail.push(tx.toFixed(1) + "," + ty.toFixed(1));
        if (this.trail.length > 44) this.trail.shift();
      } else if (this.trail.length) {
        this.trail.splice(0, 2); // fade the trail out after a catch
      }
      this.eTrail.setAttribute("points", this.trail.join(" "));
      const gt = this._gate;
      if (gt && gt.x !== this._gateDrawn) {
        this._gateDrawn = gt.x;
        this.eStopR.setAttribute("x1", gt.x);
        this.eStopR.setAttribute("x2", gt.x);
        // the phantom rail runs flush to the fleeing gate at full rail
        // strength — no fade: the line growing from zero length is its own
        // smooth reveal (gate motion near rest is sub-px per frame), and any
        // partial opacity reads as a gap against the solid rail
        this.eGhost.setAttribute("x2", gt.x);
      }
      if (w.mode !== this._mode) {
        this._mode = w.mode;
        this.svg.setAttribute("data-mode", w.mode);
        const bob = w.mode === "balance" ? "var(--accent)" : "var(--pending)";
        this.eElbow.setAttribute("fill", bob);
        this.eTip.setAttribute("fill", bob);
      }
    }
    _frame(t) {
      this._raf = 0;
      if (this.opts.reducedMotion()) { this._park(); return; }
      const dt = this._last ? (t - this._last) / 1000 : 1 / 60;
      this._last = t;
      this.sim.step(dt);
      this._gateStep(dt);
      this._render();
      this._raf = requestAnimationFrame(this._frame);
    }
    // reduced motion: no animation at all — a still pose that is still
    // honest about state (upright when live, hanging when not)
    _park() {
      if (this._raf) cancelAnimationFrame(this._raf);
      this._raf = 0;
      this._last = 0;
      const up = this.sim.connected;
      this.sim.s = up ? [0, 0, 0, 0, 0, 0] : [0, 0, Math.PI, 0, Math.PI, 0];
      this.sim.mode = up ? "balance" : "limp";
      if (this._gate) { this._gate.x = this._gate.rest; this._gate.settledAt = -1; this._gate.v = 0; this._gate.rv = 0; this.sim.xMax = this._gate.restM; }
      this.trail.length = 0;
      this._render();
    }
    // dynamic right gate. the moment balance is lost the physics bound leaps
    // to the cap (the wall turns almost imaginary — mid-page by default — so
    // the planner can run the cart out under the header instead of dying
    // against the "e"), while the DRAWN gate only flees as the cart actually
    // charges it — velocity-led, riding just `min` ahead of the envelope of
    // the cart's excursions (fast out, slow back) and never letting the
    // cart's edge within `min`; the phantom track fills in flush
    // behind it. once the
    // catch is verified and the cart has walked the letter home (a beat of
    // dwell so a wobble can't flutter it), gate and track glide back to the
    // end of the word, the physics bound riding them in.
    _gateStep(dt) {
      const gt = this._gate;
      if (!gt) return;
      dt = clamp(dt, 0, 0.05);   // mirror sim.step: a refocused tab must not teleport the gate
      // a resize shrink that arrived mid-recovery applies as soon as the new
      // cap wouldn't move the drawn gate
      if (gt.pending != null && gt.x <= Math.max(gt.rest, gt.pending)) {
        const px = gt.pending;
        gt.pending = null;
        this.setRailMax(px);
      }
      const S = this._geom.S, U = this._geom.U, sim = this.sim;
      // the bound extends RELATIVE to restM by the gate's travel beyond rest,
      // with `min` reserved so the drawn gate can always clear a cart parked
      // on the extended hard stop without itself crossing `cap`
      const capM = gt.restM + Math.max(0, gt.cap - gt.rest - gt.min) / (1.05 * S);
      const cartEdge = sim.s[0] * S + 3.5 * U;
      const home = sim.mode === "balance" && Math.abs(sim.s[0]) < 0.06 && Math.abs(sim.s[1]) < 0.3;
      if (!home) gt.settledAt = -1;
      else if (gt.settledAt < 0) gt.settledAt = sim.t;
      if (sim.mode !== "balance") sim.xMax = capM;
      if (gt.settledAt >= 0 && sim.t - gt.settledAt > 1.1) {
        // verified catch + the letter stands at home: glide everything back.
        // xMax is set even when the gate is already home — the escort below
        // may have brought it in before the dwell elapsed, and the physics
        // bound must still come home with it
        if (gt.x > gt.rest) {
          // stately ride home: the speed eases in from zero (velocity-
          // continuous with the escort's gentle drift) and tracks
          // distance/1.4 s clamped to [30, 200] px/s — the floor keeps a
          // far gate from dawdling, the ceiling and ease-in keep a
          // half-retracted one from snapping
          const dist = gt.x - gt.rest;
          const wantRate = clamp(dist / 1.4, 30, 200);
          gt.rv += (wantRate - gt.rv) * (1 - Math.exp(-dt / 0.4));
          gt.x -= Math.min(gt.rv * dt, dist);
          if (gt.x - gt.rest < 0.5) gt.x = gt.rest;
        }
        sim.xMax = gt.restM + Math.max(0, gt.x - gt.rest - gt.min) / (1.05 * S);   // same min reserve as capM: xMax rides [restM, capM] exactly
      } else {
        gt.rv = 0;   // any new action re-arms the soft start of the ride home
        // recovering, or walking the catch home: the gate stays put until
        // the cart genuinely approaches, then rides just `min` ahead of it,
        // leading further the faster the rightward charge — the velocity
        // anticipation outruns the smoothing lag, so the hard floor below
        // almost never has to snap (no bulldozing), and a cart nowhere near
        // the gate never moves it at all (no lurch at recovery start).
        // the charge is low-passed and the ease-back rate-capped below, so
        // the gate rides the ENVELOPE of the swing instead of chasing each
        // stroke out and back
        const charge = Math.max(0, sim.s[1]) * S;   // rightward cart speed, px/s
        gt.v += (charge - gt.v) * (1 - Math.exp(-dt / 0.35));
        const want = clamp(cartEdge + gt.min + 0.45 * gt.v, gt.rest, gt.cap);
        // fast out, slow back: the gate rides the envelope of the cart's
        // excursions instead of flapping with every swing
        let dx = (want - gt.x) * (1 - Math.exp(-dt / (want > gt.x ? 0.1 : 2.2)));
        if (dx < 0) dx = Math.max(dx, -22 * dt);   // ease back gently: ride the envelope, don't chase the cart back in
        gt.x += dx;
      }
      // the wall never exceeds the current allowance (a resize shrink can
      // finish applying while balance holds, a window where no branch above
      // rewrites xMax) — and never lands inside the cart itself
      sim.xMax = Math.max(Math.min(sim.xMax, capM), sim.s[0] + 0.02);
      gt.x = clamp(Math.max(gt.x, cartEdge + gt.min), gt.rest, gt.cap);
    }
    /** Re-cap the dynamic right gate (glyph mounts re-measure on resize).
     *  The svg box tracks the cap BOTH ways — an oversized box left behind by
     *  a window shrink would push a horizontal scrollbar. A cap below the
     *  current gate position is held in gt.pending and applied by _gateStep
     *  once the gate is back under it, so nothing on screen ever jumps. */
    setRailMax(px) {
      const gt = this._gate;
      if (!gt || !isFinite(px)) return;
      gt.pending = px < gt.x ? px : null;   // shrink while out: hold until the gate is back
      gt.cap = Math.max(gt.rest, gt.x, px);
      const vb = this.svg.viewBox.baseVal;
      const need = Math.ceil(gt.cap + (this.sim.p.L1 + this.sim.p.L2) * this._geom.S + 4);
      if (need !== vb.x + vb.width) {
        this.svg.setAttribute("viewBox", vb.x + " " + vb.y + " " + (need - vb.x) + " " + vb.height);
        this.svg.setAttribute("width", need - vb.x);
      }
    }
    /** Re-read connected()/reducedMotion() and start/stop the loop. */
    sync() {
      this.sim.setConnected(!!this.opts.connected());
      if (this.opts.reducedMotion()) this._park();
      else if (!this._raf) { this._last = 0; this._raf = requestAnimationFrame(this._frame); }
    }
    poke(k) {
      if (!this.opts.reducedMotion()) this.sim.poke(k);
    }
    /** The cursor is weather: mousemoves over `el` become viscous brushes on
     *  whichever link the pointer grazes — direction, speed and where along
     *  the link it hits all shape the response (see PendulumSim.brush). */
    attachPointer(el) {
      if (this._ptrEl) this.detachPointer();
      const move = (e) => {
        if (this.opts.reducedMotion()) { this._ptrLast = null; return; }
        const r = this.svg.getBoundingClientRect();
        if (!r.width) return;
        const vb = this.svg.viewBox.baseVal;
        const S = this._geom.S, k = vb.width / r.width;
        const px = ((e.clientX - r.left) * k + vb.x) / S;      // sim meters,
        const py = -((e.clientY - r.top) * k + vb.y) / S;      // y up, pivot line = 0
        const t = e.timeStamp / 1000;
        const last = this._ptrLast;
        this._ptrLast = { x: px, y: py, t: t };
        if (!last || t - last.t > 0.1) return;                 // stroke start (or stall): no velocity yet
        const dt = Math.max(1e-3, t - last.t);
        this.sim.brush(last.x, last.y, px, py, dt, Math.max(0.06, 7 / S));
      };
      const leave = () => { this._ptrLast = null; };
      this._ptrEl = el;
      this._ptrFns = [["mousemove", move], ["mouseleave", leave]];
      for (const nf of this._ptrFns) el.addEventListener(nf[0], nf[1]);
    }
    detachPointer() {
      if (!this._ptrEl) return;
      for (const nf of this._ptrFns) this._ptrEl.removeEventListener(nf[0], nf[1]);
      this._ptrEl = null;
      this._ptrFns = null;
      this._ptrLast = null;
    }
    destroy() {
      this.detachPointer();
      if (this._raf) cancelAnimationFrame(this._raf);
      this._raf = 0;
      if (this.svg) this.svg.remove();
    }
  }

  // ---- glyph mount: the pendulum stands in for a letter of the wordmark ----
  // `slot` is an inline element wrapping the letter (the l of "cronstable").
  // The glyph is measured (canvas TextMetrics) and the sim scaled so the
  // balanced pendulum — cart pivot on the baseline, tip at the stem's top —
  // spans the letter exactly, with the svg anchored to the letter's cell.
  // The sim's equilibrium is x = 0, so the controller literally targets the
  // letter's position, while the cart's track runs the full width of the
  // surrounding word — the end gates are the wordmark's outer edges. The
  // printed glyph stays in the DOM for copy/paste, screen readers and no-JS,
  // and is only blanked here, once its live replacement is standing.
  // During a hard recovery the right gate opens out toward mid-page and
  // closes back to the wordmark's edge once the letter stands (railMax).
  CronstableLogo.mountGlyph = function (slot, opts) {
    const cs = getComputedStyle(slot);
    // computed lengths are unzoomed CSS px (standardized zoom does not
    // premultiply getComputedStyle values), so they are already the right
    // measurement space: the UI zoom (Settings → scale) then scales the
    // finished svg together with the text, keeping the l in register
    const fpx = parseFloat(cs.fontSize) || 15;
    const ctx = document.createElement("canvas").getContext("2d");
    if (ctx) ctx.font = cs.fontStyle + " " + cs.fontWeight + " " + fpx + "px " + cs.fontFamily;
    const m = ctx ? ctx.measureText(slot.textContent || "l") : null;
    const stem = (m && m.actualBoundingBoxAscent) || fpx * 0.72;  // glyph height above the baseline
    const asc = (m && m.fontBoundingBoxAscent) || fpx * 0.78;     // font box: locates the baseline
    const desc = (m && m.fontBoundingBoxDescent) || fpx * 0.22;
    const lh = parseFloat(cs.lineHeight) || fpx * 1.5;            // slot line-box height
    const p = CronstableLogo.PARAMS;
    const S = stem / (p.L1 + p.L2), U = S / 28;
    // the track runs the full word: the text around the slot ("cronstab",
    // "e") is measured with the same font, the gates sit at the wordmark's
    // outer ends and the cart's hard stops just inside them — while the
    // balance point stays x = 0, the l's cell.
    let prefix = "", suffix = "";
    for (let n = slot.previousSibling; n; n = n.previousSibling) prefix = (n.textContent || "") + prefix;
    for (let n = slot.nextSibling; n; n = n.nextSibling) suffix += n.textContent || "";
    const ls = parseFloat(cs.letterSpacing) || 0;
    const adv = (t) => (m ? ctx.measureText(t).width + t.length * ls : t.length * fpx * 0.6);
    const slotW = adv(slot.textContent || "l");
    const pad = 3.5 * U + 0.8;   // gate → cart-center clearance (cart half-width + a hair)
    // tuck the neighbors in: the live stem is a hairline, not a full bold
    // glyph, so a whole printed cell reads loose around it. negative margins
    // (applied only here, once the pendulum takes over, so the no-JS printed
    // l keeps exact metrics) pull the flanking letters toward the stem; the
    // slot's content box — and with it x = 0 — stays midway between them.
    const tuck = slotW * 0.12;
    const railL = prefix ? -(adv(prefix) + slotW / 2 - tuck) : -(p.trackHalf * 1.05 * S + pad);
    const railR = suffix ? adv(suffix) + slotW / 2 - ls - tuck : p.trackHalf * 1.05 * S + pad;
    // the dynamic right gate may open out to the middle of the page during
    // a recovery (see _gateStep). Its cap is measured in the svg's own
    // unzoomed px: the slot's rendered width against its measured advance
    // cancels any host UI zoom (rects are zoomed, canvas metrics are not).
    const midPage = () => {
      const r = slot.getBoundingClientRect();
      return r.width > 0 && slotW > 0
        ? (window.innerWidth / 2 - (r.left + r.width / 2)) * (slotW / r.width)
        : railR;
    };
    const logo = new CronstableLogo(slot, Object.assign({
      scale: S,
      ui: U,
      railX: [railL, railR],
      railMax: Math.max(railR, midPage()),
      params: { trackMin: (railL + pad) / (1.05 * S), trackMax: (railR - pad) / (1.05 * S) },
      decorative: true,                               // the wordmark itself already reads "cronstable"
    }, opts));
    slot.style.position = "relative";
    slot.style.display = "inline-block";
    const svg = logo.svg;
    const vb = svg.viewBox.baseVal;
    svg.style.position = "absolute";
    svg.style.pointerEvents = "none";
    svg.style.left = "calc(50% + " + vb.x + "px)";
    svg.style.top = ((lh - asc - desc) / 2 + asc - vb.height / 2).toFixed(2) + "px";
    slot.style.color = "transparent";   // the printed l yields to the live one
    slot.style.textShadow = "none";     // (a transparent glyph still casts its text-shadow glow)
    slot.style.margin = "0 " + (-tuck).toFixed(2) + "px";
    // the mid-page cap follows the window; measured once more right away,
    // because the tuck margins just shifted the slot under the first reading
    const recap = () => logo.setRailMax(Math.max(railR, midPage()));
    recap();
    window.addEventListener("resize", recap);
    const bye = logo.destroy.bind(logo);
    logo.destroy = () => { window.removeEventListener("resize", recap); bye(); };
    return logo;
  };

  CronstableLogo.Sim = PendulumSim;
  CronstableLogo.PARAMS = DP;
  window.CronstableLogo = CronstableLogo;
})();
