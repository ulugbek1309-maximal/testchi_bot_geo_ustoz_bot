/**
 * ============================================================
 *  TESTCHI — GLOBAL ANIMATION SYSTEM  v3.0  🚀
 *  Yangiliklar:
 *    1.  Performance Detection (avvalgi)
 *    2.  Page Transitions (avvalgi)
 *    3.  Ripple Effect (avvalgi)
 *    4.  Toast System (avvalgi)
 *    5.  Card Appear — Intersection Observer (avvalgi)
 *    6.  Navbar Scroll Hide (avvalgi)
 *    7.  Question Slide — solve_test (avvalgi)
 *    8.  CountUp Animation (avvalgi)
 *    ── YANGI ──
 *    9.  Floating Particles — fon zarralar
 *    10. Neon Glow Pulse — kartalar neon yaltirashi
 *    11. Magnetic Hover — kursor tortish effekti
 *    12. Konfetti — test yakunida yomg'ir
 *    13. Glassmorphism Shimmer — shisha yaltirash
 *    14. Emoji Float — emoji lar suzib chiqishi
 *    15. Score Ring — progress ring animatsiyasi
 *    16. Tilt 3D — kartalar 3D burilishi
 *    17. Gradient Text Animate — rang almashuvi
 *    18. Stagger List — ro'yxat ketma-ket paydo bo'lish
 * ============================================================
 */

(function () {
  'use strict';

  /* ═══════════════════════════════════════════════════════════
     1. PERFORMANCE DETECTION
  ═══════════════════════════════════════════════════════════ */
  const PERF_KEY = 'testchi_perf';
  let animEnabled = true;

  function detectPerformance() {
    const saved = localStorage.getItem(PERF_KEY);
    if (saved !== null) { animEnabled = saved === '1'; applyPerfMode(); return; }
    const cores = navigator.hardwareConcurrency || 2;
    const mem   = navigator.deviceMemory || 4;
    if (cores <= 2 || mem <= 2) {
      animEnabled = false; localStorage.setItem(PERF_KEY, '0'); applyPerfMode(); return;
    }
    let frames = 0; const start = performance.now();
    function countFrame() {
      frames++;
      if (performance.now() - start < 300) { requestAnimationFrame(countFrame); }
      else {
        animEnabled = Math.round(frames / 0.3) >= 30;
        localStorage.setItem(PERF_KEY, animEnabled ? '1' : '0');
        applyPerfMode();
        if (animEnabled) lateInit();
      }
    }
    requestAnimationFrame(countFrame);
    applyPerfMode();
  }

  function applyPerfMode() {
    document.documentElement.classList.toggle('no-anim', !animEnabled);
  }

  /* ═══════════════════════════════════════════════════════════
     2. GLOBAL CSS INJECTION
  ═══════════════════════════════════════════════════════════ */
  function injectStyles() {
    if (document.getElementById('testchi-anim-style')) return;
    const s = document.createElement('style');
    s.id = 'testchi-anim-style';
    s.textContent = `
/* ─── PERF OFF ─── */
html.no-anim *, html.no-anim *::before, html.no-anim *::after {
  animation-duration: 0.001ms !important;
  transition-duration: 0.001ms !important;
}

/* ─── BODY ENTER ─── */
body { animation: bodyFadeIn 0.4s ease both; }
@keyframes bodyFadeIn {
  from { opacity: 0; transform: translateY(8px); }
  to   { opacity: 1; transform: translateY(0); }
}

/* ─── PAGE TRANSITION OVERLAY ─── */
#page-transition-overlay {
  position: fixed; inset: 0; z-index: 99999;
  background: radial-gradient(circle at center, #0d1528 0%, #050810 100%);
  display: flex; align-items: center; justify-content: center;
  pointer-events: none; opacity: 0;
  transition: opacity 0.3s cubic-bezier(0.4,0,0.2,1);
}
#page-transition-overlay.active { opacity: 1; pointer-events: all; }
#page-transition-overlay .logo-spin {
  font-size: 40px;
  animation: ptSpin 0.7s ease-in-out infinite alternate;
}
@keyframes ptSpin {
  0%   { transform: scale(0.7) rotate(-12deg); opacity: 0.5; }
  100% { transform: scale(1.3) rotate(12deg);  opacity: 1; }
}

/* ─── RIPPLE ─── */
.ripple-host { position: relative; overflow: hidden; }
.ripple-wave {
  position: absolute; border-radius: 50%;
  background: rgba(255,255,255,0.22);
  transform: scale(0); pointer-events: none;
  animation: rippleAnim 0.6s ease-out forwards;
}
@keyframes rippleAnim { to { transform: scale(4.5); opacity: 0; } }

/* ─── CARD APPEAR ─── */
.anim-card {
  opacity: 0; transform: translateY(24px) scale(0.96);
  transition: opacity 0.45s cubic-bezier(0.22,1,0.36,1),
              transform 0.45s cubic-bezier(0.22,1,0.36,1);
}
.anim-card.visible { opacity: 1; transform: translateY(0) scale(1); }

/* ─── HOVER LIFT ─── */
nav a, .btn, .test-card, .card, button:not(.no-lift) {
  transition: transform 0.2s cubic-bezier(0.34,1.56,0.64,1),
              box-shadow 0.2s ease,
              background 0.2s ease,
              border-color 0.2s ease !important;
}
nav a:hover    { transform: translateY(-2px) scale(1.04) !important; }
.test-card:hover {
  transform: translateY(-6px) scale(1.03) !important;
  box-shadow: 0 20px 55px rgba(0,0,0,0.5) !important;
}
.btn:hover:not(:disabled) {
  transform: translateY(-2px) scale(1.025) !important;
  box-shadow: 0 10px 28px rgba(0,0,0,0.35) !important;
}
button:not(.no-lift):hover:not(:disabled) { transform: translateY(-1px) !important; }
.key:active { transform: scale(0.86) !important; transition-duration: 0.07s !important; }

/* ─── TOAST ─── */
#toast-container {
  position: fixed; bottom: 28px; left: 50%; transform: translateX(-50%);
  z-index: 99990; display: flex; flex-direction: column-reverse;
  align-items: center; gap: 10px; pointer-events: none;
}
.toast {
  background: rgba(18,26,48,0.97); backdrop-filter: blur(20px);
  border: 1px solid rgba(255,255,255,0.14);
  color: #f3f6ff; border-radius: 18px; padding: 14px 24px;
  font-size: 14px; font-weight: 500;
  box-shadow: 0 14px 40px rgba(0,0,0,0.5);
  display: flex; align-items: center; gap: 10px;
  animation: toastIn 0.38s cubic-bezier(0.22,1,0.36,1) forwards;
  max-width: 90vw; pointer-events: all;
}
.toast.hide { animation: toastOut 0.3s ease forwards; }
.toast.success { border-left: 3px solid #38d39f; }
.toast.error   { border-left: 3px solid #ff6b6b; }
.toast.info    { border-left: 3px solid #6cb2ff; }
.toast.warning { border-left: 3px solid #fbbf24; }
@keyframes toastIn  { from { opacity:0; transform:translateY(22px) scale(0.9); } to { opacity:1; transform:translateY(0) scale(1); } }
@keyframes toastOut { to   { opacity:0; transform:translateY(16px) scale(0.88); } }

/* ─── MODAL ─── */
.modal[style*="flex"], .modal[style*="block"] { animation: modalIn 0.3s cubic-bezier(0.22,1,0.36,1); }
@keyframes modalIn { from { opacity:0; } to { opacity:1; } }
.modal-content, .modal .modal-content {
  animation: modalSlide 0.32s cubic-bezier(0.22,1,0.36,1) both;
}
@keyframes modalSlide {
  from { transform: scale(0.9) translateY(20px); opacity:0; }
  to   { transform: scale(1)   translateY(0);    opacity:1; }
}

/* ─── SPINNER ─── */
.btn.loading::after {
  content: ''; display: inline-block; width: 14px; height: 14px;
  border: 2px solid rgba(255,255,255,0.3); border-top-color: #fff;
  border-radius: 50%; animation: spin 0.65s linear infinite;
  margin-left: 8px; vertical-align: middle;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* ─── DOT PULSE ─── */
.dot.filled { animation: dotPulse 0.28s cubic-bezier(0.34,1.56,0.64,1) !important; }
@keyframes dotPulse { 0%{transform:scale(0.5)} 60%{transform:scale(1.3)} 100%{transform:scale(1.1)} }

/* ─── OPTION LABEL ─── */
.option-label {
  transition: background 0.18s ease, border-color 0.18s ease,
              transform 0.2s cubic-bezier(0.34,1.56,0.64,1),
              box-shadow 0.18s ease !important;
}
.option-label:hover  { transform: translateX(5px) !important; }
.option-label.selected { transform: translateX(5px) scale(1.015) !important; }

/* ─── PROGRESS BAR ─── */
.progress-bar-fill { transition: width 0.55s cubic-bezier(0.22,1,0.36,1) !important; }

/* ─── QUESTION SLIDE ─── */
#questionCard.slide-in  { animation: slideIn  0.32s cubic-bezier(0.22,1,0.36,1) both; }
#questionCard.slide-out { animation: slideOut 0.22s ease-in both; }
@keyframes slideIn  { from { opacity:0; transform:translateX(35px); } to { opacity:1; transform:translateX(0); } }
@keyframes slideOut { from { opacity:1; transform:translateX(0); }    to { opacity:0; transform:translateX(-35px); } }

/* ─── SCORE CIRCLE ─── */
.score-circle { animation: scoreAppear 0.8s cubic-bezier(0.22,1,0.36,1) both; }
@keyframes scoreAppear { from { transform:scale(0.3) rotate(-15deg); opacity:0; } to { transform:scale(1) rotate(0); opacity:1; } }

/* ─── NAVBAR SCROLL ─── */
nav { transition: transform 0.3s ease !important; }
nav.nav-hidden { transform: translateY(-100%) !important; }

/* ─── HERO TEXT ─── */
.hero h2, main > h2, .hero > h2 {
  animation: heroTitle 0.55s cubic-bezier(0.22,1,0.36,1) both;
  animation-delay: 0.06s;
}
@keyframes heroTitle { from { opacity:0; transform:translateY(12px); } to { opacity:1; transform:translateY(0); } }

/* ─── SESSION ITEM ─── */
.session-item {
  transition: background 0.18s ease, color 0.18s ease,
              transform 0.2s cubic-bezier(0.34,1.56,0.64,1) !important;
}
.session-item:hover { transform: translateX(5px) !important; }

/* ══════════════════════════════════════
   ✨ YANGI — FLOATING PARTICLES
══════════════════════════════════════ */
#tc-particles-canvas {
  position: fixed; inset: 0; pointer-events: none;
  z-index: 0; opacity: 0.55;
}

/* ══════════════════════════════════════
   ✨ YANGI — NEON GLOW PULSE
══════════════════════════════════════ */
@keyframes neonPulse {
  0%,100% { box-shadow: 0 0 0px rgba(56,211,159,0); }
  50%     { box-shadow: 0 0 22px rgba(56,211,159,0.35), 0 0 55px rgba(56,211,159,0.12); }
}
.neon-pulse { animation: neonPulse 3s ease-in-out infinite; }

@keyframes neonPulseBlue {
  0%,100% { box-shadow: 0 0 0px rgba(108,178,255,0); }
  50%     { box-shadow: 0 0 20px rgba(108,178,255,0.35), 0 0 50px rgba(108,178,255,0.12); }
}
.neon-pulse-blue { animation: neonPulseBlue 3.2s ease-in-out infinite; }

/* ══════════════════════════════════════
   ✨ YANGI — SHIMMER SWEEP
══════════════════════════════════════ */
@keyframes shimmerSweep {
  0%   { background-position: -200% center; }
  100% { background-position:  200% center; }
}
.shimmer-text {
  background: linear-gradient(
    90deg,
    #38d39f 0%, #6cb2ff 25%, #b794f4 50%, #6cb2ff 75%, #38d39f 100%
  );
  background-size: 200% auto;
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  animation: shimmerSweep 3.5s linear infinite;
}

/* ══════════════════════════════════════
   ✨ YANGI — KONFETTI
══════════════════════════════════════ */
#tc-confetti-canvas {
  position: fixed; inset: 0; pointer-events: none; z-index: 99998;
}

/* ══════════════════════════════════════
   ✨ YANGI — EMOJI FLOAT
══════════════════════════════════════ */
.tc-emoji-float {
  position: fixed; pointer-events: none; z-index: 9999;
  font-size: 22px; user-select: none;
  animation: emojiFly 1.8s cubic-bezier(0.22,1,0.36,1) forwards;
}
@keyframes emojiFly {
  0%   { opacity:1; transform: translateY(0) scale(1) rotate(0deg); }
  60%  { opacity:1; transform: translateY(-80px) scale(1.3) rotate(15deg); }
  100% { opacity:0; transform: translateY(-140px) scale(0.6) rotate(-10deg); }
}

/* ══════════════════════════════════════
   ✨ YANGI — SCORE RING (SVG)
══════════════════════════════════════ */
.score-ring-svg { transform: rotate(-90deg); }
.score-ring-track { fill: none; stroke: rgba(255,255,255,0.1); }
.score-ring-fill {
  fill: none;
  stroke-linecap: round;
  stroke-dasharray: 440;
  stroke-dashoffset: 440;
  transition: stroke-dashoffset 1.4s cubic-bezier(0.22,1,0.36,1);
}

/* ══════════════════════════════════════
   ✨ YANGI — TILT 3D (JS boshqaradi)
══════════════════════════════════════ */
.tilt-card {
  transition: transform 0.12s ease, box-shadow 0.12s ease !important;
  will-change: transform;
  transform-style: preserve-3d;
}

/* ══════════════════════════════════════
   ✨ YANGI — GRADIENT BG ANIMATE
══════════════════════════════════════ */
@keyframes gradientShift {
  0%   { background-position: 0% 50%; }
  50%  { background-position: 100% 50%; }
  100% { background-position: 0% 50%; }
}
.gradient-bg-anim {
  background: linear-gradient(135deg, #090c14, #101b34, #0d1a2e, #070b15);
  background-size: 400% 400%;
  animation: gradientShift 12s ease infinite;
}

/* ══════════════════════════════════════
   ✨ YANGI — STAGGER LIST
══════════════════════════════════════ */
.stagger-item {
  opacity: 0; transform: translateX(-18px);
  transition: opacity 0.38s ease, transform 0.38s cubic-bezier(0.22,1,0.36,1);
}
.stagger-item.stagger-visible { opacity:1; transform: translateX(0); }

/* ══════════════════════════════════════
   ✨ YANGI — GLOW BORDER
══════════════════════════════════════ */
@keyframes glowBorder {
  0%,100% { border-color: rgba(56,211,159,0.2); }
  50%      { border-color: rgba(56,211,159,0.7); }
}
.glow-border { animation: glowBorder 2.5s ease-in-out infinite; }

/* ══════════════════════════════════════
   ✨ YANGI — WAVE LOADER
══════════════════════════════════════ */
.wave-loader { display:inline-flex; gap:4px; align-items:flex-end; }
.wave-loader span {
  display:inline-block; width:4px; background:#38d39f; border-radius:2px;
  animation: waveBar 1s ease-in-out infinite;
}
.wave-loader span:nth-child(1) { height:8px;  animation-delay:0s; }
.wave-loader span:nth-child(2) { height:16px; animation-delay:0.1s; }
.wave-loader span:nth-child(3) { height:12px; animation-delay:0.2s; }
.wave-loader span:nth-child(4) { height:20px; animation-delay:0.3s; }
.wave-loader span:nth-child(5) { height:10px; animation-delay:0.4s; }
@keyframes waveBar {
  0%,100% { transform: scaleY(1); opacity:0.7; }
  50%      { transform: scaleY(1.8); opacity:1; }
}

/* ══════════════════════════════════════
   ✨ YANGI — TYPEWRITER CURSOR
══════════════════════════════════════ */
.typewriter::after {
  content: '|'; animation: blink 1s step-end infinite;
  color: #38d39f; font-weight: 300;
}
@keyframes blink { 50% { opacity:0; } }

/* ══════════════════════════════════════
   ✨ YANGI — MAGNETIC BTN glow
══════════════════════════════════════ */
.btn-magnetic {
  position: relative; overflow: hidden;
}
.btn-magnetic::before {
  content: '';
  position: absolute; inset: -2px;
  background: linear-gradient(45deg, #38d39f, #6cb2ff, #b794f4, #38d39f);
  background-size: 300% 300%;
  border-radius: inherit;
  z-index: -1; opacity: 0;
  animation: gradientShift 3s ease infinite;
  transition: opacity 0.3s ease;
}
.btn-magnetic:hover::before { opacity: 1; }

/* ══════════════════════════════════════
   ✨ YANGI — FLOATING BADGE
══════════════════════════════════════ */
@keyframes floatBadge {
  0%,100% { transform: translateY(0px); }
  50%     { transform: translateY(-6px); }
}
.float-badge { animation: floatBadge 2.8s ease-in-out infinite; }

/* ══════════════════════════════════════
   ✨ YANGI — NUMBER COUNT RING
══════════════════════════════════════ */
@keyframes ringFill {
  from { stroke-dashoffset: 440; }
}

/* ─── BADGE PULSE ─── */
@keyframes badgePulse { 0%,100%{transform:scale(1)} 50%{transform:scale(1.25)} }

/* ─── SKELETON LOADING ─── */
@keyframes skeletonShimmer {
  0%   { background-position: -400px 0; }
  100% { background-position:  400px 0; }
}
.skeleton {
  background: linear-gradient(90deg, rgba(255,255,255,0.04) 25%, rgba(255,255,255,0.1) 50%, rgba(255,255,255,0.04) 75%);
  background-size: 400px 100%;
  animation: skeletonShimmer 1.5s infinite;
  border-radius: 8px;
}
    `;
    document.head.appendChild(s);
  }

  /* ═══════════════════════════════════════════════════════════
     3. PAGE TRANSITIONS
  ═══════════════════════════════════════════════════════════ */
  function setupPageTransitions() {
    if (!animEnabled) return;
    let overlay = document.getElementById('page-transition-overlay');
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.id = 'page-transition-overlay';
      overlay.innerHTML = '<span class="logo-spin">🎓</span>';
      document.body.appendChild(overlay);
    }
    document.addEventListener('click', function (e) {
      const link = e.target.closest('a');
      if (!link) return;
      const href = link.getAttribute('href');
      if (!href) return;
      if (href.startsWith('http') || href.startsWith('#') || href.startsWith('javascript') ||
          href.startsWith('mailto') || link.target === '_blank' || e.ctrlKey || e.metaKey || e.shiftKey) return;
      if (link.closest('form')) return;
      e.preventDefault();
      overlay.classList.add('active');
      setTimeout(() => { window.location.href = href; }, 300);
    });
  }

  /* ═══════════════════════════════════════════════════════════
     4. RIPPLE EFFECT
  ═══════════════════════════════════════════════════════════ */
  function setupRipple() {
    const SEL = 'a, button, .btn, .card.test-card, .key, .option-label, .star-option, .tab, .session-item, .list > li, .user-card, .new-chat-btn';
    document.addEventListener('pointerdown', function (e) {
      if (!animEnabled) return;
      const el = e.target.closest(SEL);
      if (!el) return;
      el.classList.add('ripple-host');
      const rect = el.getBoundingClientRect();
      const size = Math.max(rect.width, rect.height) * 2.2;
      const wave = document.createElement('span');
      wave.className = 'ripple-wave';
      wave.style.cssText = `width:${size}px;height:${size}px;left:${e.clientX-rect.left-size/2}px;top:${e.clientY-rect.top-size/2}px;`;
      el.appendChild(wave);
      wave.addEventListener('animationend', () => wave.remove(), { once: true });
    });
  }

  /* ═══════════════════════════════════════════════════════════
     5. TOAST SYSTEM
  ═══════════════════════════════════════════════════════════ */
  window.showToast = function (message, type = 'info', duration = 3500) {
    let container = document.getElementById('toast-container');
    if (!container) {
      container = document.createElement('div');
      container.id = 'toast-container';
      document.body.appendChild(container);
    }
    const icons = { success: '✅', error: '❌', info: 'ℹ️', warning: '⚠️' };
    const toast  = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `<span>${icons[type] || 'ℹ️'}</span><span>${message}</span>`;
    container.appendChild(toast);
    setTimeout(() => {
      toast.classList.add('hide');
      toast.addEventListener('animationend', () => toast.remove(), { once: true });
    }, duration);
  };

  const _nativeAlert = window.alert.bind(window);
  window.alert = function (msg) {
    if (typeof msg !== 'string') msg = String(msg);
    const l = msg.toLowerCase();
    let type = 'info';
    if (l.includes('✅') || l.includes('muvaffaqiyat') || l.includes('успешно') || l.includes('saqlandi')) type = 'success';
    else if (l.includes('❌') || l.includes('xato') || l.includes('ошибка') || l.includes('error') || l.includes('xatolik')) type = 'error';
    else if (l.includes('⚠️') || l.includes('ogohlantirish') || l.includes('внимание') || l.includes('warning')) type = 'warning';
    showToast(msg, type);
  };

  /* ═══════════════════════════════════════════════════════════
     6. CARD APPEAR — Intersection Observer
  ═══════════════════════════════════════════════════════════ */
  function setupCardAppear() {
    if (!animEnabled) return;
    const selector = '.card, .test-card, .user-card, .payment-item, .test-item, .question-block, .session-item, .bubble, .rank-item';
    const observer = new IntersectionObserver((entries) => {
      entries.forEach((entry, i) => {
        if (entry.isIntersecting) {
          setTimeout(() => entry.target.classList.add('visible'), Math.min(i * 60, 450));
          observer.unobserve(entry.target);
        }
      });
    }, { threshold: 0.08, rootMargin: '0px 0px -28px 0px' });
    document.querySelectorAll(selector).forEach(el => {
      el.classList.add('anim-card');
      observer.observe(el);
    });
  }

  /* ═══════════════════════════════════════════════════════════
     7. NAVBAR SCROLL
  ═══════════════════════════════════════════════════════════ */
  function setupNavbarScroll() {
    const nav = document.querySelector('nav');
    if (!nav || !animEnabled) return;
    let lastY = window.scrollY, ticking = false;
    window.addEventListener('scroll', () => {
      if (!ticking) {
        requestAnimationFrame(() => {
          const cur = window.scrollY;
          nav.classList.toggle('nav-hidden', cur > lastY && cur > 80);
          lastY = cur; ticking = false;
        });
        ticking = true;
      }
    }, { passive: true });
  }

  /* ═══════════════════════════════════════════════════════════
     8. QUESTION SLIDE
  ═══════════════════════════════════════════════════════════ */
  window.animateQuestionSlide = function (direction, callback) {
    if (!animEnabled) { callback(); return; }
    const card = document.getElementById('questionCard');
    if (!card) { callback(); return; }
    card.classList.add('slide-out');
    setTimeout(() => {
      card.classList.remove('slide-out');
      callback();
      card.classList.add('slide-in');
      setTimeout(() => card.classList.remove('slide-in'), 340);
    }, 230);
  };

  /* ═══════════════════════════════════════════════════════════
     9. COUNT UP
  ═══════════════════════════════════════════════════════════ */
  window.animateCountUp = function (el, target, duration = 1000) {
    if (!animEnabled || !el) { el && (el.textContent = target); return; }
    let startTime = null;
    const step = (ts) => {
      if (!startTime) startTime = ts;
      const p = Math.min((ts - startTime) / duration, 1);
      el.textContent = Math.round(p * target);
      if (p < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  };

  /* ═══════════════════════════════════════════════════════════
     10. ✨ FLOATING PARTICLES (Canvas)
  ═══════════════════════════════════════════════════════════ */
  function setupFloatingParticles() {
    if (!animEnabled) return;
    const existing = document.getElementById('tc-particles-canvas');
    if (existing) return;

    const canvas = document.createElement('canvas');
    canvas.id = 'tc-particles-canvas';
    document.body.insertBefore(canvas, document.body.firstChild);

    const ctx = canvas.getContext('2d');
    let W, H, particles;

    const COLORS = ['rgba(56,211,159,', 'rgba(108,178,255,', 'rgba(183,148,244,', 'rgba(251,191,36,'];

    function resize() {
      W = canvas.width  = window.innerWidth;
      H = canvas.height = window.innerHeight;
    }

    function createParticle() {
      return {
        x: Math.random() * W,
        y: Math.random() * H,
        r: Math.random() * 1.8 + 0.4,
        color: COLORS[Math.floor(Math.random() * COLORS.length)],
        vx: (Math.random() - 0.5) * 0.4,
        vy: (Math.random() - 0.5) * 0.4,
        alpha: Math.random() * 0.5 + 0.1,
        alphaDir: Math.random() > 0.5 ? 1 : -1,
        alphaSpeed: Math.random() * 0.004 + 0.001,
      };
    }

    function init() {
      resize();
      const count = Math.min(Math.floor((W * H) / 18000), 80);
      particles = Array.from({ length: count }, createParticle);
    }

    function draw() {
      ctx.clearRect(0, 0, W, H);
      for (const p of particles) {
        p.x += p.vx; p.y += p.vy;
        p.alpha += p.alphaDir * p.alphaSpeed;
        if (p.alpha >= 0.6 || p.alpha <= 0.05) p.alphaDir *= -1;
        if (p.x < -5) p.x = W + 5;
        if (p.x > W + 5) p.x = -5;
        if (p.y < -5) p.y = H + 5;
        if (p.y > H + 5) p.y = -5;

        ctx.beginPath();
        ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
        ctx.fillStyle = p.color + p.alpha + ')';
        ctx.fill();
      }
      requestAnimationFrame(draw);
    }

    window.addEventListener('resize', () => { resize(); init(); }, { passive: true });
    init();
    draw();
  }

  /* ═══════════════════════════════════════════════════════════
     11. ✨ NEON GLOW — Muhim kartalar
  ═══════════════════════════════════════════════════════════ */
  function applyNeonGlow() {
    if (!animEnabled) return;
    // AI chat kartasi — yashil neon
    document.querySelectorAll('a[href*="ai-chat"], a[href*="/ai-chat"]').forEach(el => {
      el.classList.add('neon-pulse');
    });
    // Premium kartasi — ko'k neon
    document.querySelectorAll('a[href*="premium"], a[href*="buy-premium"]').forEach(el => {
      el.classList.add('neon-pulse-blue');
    });
    // Score circle ham yonsin
    document.querySelectorAll('.score-circle').forEach(el => {
      el.classList.add('neon-pulse');
    });
  }

  /* ═══════════════════════════════════════════════════════════
     12. ✨ MAGNETIC HOVER — Kartalar kursorga tortiladi
  ═══════════════════════════════════════════════════════════ */
  function setupMagneticHover() {
    if (!animEnabled) return;
    const cards = document.querySelectorAll('.test-card');
    cards.forEach(card => {
      card.classList.add('tilt-card');

      card.addEventListener('mousemove', function (e) {
        const rect = this.getBoundingClientRect();
        const cx   = rect.left + rect.width  / 2;
        const cy   = rect.top  + rect.height / 2;
        const dx   = (e.clientX - cx) / (rect.width  / 2);
        const dy   = (e.clientY - cy) / (rect.height / 2);
        const rotX = dy * -8;   // max 8deg
        const rotY = dx *  8;
        const glow = `radial-gradient(circle at ${(dx+1)/2*100}% ${(dy+1)/2*100}%, rgba(255,255,255,0.06), transparent 70%)`;

        this.style.transform     = `perspective(600px) rotateX(${rotX}deg) rotateY(${rotY}deg) translateZ(4px) scale(1.02)`;
        this.style.boxShadow     = `${-dx*12}px ${-dy*12}px 30px rgba(0,0,0,0.3), 0 0 30px rgba(56,211,159,0.08)`;
        this.style.backgroundImage = glow;
      });

      card.addEventListener('mouseleave', function () {
        this.style.transform       = '';
        this.style.boxShadow       = '';
        this.style.backgroundImage = '';
      });
    });
  }

  /* ═══════════════════════════════════════════════════════════
     13. ✨ KONFETTI — Test yakunlanganda
  ═══════════════════════════════════════════════════════════ */
  window.launchConfetti = function (duration = 3000) {
    if (!animEnabled) return;
    let canvas = document.getElementById('tc-confetti-canvas');
    if (!canvas) {
      canvas = document.createElement('canvas');
      canvas.id = 'tc-confetti-canvas';
      document.body.appendChild(canvas);
    }
    const ctx = canvas.getContext('2d');
    canvas.width  = window.innerWidth;
    canvas.height = window.innerHeight;

    const COLORS = ['#38d39f','#6cb2ff','#f6c90e','#ff6b6b','#b794f4','#fbbf24','#34d399','#60a5fa'];
    const pieces = Array.from({ length: 140 }, () => ({
      x: Math.random() * canvas.width,
      y: -20,
      w: Math.random() * 10 + 4,
      h: Math.random() * 6  + 3,
      color: COLORS[Math.floor(Math.random() * COLORS.length)],
      vx: (Math.random() - 0.5) * 5,
      vy: Math.random() * 4 + 2,
      rot: Math.random() * 360,
      rotSpeed: (Math.random() - 0.5) * 8,
    }));

    const end = Date.now() + duration;
    function animate() {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      for (const p of pieces) {
        p.x   += p.vx;
        p.y   += p.vy;
        p.vy  += 0.12; // gravity
        p.rot += p.rotSpeed;
        ctx.save();
        ctx.translate(p.x, p.y);
        ctx.rotate((p.rot * Math.PI) / 180);
        ctx.fillStyle = p.color;
        ctx.fillRect(-p.w/2, -p.h/2, p.w, p.h);
        ctx.restore();
      }
      if (Date.now() < end) requestAnimationFrame(animate);
      else {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        canvas.remove();
      }
    }
    animate();

    // Tepadan yangi to'lqin
    setTimeout(() => {
      for (const p of pieces) {
        p.x = Math.random() * canvas.width;
        p.y = -20;
        p.vx = (Math.random() - 0.5) * 6;
        p.vy = Math.random() * 3 + 1;
      }
    }, duration / 2);
  };

  /* ═══════════════════════════════════════════════════════════
     14. ✨ EMOJI FLOAT — Bosish yoki event da
  ═══════════════════════════════════════════════════════════ */
  window.floatEmoji = function (emoji, x, y) {
    if (!animEnabled) return;
    const el = document.createElement('span');
    el.className = 'tc-emoji-float';
    el.textContent = emoji;
    el.style.left = (x - 12) + 'px';
    el.style.top  = (y - 12) + 'px';
    document.body.appendChild(el);
    el.addEventListener('animationend', () => el.remove(), { once: true });
  };

  // Correct option tanlanganda emojilar chiqsin
  function setupEmojiOnCorrect() {
    document.addEventListener('click', function (e) {
      if (!animEnabled) return;
      const opt = e.target.closest('.option-label');
      if (!opt) return;
      if (Math.random() < 0.4) { // 40% ehtimol
        const emojis = ['✨','⚡','🎯','💫','🌟','🔥'];
        floatEmoji(emojis[Math.floor(Math.random() * emojis.length)], e.clientX, e.clientY);
      }
    });
  }

  /* ═══════════════════════════════════════════════════════════
     15. ✨ SCORE RING — SVG progress ring
  ═══════════════════════════════════════════════════════════ */
  window.buildScoreRing = function (containerId, score, maxScore, color = '#38d39f') {
    const container = document.getElementById(containerId);
    if (!container) return;
    const R = 70, C = 2 * Math.PI * R; // circumference
    const pct = Math.min(score / maxScore, 1);
    const offset = C * (1 - pct);

    container.innerHTML = `
      <svg class="score-ring-svg" width="160" height="160" viewBox="0 0 160 160">
        <circle class="score-ring-track" cx="80" cy="80" r="${R}" stroke-width="10"/>
        <circle class="score-ring-fill" id="sr-fill"
          cx="80" cy="80" r="${R}"
          stroke="${color}" stroke-width="10"
          stroke-dasharray="${C}"
          stroke-dashoffset="${C}"/>
      </svg>
      <div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);text-align:center;">
        <div id="sr-num" style="font-size:32px;font-weight:800;color:${color}">0</div>
        <div style="font-size:12px;color:rgba(243,246,255,0.6)">/ ${maxScore}</div>
      </div>
    `;
    container.style.position = 'relative';
    container.style.display  = 'inline-block';

    setTimeout(() => {
      const fill = document.getElementById('sr-fill');
      const num  = document.getElementById('sr-num');
      if (fill) fill.style.strokeDashoffset = offset;
      if (num) animateCountUp(num, score, 1200);
    }, 80);
  };

  /* ═══════════════════════════════════════════════════════════
     16. ✨ GLASSMORPHISM SHIMMER — Kartalar ustida yaltirash
  ═══════════════════════════════════════════════════════════ */
  function setupGlassShimmer() {
    if (!animEnabled) return;
    document.querySelectorAll('.card, .test-card').forEach(card => {
      // Shimmer overlay
      const shine = document.createElement('div');
      shine.style.cssText = `
        position:absolute; inset:0; border-radius:inherit;
        background: linear-gradient(105deg, transparent 40%, rgba(255,255,255,0.04) 50%, transparent 60%);
        background-size: 200% 100%; opacity:0; pointer-events:none;
        transition: opacity 0.2s ease; z-index: 1;
      `;
      if (getComputedStyle(card).position === 'static') card.style.position = 'relative';
      card.appendChild(shine);

      card.addEventListener('mouseenter', () => { shine.style.opacity = '1'; });
      card.addEventListener('mouseleave', () => { shine.style.opacity = '0'; });
      card.addEventListener('mousemove', (e) => {
        const r   = card.getBoundingClientRect();
        const pct = ((e.clientX - r.left) / r.width) * 100;
        shine.style.backgroundPosition = `${pct}% center`;
      });
    });
  }

  /* ═══════════════════════════════════════════════════════════
     17. ✨ STAGGER LIST — ro'yxat ketma-ket paydo bo'ladi
  ═══════════════════════════════════════════════════════════ */
  function setupStaggerList() {
    if (!animEnabled) return;
    const selector = '.list li, .rank-item, ul.session-list .session-item';
    document.querySelectorAll(selector).forEach((el, i) => {
      el.classList.add('stagger-item');
      setTimeout(() => el.classList.add('stagger-visible'), 80 + i * 65);
    });
  }

  /* ═══════════════════════════════════════════════════════════
     18. ✨ GRADIENT BG — Body ga qo'shish
  ═══════════════════════════════════════════════════════════ */
  function applyGradientBg() {
    // Agar fon tasviri yo'q bo'lsa gradient qo'sh
    const bodyBg = document.body.style.background || document.body.style.backgroundImage;
    if (!bodyBg || bodyBg.trim() === '') {
      document.body.classList.add('gradient-bg-anim');
    }
  }

  /* ═══════════════════════════════════════════════════════════
     19. TOGGLE ANIMATIONS
  ═══════════════════════════════════════════════════════════ */
  window.toggleAnimations = function () {
    animEnabled = !animEnabled;
    localStorage.setItem(PERF_KEY, animEnabled ? '1' : '0');
    applyPerfMode();
    showToast(
      animEnabled ? '✨ Animatsiyalar yoqildi' : '⚡ Animatsiyalar o\'chirildi (tezlashtirildi)',
      animEnabled ? 'success' : 'info'
    );
  };

  /* ═══════════════════════════════════════════════════════════
     INIT
  ═══════════════════════════════════════════════════════════ */
  function domReady(fn) {
    if (document.readyState !== 'loading') fn();
    else document.addEventListener('DOMContentLoaded', fn);
  }

  function mainInit() {
    setupPageTransitions();
    setupRipple();
    setupCardAppear();
    setupNavbarScroll();
    applyNeonGlow();
    setupMagneticHover();
    setupGlassShimmer();
    setupStaggerList();
    setupEmojiOnCorrect();
    applyGradientBg();
    if (animEnabled) setupFloatingParticles();
  }

  function lateInit() {
    // FPS testi keyin keladi
    setupFloatingParticles();
  }

  function init() {
    injectStyles();
    detectPerformance();
    domReady(mainInit);
  }

  init();

})();
