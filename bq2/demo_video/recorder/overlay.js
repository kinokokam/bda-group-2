// Caption bar + step badge + smooth scroll. Annotation only: drawn OVER real page state.
(() => {
  if (window.__cap) return;
  const css = `#__capbar{position:fixed;left:0;right:0;bottom:0;height:74px;z-index:2147483646;
    background:rgba(16,47,40,.94);color:#fff;display:flex;align-items:center;gap:18px;padding:0 26px;
    font:500 21px/1.3 'DejaVu Sans',sans-serif;opacity:0;transition:opacity .35s;pointer-events:none}
    #__capbar.on{opacity:1}#__capstep{flex:none;background:#f6c64b;color:#10231f;border-radius:999px;
    padding:6px 14px;font:700 15px 'DejaVu Sans',sans-serif;letter-spacing:.02em}#__captxt{flex:1}`;
  function ensure() {
    if (document.getElementById('__capbar')) return;
    const s = document.createElement('style'); s.textContent = css; document.documentElement.appendChild(s);
    const b = document.createElement('div'); b.id = '__capbar';
    b.innerHTML = '<span id="__capstep"></span><span id="__captxt"></span>';
    document.documentElement.appendChild(b);
  }
  window.__cap = (step, text) => {
    ensure(); const b = document.getElementById('__capbar');
    if (!text) { b.classList.remove('on'); return; }
    const st = document.getElementById('__capstep');
    st.textContent = step || ''; st.style.display = step ? '' : 'none';
    b.classList.remove('on');
    setTimeout(() => { document.getElementById('__captxt').textContent = text; b.classList.add('on'); }, 120);
  };
  window.__scroll = (sel, to, ms = 3000) => new Promise(res => {
    const el = sel ? document.querySelector(sel) : document.scrollingElement;
    if (!el) return res(false);
    const from = el.scrollTop, max = el.scrollHeight - el.clientHeight;
    let target;
    if (to === 'end') target = max;
    else if (typeof to === 'string') { const t = document.querySelector(to);
      target = t ? (t.getBoundingClientRect().top - el.getBoundingClientRect().top + el.scrollTop - 120) : from; }
    else target = to;
    target = Math.max(0, Math.min(max, target));
    const t0 = performance.now(), ease = x => x < .5 ? 2*x*x : 1 - Math.pow(-2*x + 2, 2)/2;
    const step = now => { const k = Math.min(1, (now - t0)/ms); el.scrollTop = from + (target - from)*ease(k);
      k < 1 ? requestAnimationFrame(step) : res(true); };
    requestAnimationFrame(step);
  });
})();
