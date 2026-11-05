/* BQ2 theme: tag table cells so CSS can draw status pills (display only) */
(function () {
  if (window.__bq2Pills) return; window.__bq2Pills = true;
  var KEYS = { pass: 1, fail: 1, "true": 1, "false": 1 };
  var queued = false;
  function run() {
    queued = false;
    var cells = document.querySelectorAll('.react-grid-item td.ant-table-cell > .hr-report-cell');
    for (var i = 0; i < cells.length; i++) {
      var c = cells[i], t = (c.textContent || '').trim().toLowerCase();
      if (KEYS[t]) { if (c.getAttribute('data-bq2') !== t) c.setAttribute('data-bq2', t); }
      else if (c.hasAttribute('data-bq2')) c.removeAttribute('data-bq2');
      var shortTxt = t.length > 0 && t.length <= 14;
      if (shortTxt !== c.hasAttribute('data-bq2n')) { if (shortTxt) c.setAttribute('data-bq2n', '1'); else c.removeAttribute('data-bq2n'); }
    }
    var rows = document.querySelectorAll('.react-grid-item tr.ant-table-row');
    for (var j = 0; j < rows.length; j++) {
      var q = !!rows[j].querySelector('[data-bq2="true"]');
      if (q !== rows[j].hasAttribute('data-bq2q')) { if (q) rows[j].setAttribute('data-bq2q', '1'); else rows[j].removeAttribute('data-bq2q'); }
    }
  }
  function queue() { if (!queued) { queued = true; requestAnimationFrame(run); } }
  new MutationObserver(queue).observe(document.body, { childList: true, subtree: true, characterData: true });
  queue();
})();
