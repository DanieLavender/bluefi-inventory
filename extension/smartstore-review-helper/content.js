/**
 * 블루파이 리뷰 답글 도우미 — content script
 *
 * 역할: 스마트스토어 판매자센터(SPA) 리뷰 화면에서
 *  1) 답글 textarea 를 감지해 "✨ AI 답글 초안" 버튼 + "AI가 읽은 내용 보기" 링크 주입
 *  2) 리뷰 행 DOM 에서 리뷰 본문/상품명/옵션/별점/상품번호/주문번호 추출 (extractReviewContext)
 *  3) background 를 통해 블루파이 서버에 초안 요청 → 후보 3개 패널 표시 → 클릭 시 입력칸 채움
 *
 * 설계 원칙: 등록 버튼 자동 클릭은 절대 하지 않는다 (네이버 약관 — 반자동).
 * AI 키는 클라이언트에 없다. 서버 주소/토큰은 chrome.storage.local (설정 팝업에서 입력).
 */
(() => {
  'use strict';

  const ORPHAN_MSG = '확장이 업데이트되었습니다. F5로 새로고침해주세요';
  const ATTACH_ATTR = 'data-ss-ai-attached';
  const REPLY_BRANCH_ATTR = 'data-ss-ai-reply-branch';

  /* ------------------------------------------------------------------
   * 지뢰 7: 노이즈·개인정보 필터
   * ------------------------------------------------------------------ */

  // 판매자센터 UI 안내문구 블랙리스트 (부분일치로 제거)
  const NOISE_PHRASES = [
    '반복적인 답글이 아닌 정성스러운 답글을 남겨주세요',
    '정성스러운 답글을 남겨주세요',
    '답글을 입력해주세요',
    '답글을 입력하세요',
    '리뷰 답글은 구매자에게',
    '답글 등록',
    '답글 작성',
    '답글달기',
    '리뷰 신고',
    '혜택 지급',
    '리뷰이벤트',
    '베스트 리뷰',
    '도움이 돼요',
    '신고하기',
    '더보기',
    '접기',
    '수정',
    '삭제'
  ];

  // 신체정보 등 개인정보가 포함된 줄은 서버로 보내지 않는다
  const PII_LINE_PATTERNS = [
    /유저정보/,
    /체형/,
    /\d{2,3}\s*cm/i,
    /\d{2,3}\s*kg/i,
    /평소\s*사이즈/
  ];

  // 메타데이터(리뷰 본문 아님)로 간주하는 줄
  const META_LINE_PATTERNS = [
    /^주문\s*번호/,
    /^상품\s*번호/,
    /^구매자/,
    /^작성자/,
    /^작성일/,
    /^옵션\s*[:：]/,
    /^리뷰\s*(구분|영역)/,
    /^\d{4}[.\-\/]\s?\d{1,2}[.\-\/]\s?\d{1,2}\.?$/, // 날짜만 있는 줄
    /^[0-5]\s*점$/,                                    // 별점만 있는 줄
    /^평점/,
    /^별점/,
    /\*{2,}/                                           // 마스킹된 구매자 ID (예: abc***)
  ];

  function isNoiseLine(line) {
    return NOISE_PHRASES.some((p) => line.includes(p));
  }
  function isPiiLine(line) {
    return PII_LINE_PATTERNS.some((re) => re.test(line));
  }
  function isMetaLine(line) {
    return META_LINE_PATTERNS.some((re) => re.test(line));
  }

  /* ------------------------------------------------------------------
   * 지뢰 5: 고아 스크립트 (확장 업데이트/리로드 시 chrome.runtime 소멸)
   * ------------------------------------------------------------------ */

  function extAlive() {
    try {
      return !!(typeof chrome !== 'undefined' && chrome.runtime && chrome.runtime.id);
    } catch (e) {
      return false;
    }
  }

  function sendToBackground(msg) {
    return new Promise((resolve) => {
      if (!extAlive()) {
        resolve({ ok: false, error: ORPHAN_MSG, orphaned: true });
        return;
      }
      try {
        chrome.runtime.sendMessage(msg, (resp) => {
          const err = chrome.runtime && chrome.runtime.lastError;
          if (err) {
            const orphaned = /invalidated|closed|Receiving end/i.test(err.message || '');
            resolve({ ok: false, error: orphaned ? ORPHAN_MSG : err.message, orphaned });
          } else if (!resp) {
            resolve({ ok: false, error: '백그라운드 응답이 없습니다.' });
          } else {
            resolve(resp);
          }
        });
      } catch (e) {
        resolve({ ok: false, error: ORPHAN_MSG, orphaned: true });
      }
    });
  }

  /* ------------------------------------------------------------------
   * 리뷰 컨텍스트 추출
   * ------------------------------------------------------------------
   * ⚠ 실화면에서 조정 필요할 수 있음:
   * 판매자센터 클래스명은 난독화/변경될 수 있어 특정 클래스 의존을 최소화하고
   * 구조(tr/li/role)·텍스트 휴리스틱을 병행한다. 셀렉터 튜닝은 이 블록과
   * findReviewRoot / isReplyTextarea 만 고치면 되도록 로직을 모아 두었다.
   * "AI가 읽은 내용 보기" 링크로 추출 결과(전송 JSON)를 즉시 확인할 수 있다.
   * ------------------------------------------------------------------ */

  const BLOCK_TAGS = new Set([
    'DIV', 'P', 'LI', 'TR', 'TD', 'TH', 'SECTION', 'ARTICLE', 'UL', 'OL',
    'TABLE', 'THEAD', 'TBODY', 'DL', 'DT', 'DD', 'H1', 'H2', 'H3', 'H4',
    'H5', 'H6', 'BR', 'HR', 'FIELDSET', 'LABEL', 'FORM'
  ]);

  // 클론(비부착 노드)에서 블록 경계마다 줄바꿈을 넣어 텍스트 줄 목록을 만든다
  function collectLines(rootNode) {
    const lines = [];
    let buf = [];
    const flush = () => {
      const t = buf.join(' ').replace(/\s+/g, ' ').trim();
      if (t) lines.push(t);
      buf = [];
    };
    const walk = (node) => {
      for (const child of node.childNodes) {
        if (child.nodeType === Node.TEXT_NODE) {
          const t = child.textContent.trim();
          if (t) buf.push(t);
        } else if (child.nodeType === Node.ELEMENT_NODE) {
          const tag = child.tagName;
          if (tag === 'SCRIPT' || tag === 'STYLE' || tag === 'NOSCRIPT') continue;
          const isBlock = BLOCK_TAGS.has(tag);
          if (isBlock) flush();
          walk(child);
          if (isBlock) flush();
        }
      }
    };
    walk(rootNode);
    flush();
    return lines;
  }

  /**
   * textarea 로부터 리뷰 행/카드 루트를 찾는다.
   * 휴리스틱: 상품 링크(/products/)나 별점 흔적을 포함하는 가장 가까운 조상,
   * 없으면 tr/li/role=listitem, 그것도 없으면 6단계 위 조상.
   */
  function findReviewRoot(textarea) {
    let el = textarea.parentElement;
    let hops = 0;
    let fallback = null;
    while (el && el !== document.body && hops < 12) {
      const tag = el.tagName;
      const cls = typeof el.className === 'string' ? el.className : '';
      if (
        tag === 'TR' || tag === 'LI' ||
        el.getAttribute('role') === 'listitem' ||
        /review/i.test(cls)
      ) {
        fallback = el;
      }
      const hasProductLink = !!el.querySelector('a[href*="/products/"]');
      const hasRating = /[0-5]\s*점|평점|별점/.test(el.textContent || '');
      if (hasProductLink || hasRating) {
        // 리뷰 목록 전체 컨테이너까지 올라가 버린 경우를 피한다:
        // 열린 답글 textarea 가 2개 이상이면 너무 넓게 잡은 것
        if (el.querySelectorAll('textarea').length <= 1) return el;
        return fallback || el;
      }
      el = el.parentElement;
      hops++;
    }
    if (fallback) return fallback;
    // 최후: 6단계 위 조상
    let up = textarea;
    for (let i = 0; i < 6 && up.parentElement && up.parentElement !== document.body; i++) {
      up = up.parentElement;
    }
    return up;
  }

  // 등록 완료된 판매자 답글 영역 라벨 휴리스틱
  const REPLY_LABEL_RE = /판매자\s*답글|스토어\s*답글|사장님|^답글$/;

  function truncate(s, n) {
    if (s == null) return null;
    return s.length > n ? s.slice(0, n) : s;
  }

  // 줄 목록에 노이즈/개인정보/메타 필터를 공통 적용 (현재 리뷰·형제 리뷰·기존 답글 동일)
  function filterContentLines(lines, fields) {
    const seen = new Set();
    const kept = [];
    for (let line of lines) {
      line = line.trim();
      if (!line || seen.has(line)) continue;
      seen.add(line);
      if (isNoiseLine(line)) continue;      // UI 안내문구
      if (isPiiLine(line)) continue;        // 신체정보 등 개인정보 줄 삭제
      if (isMetaLine(line)) continue;       // 주문번호/날짜/별점 등 메타
      if (fields.productName && line === fields.productName) continue;
      if (fields.option && line.includes(fields.option) && line.length < fields.option.length + 15) continue;
      if (fields.orderNo && line.includes(fields.orderNo)) continue;
      if (/^[\d\s.,:\-\/]+$/.test(line)) continue; // 숫자/기호만 있는 줄
      if (line.length < 2) continue;
      kept.push(line);
    }
    return kept;
  }

  function joinReviewLines(kept) {
    if (!kept.length) return null;
    // 가장 긴 줄이 대체로 리뷰 본문. 그 줄 + 15자 이상인 나머지 줄을 순서대로 합친다.
    const longest = kept.reduce((a, b) => (b.length > a.length ? b : a), kept[0]);
    const parts = kept.filter((l) => l === longest || l.length >= 15);
    return (parts.length ? parts : [longest]).join('\n');
  }

  /**
   * 리뷰 행 안에서 "이미 등록 완료된 판매자 답글" 표시 영역을 찾는다.
   * (답글 입력 textarea 값이 아님 — 등록된 답글이 렌더된 블록)
   * ⚠ 실화면에서 조정 필요할 수 있음: 라벨 문구('판매자 답글' 등)와 블록 구조는
   * 실화면 확인 전 휴리스틱이다. 오탐 시 REPLY_LABEL_RE 와 이 함수만 보정.
   */
  function findExistingReplyEl(rowEl, textarea) {
    let labelEl = null;
    for (const el of rowEl.querySelectorAll('*')) {
      if (textarea && (el === textarea || el.contains(textarea))) continue; // 입력폼 쪽 라벨 제외
      if (el.children.length > 2) continue;
      const t = (el.textContent || '').trim();
      if (t && t.length <= 20 && REPLY_LABEL_RE.test(t)) {
        labelEl = el;
        break;
      }
    }
    if (!labelEl) return null;
    // 라벨보다 텍스트가 길어지는 첫 조상을 답글 블록으로 본다
    let block = labelEl;
    const labelLen = (labelEl.textContent || '').trim().length;
    while (block.parentElement && block.parentElement !== rowEl) {
      const p = block.parentElement;
      if (((p.textContent || '').trim().length) > labelLen + 5) {
        block = p;
        break;
      }
      block = p;
    }
    if (block === rowEl) return null;
    if (block.querySelector('a[href*="/products/"]')) return null; // 행 전체를 잡은 오탐
    if (textarea && block.contains(textarea)) return null;          // 입력폼 가지
    return block;
  }

  function extractExistingReplyText(replyBlock) {
    const clone = replyBlock.cloneNode(true);
    clone
      .querySelectorAll('textarea, button, input, select, [class*="ss-ai-"]')
      .forEach((n) => n.remove());
    const lines = collectLines(clone).filter(
      (l) => !(l.trim().length <= 20 && REPLY_LABEL_RE.test(l.trim())) // 라벨 줄 제거
    );
    const kept = filterContentLines(lines, {});
    return kept.length ? kept.join('\n') : null;
  }

  /**
   * 리뷰 행 1개에서 필드를 추출한다 (현재 행·형제 행 공용).
   * 반환: { productNo, orderNo, rating, productName, option, reviewText, existingReply }
   * (못 찾은 값은 null. textarea 는 현재 행에만 있고 형제 행에서는 null 가능)
   */
  function extractRowContext(rootEl, textarea) {
    // --- (a) 답글 입력 폼 가지 + 등록된 판매자 답글 블록을 클론에서 제거 ---
    // 원본에 마커를 찍고 → 클론에서 마커 요소를 제거 → 마커 원복
    const marked = [];
    if (textarea && rootEl.contains(textarea)) {
      let branch = textarea.closest('form');
      if (!branch || branch === rootEl || !rootEl.contains(branch)) {
        branch = textarea;
        // textarea 를 감싼 입력 영역까지 최대 3단계 확장 (루트 직전까지만)
        for (let i = 0; i < 3; i++) {
          const p = branch.parentElement;
          if (!p || p === rootEl) break;
          branch = p;
        }
      }
      branch.setAttribute(REPLY_BRANCH_ATTR, '1');
      marked.push(branch);
    }
    const replyBlock = findExistingReplyEl(rootEl, textarea || null);
    if (replyBlock) {
      replyBlock.setAttribute(REPLY_BRANCH_ATTR, '1');
      marked.push(replyBlock);
    }
    let clone;
    try {
      clone = rootEl.cloneNode(true);
    } finally {
      marked.forEach((n) => n.removeAttribute(REPLY_BRANCH_ATTR));
    }
    clone.querySelectorAll('[' + REPLY_BRANCH_ATTR + ']').forEach((n) => n.remove());
    // 남은 입력/버튼/우리가 주입한 UI 도 제거
    clone
      .querySelectorAll('textarea, button, input, select, [class*="ss-ai-"]')
      .forEach((n) => n.remove());

    // --- 필드 추출 (필터 전 원시 DOM/텍스트 기준) ---
    const result = {
      productNo: null,
      orderNo: null,
      rating: null,
      productName: null,
      option: null,
      reviewText: null,
      existingReply: null
    };

    // productNo + productName: 상품 링크 href 의 /products/(\d+)
    const productLink = rootEl.querySelector('a[href*="/products/"]');
    if (productLink) {
      const m = (productLink.getAttribute('href') || '').match(/\/products\/(\d+)/);
      if (m) result.productNo = m[1];
      const nameText = (productLink.textContent || '').replace(/\s+/g, ' ').trim();
      if (nameText) result.productName = nameText;
    }

    const rawText = (rootEl.textContent || '').replace(/ /g, ' ');

    // orderNo: "주문번호" 라벨 뒤 숫자 (판매자센터 주문번호는 10자리 이상)
    let m = rawText.match(/주문\s*번호\s*[:：]?\s*(\d{8,20})/);
    if (m) result.orderNo = m[1];

    // rating: aria-label("별점 5점" 등) 우선 → 텍스트 패턴
    for (const el of rootEl.querySelectorAll('[aria-label]')) {
      const am = (el.getAttribute('aria-label') || '').match(/([0-5])\s*점/);
      if (am) {
        result.rating = parseInt(am[1], 10);
        break;
      }
    }
    if (result.rating == null) {
      const rm =
        rawText.match(/(?:평점|별점)\s*[:：]?\s*([0-5])/) ||
        rawText.match(/(?:^|\s)([0-5])\s*점(?:\s|$)/);
      if (rm) result.rating = parseInt(rm[1], 10);
    }

    // option: "옵션: ..." 패턴
    m = rawText.match(/옵션\s*[:：]\s*([^\n\r]{1,120})/);
    if (m) result.option = m[1].replace(/\s+/g, ' ').trim();

    // productName 폴백: "상품명: ..." 패턴
    if (!result.productName) {
      m = rawText.match(/상품명\s*[:：]\s*([^\n\r]{1,150})/);
      if (m) result.productName = m[1].replace(/\s+/g, ' ').trim();
    }

    // --- (b)(c) 리뷰 본문: 줄 단위 필터 후 남는 텍스트 ---
    result.reviewText = joinReviewLines(filterContentLines(collectLines(clone), result));

    // 등록된 판매자 답글 (노이즈/개인정보 필터 동일 적용)
    if (replyBlock) result.existingReply = extractExistingReplyText(replyBlock);

    return result;
  }

  /**
   * 현재 리뷰 행 기준으로 페이지에 렌더된 다른 리뷰 행 후보를 수집한다.
   * 1순위: 같은 부모의 형제 요소 중 리뷰 행 흔적(상품링크/별점)이 있는 것
   * 2순위(폴백): 같은 태그+클래스 조합을 문서 전역에서 탐색
   * ⚠ 실화면에서 조정 필요할 수 있음 (findReviewRoot 와 동일 휴리스틱 계열)
   */
  function collectSiblingRows(currentRoot) {
    const looksLikeRow = (el) =>
      el.nodeType === Node.ELEMENT_NODE &&
      el !== currentRoot &&
      !el.contains(currentRoot) &&
      !currentRoot.contains(el) &&
      (!!el.querySelector('a[href*="/products/"]') ||
        /[0-5]\s*점|평점|별점/.test(el.textContent || ''));

    const rows = [];
    const parent = currentRoot.parentElement;
    if (parent) {
      for (const sib of parent.children) {
        if (looksLikeRow(sib)) rows.push(sib);
      }
    }
    if (!rows.length && typeof currentRoot.className === 'string' && currentRoot.className.trim()) {
      try {
        const sel =
          currentRoot.tagName +
          '.' +
          currentRoot.className.trim().split(/\s+/).map((c) => CSS.escape(c)).join('.');
        document.querySelectorAll(sel).forEach((el) => {
          if (looksLikeRow(el)) rows.push(el);
        });
      } catch (e) {
        // 잘못된 셀렉터 조합은 무시
      }
    }
    return rows;
  }

  /**
   * 동일 상품의 다른 리뷰(+ 기존 등록 답글)를 최대 5건 수집.
   * 선별: productNo 일치 우선, 못 얻으면 상품명 완전일치 폴백.
   * 각 리뷰 300자·답글 300자 절단. 개인정보/노이즈 필터는 본문과 동일 적용.
   */
  function collectSiblingReviews(currentRoot, currentCtx) {
    const out = [];
    if (!currentCtx.productNo && !currentCtx.productName) return out;
    for (const row of collectSiblingRows(currentRoot)) {
      if (out.length >= 5) break;
      try {
        const rowTa = row.querySelector('textarea');
        const ctx = extractRowContext(row, rowTa);
        const same = currentCtx.productNo
          ? ctx.productNo === currentCtx.productNo
          : !!ctx.productName && ctx.productName === currentCtx.productName;
        if (!same) continue;
        if (!ctx.reviewText && !ctx.existingReply) continue;
        out.push({
          reviewText: truncate(ctx.reviewText, 300),
          rating: ctx.rating,
          existingReply: truncate(ctx.existingReply, 300)
        });
      } catch (e) {
        // 개별 행 실패가 전체 수집을 막지 않도록
      }
    }
    return out;
  }

  /**
   * 서버 전송용 최종 컨텍스트 (payload 그대로 — "AI가 읽은 내용 보기"에도 이대로 표시).
   * 반환: { productNo, orderNo, rating, productName, option, reviewText, siblingReviews }
   */
  function extractReviewContext(rootEl, textarea) {
    const ctx = extractRowContext(rootEl, textarea);
    return {
      productNo: ctx.productNo,
      orderNo: ctx.orderNo,
      rating: ctx.rating,
      productName: ctx.productName,
      option: ctx.option,
      reviewText: ctx.reviewText,
      siblingReviews: collectSiblingReviews(rootEl, ctx)
    };
  }

  /* ------------------------------------------------------------------
   * 지뢰 6: textarea 값 되돌림 (React controlled input 대응)
   * ------------------------------------------------------------------ */

  function fillTextarea(ta, text) {
    try {
      ta.focus();
      ta.setSelectionRange(0, ta.value.length);
    } catch (e) { /* ignore */ }
    let ok = false;
    try {
      // 1순위: execCommand — React 가 실제 사용자 입력으로 인식
      ok = document.execCommand('insertText', false, text);
    } catch (e) {
      ok = false;
    }
    if (!ok || ta.value !== text) {
      // 폴백: 네이티브 value setter + input 이벤트
      try {
        const setter = Object.getOwnPropertyDescriptor(
          window.HTMLTextAreaElement.prototype,
          'value'
        ).set;
        setter.call(ta, text);
      } catch (e) {
        ta.value = text;
      }
      ta.dispatchEvent(new Event('input', { bubbles: true }));
      ta.dispatchEvent(new Event('change', { bubbles: true }));
    }
  }

  /* ------------------------------------------------------------------
   * UI 주입
   * ------------------------------------------------------------------ */

  function el(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text != null) node.textContent = text; // 항상 textContent — XSS 방지
    return node;
  }

  function setButtonLoading(btn, loading) {
    btn.disabled = loading;
    btn.textContent = '';
    if (loading) {
      btn.appendChild(el('span', 'ss-ai-spinner'));
      btn.appendChild(document.createTextNode(' 초안 생성 중…'));
    } else {
      btn.appendChild(document.createTextNode('✨ AI 답글 초안'));
    }
  }

  function setOrphaned(state) {
    state.btn.disabled = true;
    state.btn.classList.add('ss-ai-btn--dead');
    state.btn.textContent = ORPHAN_MSG;
  }

  function ensurePanel(state) {
    if (state.panel && state.panel.isConnected) return state.panel;
    const panel = el('div', 'ss-ai-panel');
    const head = el('div', 'ss-ai-panel-head');
    const title = el('span', 'ss-ai-panel-title', 'AI 답글 초안');
    const close = el('button', 'ss-ai-close', '✕');
    close.type = 'button';
    close.addEventListener('click', () => {
      panel.remove();
    });
    head.appendChild(title);
    head.appendChild(close);
    panel.appendChild(head);
    panel.appendChild(el('div', 'ss-ai-panel-body'));
    state.bar.insertAdjacentElement('afterend', panel);
    state.panel = panel;
    state.panelTitle = title;
    return panel;
  }

  function panelBody(state) {
    return ensurePanel(state).querySelector('.ss-ai-panel-body');
  }

  function clearPanelExtras(state) {
    const panel = ensurePanel(state);
    panel.querySelectorAll('.ss-ai-warn, .ss-ai-foot').forEach((n) => n.remove());
  }

  function showError(state, message) {
    clearPanelExtras(state);
    state.panelTitle.textContent = '오류';
    const body = panelBody(state);
    body.textContent = '';
    body.appendChild(el('div', 'ss-ai-error', message)); // 서버 오류 원문 그대로 노출
  }

  function showCandidates(state, data) {
    const panel = ensurePanel(state);
    clearPanelExtras(state);
    state.panelTitle.textContent = 'AI 답글 초안';

    // needsHumanReview(저평점) 경고 띠
    if (data.needsHumanReview) {
      const warn = el(
        'div',
        'ss-ai-warn',
        '⚠ 저평점 리뷰 — 내용을 반드시 확인 후 등록하세요'
      );
      panel.insertBefore(warn, panel.querySelector('.ss-ai-panel-body'));
    }

    const body = panelBody(state);
    body.textContent = '';
    body.appendChild(
      el('div', 'ss-ai-hint', '후보를 클릭하면 답글 입력칸에 채워집니다. 등록은 직접 눌러주세요.')
    );

    const candidates = Array.isArray(data.candidates) ? data.candidates : [];
    if (!candidates.length) {
      body.appendChild(el('div', 'ss-ai-error', '서버가 후보를 반환하지 않았습니다.'));
    }
    candidates.forEach((text, i) => {
      const cand = el('button', 'ss-ai-cand');
      cand.type = 'button';
      cand.appendChild(el('span', 'ss-ai-cand-num', '후보 ' + (i + 1)));
      cand.appendChild(document.createTextNode(String(text)));
      cand.addEventListener('click', () => {
        fillTextarea(state.textarea, String(text));
      });
      body.appendChild(cand);
    });

    // 푸터: 다시 생성 / 사용된 상품 정보
    const foot = el('div', 'ss-ai-foot');
    const regen = el('button', 'ss-ai-btn-sub', '↻ 다시 생성');
    regen.type = 'button';
    regen.addEventListener('click', () => {
      generate(state);
    });
    foot.appendChild(regen);
    if (data.usedProduct) {
      let label = '';
      try {
        label =
          data.usedProduct.name ||
          data.usedProduct.productName ||
          JSON.stringify(data.usedProduct);
      } catch (e) {
        label = '';
      }
      if (label) foot.appendChild(el('span', 'ss-ai-foot-note', '상품 참조: ' + label));
    }
    panel.appendChild(foot);
  }

  function showContext(state) {
    if (!extAlive()) {
      setOrphaned(state);
      return;
    }
    const ctx = extractReviewContext(state.root, state.textarea);
    clearPanelExtras(state);
    state.panelTitle.textContent = 'AI가 읽은 내용 (서버로 전송될 JSON)';
    const body = panelBody(state);
    body.textContent = '';
    body.appendChild(
      el('div', 'ss-ai-hint', '추출이 이상하면 이 화면을 캡처해서 전달해주세요 (셀렉터 튜닝용).')
    );
    body.appendChild(el('pre', 'ss-ai-pre', JSON.stringify(ctx, null, 2)));
  }

  async function generate(state) {
    if (!extAlive()) {
      setOrphaned(state);
      return;
    }
    setButtonLoading(state.btn, true);
    try {
      const payload = extractReviewContext(state.root, state.textarea);
      const resp = await sendToBackground({ type: 'generateReply', payload });
      if (resp.orphaned) {
        setOrphaned(state);
        return;
      }
      if (!resp.ok) {
        showError(state, resp.error || '알 수 없는 오류');
        return;
      }
      showCandidates(state, resp.data || {});
    } finally {
      if (extAlive()) setButtonLoading(state.btn, false);
    }
  }

  /**
   * 답글 입력 textarea 판별 휴리스틱.
   * ⚠ 실화면에서 조정 필요할 수 있음: placeholder 문구가 다르면 여기를 보정.
   */
  function isReplyTextarea(ta) {
    if (ta.closest('[class*="ss-ai-"]')) return false;
    if (!ta.offsetParent && ta.getClientRects().length === 0) return false; // 숨김
    const hint =
      (ta.getAttribute('placeholder') || '') +
      ' ' +
      (ta.getAttribute('aria-label') || '') +
      ' ' +
      (ta.getAttribute('title') || '');
    if (/답글|리뷰/.test(hint)) return true;
    // 폴백: 리뷰 관리 화면(URL 에 review 포함)의 보이는 textarea 는 답글 입력으로 간주
    if (/review/i.test(location.href)) return true;
    return false;
  }

  function attach(ta) {
    const host = ta.closest('form') || ta.parentElement;
    if (!host) return;
    if (host.hasAttribute(ATTACH_ATTR) || ta.hasAttribute(ATTACH_ATTR)) return; // 중복 주입 금지
    host.setAttribute(ATTACH_ATTR, '1');
    ta.setAttribute(ATTACH_ATTR, '1');

    const bar = el('div', 'ss-ai-bar');
    const btn = el('button', 'ss-ai-btn', '✨ AI 답글 초안');
    btn.type = 'button';
    const link = el('button', 'ss-ai-link', 'AI가 읽은 내용 보기');
    link.type = 'button';
    bar.appendChild(btn);
    bar.appendChild(link);
    ta.insertAdjacentElement('afterend', bar);

    const state = {
      textarea: ta,
      root: findReviewRoot(ta),
      bar,
      btn,
      link,
      panel: null,
      panelTitle: null
    };

    btn.addEventListener('click', () => generate(state));
    link.addEventListener('click', () => showContext(state));
  }

  /* ------------------------------------------------------------------
   * SPA 감시 (MutationObserver)
   * ------------------------------------------------------------------ */

  let scanTimer = null;
  function scheduleScan() {
    if (scanTimer) return;
    scanTimer = setTimeout(() => {
      scanTimer = null;
      scan();
    }, 300);
  }

  function scan() {
    document.querySelectorAll('textarea').forEach((ta) => {
      try {
        if (isReplyTextarea(ta)) attach(ta);
      } catch (e) {
        // 개별 textarea 실패가 전체 스캔을 막지 않도록
      }
    });
  }

  const observer = new MutationObserver(scheduleScan);
  observer.observe(document.documentElement, { childList: true, subtree: true });
  scan();
})();
