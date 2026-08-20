/**
 * 블루파이 리뷰 답글 도우미 — background service worker (MV3)
 *
 * content script 로부터 메시지를 받아 블루파이 서버 API 를 호출한다.
 * AI 키는 서버에만 있다. 여기서는 서버 주소 + 직원 토큰(Bearer)만 다룬다.
 * 서버 주소/토큰은 chrome.storage.local (설정 팝업에서 저장).
 */
'use strict';

const DEFAULT_SERVER_URL = 'http://cosguardian.lavenderfriends.co.kr:3001';

async function getSettings() {
  const stored = await chrome.storage.local.get(['serverUrl', 'token']);
  const serverUrl = (stored.serverUrl || DEFAULT_SERVER_URL).trim().replace(/\/+$/, '');
  const token = (stored.token || '').trim();
  return { serverUrl, token };
}

/**
 * 서버 API 호출. 실패 시 서버가 준 오류 원문을 그대로 담아 throw.
 */
async function callServer(path, method, body) {
  const { serverUrl, token } = await getSettings();
  if (!token) {
    throw new Error('직원 토큰이 설정되지 않았습니다. 확장 아이콘 → 설정에서 입력해주세요.');
  }
  let res;
  try {
    res = await fetch(serverUrl + path, {
      method,
      headers: {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
      },
      body: body != null ? JSON.stringify(body) : undefined
    });
  } catch (e) {
    throw new Error('서버에 연결할 수 없습니다 (' + serverUrl + '): ' + e.message);
  }

  const raw = await res.text();
  let data = null;
  try {
    data = raw ? JSON.parse(raw) : null;
  } catch (e) {
    data = null;
  }

  if (!res.ok) {
    // 서버 오류 원문 노출: { error: "..." } 형식이면 error 필드, 아니면 본문/상태코드
    const msg =
      (data && data.error) ||
      (raw && raw.slice(0, 300)) ||
      ('HTTP ' + res.status + ' ' + res.statusText);
    throw new Error('[' + res.status + '] ' + msg);
  }
  return data;
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  (async () => {
    try {
      switch (msg && msg.type) {
        case 'generateReply': {
          const p = msg.payload || {};
          const data = await callServer('/api/review-reply', 'POST', {
            productNo: p.productNo != null ? p.productNo : null,
            orderNo: p.orderNo != null ? p.orderNo : null,
            rating: p.rating != null ? p.rating : null,
            productName: p.productName != null ? p.productName : null,
            option: p.option != null ? p.option : null,
            reviewText: p.reviewText != null ? p.reviewText : null,
            siblingReviews: Array.isArray(p.siblingReviews) ? p.siblingReviews : []
          });
          sendResponse({ ok: true, data });
          break;
        }
        case 'getConfig': {
          const data = await callServer('/api/review-reply/config', 'GET');
          sendResponse({ ok: true, data });
          break;
        }
        case 'saveConfig': {
          const data = await callServer('/api/review-reply/config', 'PUT', {
            rules: (msg.payload && msg.payload.rules) || ''
          });
          sendResponse({ ok: true, data });
          break;
        }
        case 'testConnection': {
          const data = await callServer('/api/review-reply/test', 'POST');
          sendResponse({ ok: true, data });
          break;
        }
        default:
          sendResponse({ ok: false, error: '알 수 없는 메시지 타입: ' + (msg && msg.type) });
      }
    } catch (e) {
      sendResponse({ ok: false, error: e && e.message ? e.message : String(e) });
    }
  })();
  return true; // async sendResponse
});
