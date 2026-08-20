/**
 * 블루파이 리뷰 답글 도우미 — 설정 팝업/옵션 페이지
 *
 * - 서버 주소 / 직원 토큰: chrome.storage.local 저장
 * - 답글 규칙: 서버 GET/PUT /api/review-reply/config (확장 재배포 없이 수정 가능)
 * - 연결 테스트: POST /api/review-reply/test — 서버 원문 오류를 그대로 표시
 */
'use strict';

const DEFAULT_SERVER_URL = 'http://cosguardian.lavenderfriends.co.kr:3001';

const $ = (id) => document.getElementById(id);

function setStatus(elId, kind, message) {
  const box = $(elId);
  box.className = 'status ' + kind;
  box.textContent = message;
}

function currentServerUrl() {
  return ($('serverUrl').value.trim() || DEFAULT_SERVER_URL).replace(/\/+$/, '');
}

function currentToken() {
  return $('token').value.trim();
}

/**
 * 설정 화면은 저장 전 입력값으로도 테스트할 수 있도록 직접 fetch 한다.
 * (host_permissions 에 등록된 서버 주소만 호출 가능)
 */
async function api(path, method, body) {
  const token = currentToken();
  if (!token) throw new Error('직원 토큰을 입력해주세요.');
  let res;
  try {
    res = await fetch(currentServerUrl() + path, {
      method,
      headers: {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
      },
      body: body != null ? JSON.stringify(body) : undefined
    });
  } catch (e) {
    throw new Error('서버에 연결할 수 없습니다 (' + currentServerUrl() + '): ' + e.message);
  }
  const raw = await res.text();
  let data = null;
  try {
    data = raw ? JSON.parse(raw) : null;
  } catch (e) {
    data = null;
  }
  if (!res.ok) {
    const msg =
      (data && data.error) ||
      (raw && raw.slice(0, 500)) ||
      ('HTTP ' + res.status + ' ' + res.statusText);
    throw new Error('[' + res.status + '] ' + msg); // 서버 원문 오류 그대로
  }
  return data;
}

async function loadStored() {
  const stored = await chrome.storage.local.get(['serverUrl', 'token']);
  $('serverUrl').value = stored.serverUrl || DEFAULT_SERVER_URL;
  $('token').value = stored.token || '';
}

async function saveStored() {
  await chrome.storage.local.set({
    serverUrl: currentServerUrl(),
    token: currentToken()
  });
  setStatus('connStatus', 'ok', '저장되었습니다.');
}

async function testConnection() {
  const btn = $('testBtn');
  btn.disabled = true;
  setStatus('connStatus', 'info', '연결 테스트 중…');
  try {
    const data = await api('/api/review-reply/test', 'POST');
    if (data && data.success) {
      setStatus('connStatus', 'ok', '연결 성공: ' + (data.message || 'OK'));
    } else {
      setStatus(
        'connStatus',
        'err',
        '테스트 실패: ' + ((data && data.message) || JSON.stringify(data))
      );
    }
  } catch (e) {
    setStatus('connStatus', 'err', e.message);
  } finally {
    btn.disabled = false;
  }
}

async function loadRules(silent) {
  const btn = $('loadRulesBtn');
  btn.disabled = true;
  if (!silent) setStatus('rulesStatus', 'info', '규칙 불러오는 중…');
  try {
    const data = await api('/api/review-reply/config', 'GET');
    $('rules').value = (data && data.rules) || '';
    const meta = [];
    if (data && data.model) meta.push('모델: ' + data.model);
    if (data && typeof data.hasGeminiKey === 'boolean') {
      meta.push('Gemini 키: ' + (data.hasGeminiKey ? '서버에 설정됨' : '서버에 없음 ⚠'));
    }
    $('configMeta').textContent = meta.join(' · ');
    if (!silent) setStatus('rulesStatus', 'ok', '서버에서 규칙을 불러왔습니다.');
  } catch (e) {
    if (!silent) setStatus('rulesStatus', 'err', e.message);
  } finally {
    btn.disabled = false;
  }
}

async function saveRules() {
  const btn = $('saveRulesBtn');
  btn.disabled = true;
  setStatus('rulesStatus', 'info', '규칙 저장 중…');
  try {
    await api('/api/review-reply/config', 'PUT', { rules: $('rules').value });
    setStatus('rulesStatus', 'ok', '규칙이 서버에 저장되었습니다.');
  } catch (e) {
    setStatus('rulesStatus', 'err', e.message);
  } finally {
    btn.disabled = false;
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  // 제목 옆 버전 표시 (필수)
  $('version').textContent = 'v' + chrome.runtime.getManifest().version;

  await loadStored();

  $('saveBtn').addEventListener('click', () => {
    saveStored().catch((e) => setStatus('connStatus', 'err', e.message));
  });
  $('testBtn').addEventListener('click', testConnection);
  $('loadRulesBtn').addEventListener('click', () => loadRules(false));
  $('saveRulesBtn').addEventListener('click', saveRules);

  // 토큰이 이미 있으면 규칙 자동 로드 시도 (실패해도 조용히)
  if (currentToken()) loadRules(true);
});
