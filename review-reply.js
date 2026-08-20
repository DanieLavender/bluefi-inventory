// 스마트스토어 리뷰 답글 도우미 — 서버 측 로직
// (프롬프트 조립 + Gemini 호출 + 리뷰 텍스트 정제)
// 설계 근거: 인수인계 문서 "이미 밟아본 지뢰 열 개" — 각 항목을 주석으로 표기

const GEMINI_BASE = 'https://generativelanguage.googleapis.com';
const DEFAULT_MODEL = 'gemini-3.6-flash'; // 지뢰 2: gemini-2.5-flash는 신규 사용자 차단됨

// ===== 리뷰 텍스트 정제 =====
// 지뢰 7: 신체정보·판매자센터 UI 문구가 스크래핑에 섞여 들어옴 — 서버에서도 이중 방어
const UI_NOISE_PATTERNS = [
  /반복적인\s*답글이\s*아닌\s*정성스러운\s*답글/,
  /고객에게\s*전하고\s*싶은\s*내용을\s*남겨/,
  /답글\s*등록\s*시\s*구매자에게/,
];
const BODY_INFO_LINE = /(유저\s*정보|체형|평소\s*사이즈|\d{2,3}\s*cm|\d{2,3}\s*kg)/;

function sanitizeReviewText(text) {
  if (!text) return '';
  return String(text)
    .split(/\n/)
    .filter(line => {
      const t = line.trim();
      if (!t) return false;
      if (BODY_INFO_LINE.test(t)) return false;
      if (UI_NOISE_PATTERNS.some(re => re.test(t))) return false;
      return true;
    })
    .join('\n')
    .slice(0, 2000);
}

// ===== 소재 추출 =====
// 지뢰 9: 상품명의 계절 단어(봄/여름)를 소재로 오인 — 명시적 소재 단어만 인정
const MATERIAL_WORDS = [
  '캐시미어', '울', '메리노', '알파카', '앙고라', '라쿤', '모헤어',
  '면', '코튼', '수피마', '오가닉코튼', '린넨', '마',
  '텐셀', '모달', '레이온', '비스코스', '실크', '한지',
  '아크릴', '폴리', '폴리에스터', '나일론', '스판',
];
function extractExplicitMaterial(name) {
  if (!name) return null;
  // 긴 단어 우선 매칭 후 소거 — '수피마'의 '마', '오가닉코튼'의 '코튼' 같은 부분 문자열 중복 방지
  let rest = name;
  const found = [];
  for (const w of [...MATERIAL_WORDS].sort((a, b) => b.length - a.length)) {
    if (rest.includes(w)) {
      found.push(w);
      rest = rest.split(w).join(' ');
    }
  }
  return found.length > 0 ? found.join(', ') : null;
}

// ===== 시스템 프롬프트 조립 =====
// 인수인계 문서의 buildSystemPrompt 역할. 모든 지침이 이 함수 한 곳에 모임.
function buildSystemPrompt({ rules, product, rating, productName, pastReplies, siblingReviews, single }) {
  const isLowRating = Number(rating) > 0 && Number(rating) <= 2;
  const material = (product && product.material) || extractExplicitMaterial(productName);

  const lines = [];
  lines.push('당신은 한국 여성의류 쇼핑몰 "블루파이"의 스마트스토어 리뷰 답글 담당자입니다.');
  if (single) {
    // Ollama 등 소형 모델용: 한 호출에 1개만 — 지시 준수율이 훨씬 높음
    lines.push('구매자가 남긴 리뷰에 대한 판매자 답글을 정확히 1개만 작성하세요.');
    lines.push('답글 본문만 출력하세요. 설명·번호·머리말·구분선은 일절 출력하지 마세요.');
  } else {
    lines.push('구매자가 남긴 리뷰에 대한 판매자 답글 후보를 정확히 3개 작성하세요.');
    lines.push('각 후보는 반드시 "---" 한 줄로만 구분하세요. 다른 설명·번호·머리말은 일절 출력하지 마세요.');
  }
  // 현재 계절 주입 — 상품명의 계절 단어에 끌려 엉뚱한 계절 인사를 하는 것 방지
  const month = new Date(Date.now() + 9 * 60 * 60 * 1000).getUTCMonth() + 1; // KST
  const season = month >= 3 && month <= 5 ? '봄' : month >= 6 && month <= 8 ? '여름' : month >= 9 && month <= 11 ? '가을' : '겨울';
  lines.push(`오늘은 ${month}월(${season})입니다. 계절을 언급할 때는 반드시 지금 계절 기준으로만 말하고, 상품명에 있는 계절 단어를 현재 계절로 착각하지 마세요. 확실하지 않으면 계절 언급을 생략하세요.`);
  lines.push('');

  lines.push('## 답글 구조 (4줄, 반드시 준수)');
  // 지뢰 10: 금지만 하면 상투어로 되살아남 — 대체 행동을 명시적으로 지정
  lines.push('1줄: 리뷰에 대한 감사 인사');
  lines.push('2줄: 아래 우선순위 중 리뷰에 실제로 언급된 첫 항목에 대한 반응 — (1) 배송·포장 (2) 재구매·재방문 (3) 리뷰를 정성껏 써주신 점. 어느 것도 없으면 리뷰의 사실 관계 중 주관 판단이 아닌 것 하나.');
  lines.push('3줄: 상품에 대한 한마디');
  lines.push('4줄: 마무리 인사');
  lines.push('');

  lines.push('## 절대 규칙');
  lines.push('- 당신은 판매자입니다. 모든 문장은 고객에게 말을 거는 판매자 시점으로 씁니다. 고객의 리뷰 문장을 자신의 감상처럼 되풀이하는 것("~해서 참 좋네요", "~라 다행입니다" 같은 구매자 어투)은 금지입니다.');
  lines.push('- 리뷰 문장을 그대로 되풀이하지 않습니다.');
  lines.push('- 고객이 리뷰에 쓴 신체 관련 표현(배, 뱃살, 몸매, 체형, 팔뚝, 허벅지, 키, 몸무게 등)을 답글에서 반복하거나 언급하지 않습니다. 공개 답글이므로 고객이 민망할 수 있습니다.');
  lines.push('- 마무리 인사는 딱 한 번만 합니다. 마지막 문장이 규칙으로 정해져 있으면 그 문장 외의 추가 인사("기분 좋게 입으세요" 등)를 덧붙이지 않습니다.');
  // 지뢰 8: 리뷰 사진과 썸네일 구분 불가 — 사진 언급 자체 금지
  lines.push('- 사진·이미지에 대한 언급을 하지 않습니다 (사진 첨부 여부를 알 수 없습니다).');
  lines.push('- 고객의 키·몸무게·체형 등 신체정보를 언급하지 않습니다.');
  // 바꾸면 안 되는 것 4: 주관 판단을 판매자가 단정하지 않음
  lines.push('- 핏·색감 등 주관 항목을 판매자가 단정하지 않습니다. "핏이 예쁘게 떨어져요"(금지) 대신 "잘 맞으셨다니 반가워요"처럼 이 고객 한 사람으로 한정하는 어법만 사용합니다.');
  if (material) {
    lines.push(`- 이 상품의 소재는 "${material}" 입니다. 소재를 언급할 때는 이것만 사용합니다.`);
  } else {
    lines.push('- 소재 정보가 없습니다. 소재·촉감·계절 적합성을 추측해서 언급하지 않습니다. 상품명의 계절 단어(봄/여름/가을/겨울)는 소재가 아닙니다.');
  }
  lines.push('');

  if (isLowRating) {
    // 바꾸면 안 되는 것 2: 저평점 분기 유지
    lines.push('## 저평점(1~2점) 리뷰입니다');
    lines.push('- 재구매·재방문 유도 문구를 넣지 않습니다.');
    lines.push('- 스토어 답글 규칙을 적용하지 않습니다.');
    lines.push('- 불편에 대한 진심 어린 사과와, 문제를 확인하겠다는 태도를 중심으로 작성합니다.');
    lines.push('- 변명하거나 고객 과실을 암시하지 않습니다.');
  } else if (rules && rules.trim()) {
    lines.push('## 스토어 답글 규칙 (사장님이 직접 작성 — 우선 적용)');
    lines.push(rules.trim());
  }

  if (product && product.info) {
    lines.push('');
    lines.push('## 상품 정보 (DB 조회 결과 — 이 범위에서만 상품을 언급)');
    lines.push(product.info);
  }

  // 같은 상품에 이미 나간 답글들 — 표현 중복 회피 (지뢰 10: 금지 대신 대체 행동 지시)
  const priorReplies = [];
  (siblingReviews || []).forEach(s => { if (s && s.existingReply) priorReplies.push(s.existingReply); });
  (pastReplies || []).forEach(r => { if (r) priorReplies.push(r); });
  if (priorReplies.length > 0) {
    lines.push('');
    lines.push('## 같은 상품에 이미 등록된 답글들');
    priorReplies.slice(0, 5).forEach((r, i) => {
      lines.push(`[기존 답글 ${i + 1}] ${String(r).replace(/\s+/g, ' ').slice(0, 300)}`);
    });
    lines.push('위 답글들과 인사말·상품 한마디·마무리 문장이 겹치지 않도록, 매 줄마다 다른 표현을 선택해 새로 작성하세요. 구조(4줄)는 유지합니다.');
  }

  // 같은 상품의 다른 구매자 리뷰 — 경향 참고용 (답변 대상 아님)
  // 심층 방어: 호출부에서 정제했더라도 여기서 한 번 더 신체정보·UI 문구 제거
  const otherReviews = (siblingReviews || [])
    .map(s => s && sanitizeReviewText(s.reviewText))
    .filter(Boolean);
  if (otherReviews.length > 0) {
    lines.push('');
    lines.push('## 같은 상품의 다른 구매자 리뷰 (경향 참고용)');
    otherReviews.slice(0, 5).forEach((t, i) => {
      lines.push(`[다른 리뷰 ${i + 1}] ${String(t).replace(/\s+/g, ' ').slice(0, 300)}`);
    });
    lines.push('여러 구매자가 공통으로 만족한 점이 보이면 상품 한마디(3줄)에 자연스럽게 반영해도 됩니다. 단, 답글은 오직 현재 리뷰 작성자에게만 씁니다.');
  }

  return lines.join('\n');
}

// ===== Gemini 호출 =====
// 지뢰 3: 엔드포인트 이원화 — 현행(interactions) 먼저, 실패 시 legacy(generateContent) 폴백
async function geminiInteractions(apiKey, model, systemPrompt, userText) {
  const res = await fetch(`${GEMINI_BASE}/v1beta/interactions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-goog-api-key': apiKey },
    body: JSON.stringify({
      model: `models/${model}`,
      system_instruction: { parts: [{ text: systemPrompt }] },
      input: [{ role: 'user', parts: [{ text: userText }] }],
    }),
  });
  const body = await res.text();
  if (!res.ok) throw new Error(`interactions ${res.status}: ${body.slice(0, 500)}`);
  const data = JSON.parse(body);
  const text = data.output?.[0]?.content?.parts?.map(p => p.text).join('')
    || data.candidates?.[0]?.content?.parts?.map(p => p.text).join('');
  if (!text) throw new Error('interactions: 빈 응답');
  return text;
}

async function geminiGenerateContent(apiKey, model, systemPrompt, userText) {
  const res = await fetch(`${GEMINI_BASE}/v1beta/models/${model}:generateContent`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-goog-api-key': apiKey },
    body: JSON.stringify({
      systemInstruction: { parts: [{ text: systemPrompt }] },
      contents: [{ role: 'user', parts: [{ text: userText }] }],
    }),
  });
  const body = await res.text();
  if (!res.ok) {
    // 지뢰 2: 404 응답 본문에 구글이 권장 모델명을 담아 보냄 — 파싱해 안내
    let hint = '';
    if (res.status === 404) {
      const m = body.match(/gemini-[\w.\-]+/g);
      if (m) hint = ` (권장 모델: ${[...new Set(m)].join(', ')})`;
    }
    throw new Error(`Gemini API 오류 (${res.status})${hint}: ${body.slice(0, 500)}`);
  }
  const data = JSON.parse(body);
  const text = data.candidates?.[0]?.content?.parts?.map(p => p.text).join('');
  if (!text) throw new Error('Gemini: 빈 응답');
  return text;
}

async function callGemini(apiKey, model, systemPrompt, userText) {
  const useModel = model || DEFAULT_MODEL;
  try {
    return await geminiInteractions(apiKey, useModel, systemPrompt, userText);
  } catch (e1) {
    try {
      return await geminiGenerateContent(apiKey, useModel, systemPrompt, userText);
    } catch (e2) {
      // legacy 쪽 오류가 사용자에게 더 유용 (권장 모델 힌트 포함)
      throw e2;
    }
  }
}

// ===== Ollama (OpenAI 호환 API) — Gemini 폴백 =====
async function callOllama(baseUrl, apiKey, model, systemPrompt, userText, temperature = 0.7) {
  const url = String(baseUrl).replace(/\/+$/, '') + '/chat/completions';
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + (apiKey || ''),
    },
    body: JSON.stringify({
      model,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userText },
      ],
      temperature,
    }),
  });
  const body = await res.text();
  if (!res.ok) throw new Error(`Ollama API 오류 (${res.status}): ${body.slice(0, 300)}`);
  const data = JSON.parse(body);
  const text = data.choices?.[0]?.message?.content;
  if (!text) throw new Error('Ollama: 빈 응답');
  return text;
}

// Gemini 우선 → 실패(429 한도 등) 시 Ollama 폴백
// 통신 전략: Gemini는 토큰 비용이 있으므로 1회 호출로 후보 3개(--- 구분).
// Ollama는 비용이 없으므로 병렬 5회 호출(호출당 후보 1개, 온도 다양화) — 소형 모델 지시 준수율도 개선.
const OLLAMA_TEMPS = [0.6, 0.8, 0.95, 1.05, 0.9];

async function generateWithFallback(cfg, prompts, userText) {
  const errors = [];
  if (cfg.geminiKey) {
    try {
      const text = await callGemini(cfg.geminiKey, cfg.model, prompts.multi, userText);
      const candidates = parseCandidates(text);
      if (candidates.length > 0) {
        return { candidates, provider: 'gemini:' + (cfg.model || DEFAULT_MODEL) };
      }
      errors.push('Gemini: 응답에서 후보를 추출하지 못했습니다.');
    } catch (e) {
      errors.push('Gemini: ' + e.message);
    }
  }
  if (cfg.ollamaUrl && cfg.ollamaModel) {
    const settled = await Promise.allSettled(
      OLLAMA_TEMPS.map(t => callOllama(cfg.ollamaUrl, cfg.ollamaKey, cfg.ollamaModel, prompts.single, userText, t))
    );
    const candidates = [];
    const seen = new Set();
    for (const s of settled) {
      if (s.status !== 'fulfilled') continue;
      const c = (parseCandidates(s.value)[0] || String(s.value)).trim();
      const norm = c.replace(/\s+/g, ' ').trim();
      if (norm && !seen.has(norm)) {
        seen.add(norm);
        candidates.push(c);
      }
    }
    if (candidates.length > 0) {
      return { candidates: candidates.slice(0, 5), provider: 'ollama:' + cfg.ollamaModel };
    }
    const firstFail = settled.find(s => s.status === 'rejected');
    errors.push('Ollama: ' + (firstFail ? firstFail.reason.message : '유효한 후보 없음'));
  }
  throw new Error(errors.length > 0 ? errors.join('\n') : 'AI 제공자(Gemini 또는 Ollama)가 설정되지 않았습니다.');
}

// ===== 상세 본문 HTML → 텍스트 =====
// 상세페이지 본문에서 텍스트만 추출 (이미지·스크립트 제외). 프롬프트 참고용.
function htmlToText(html) {
  if (!html) return '';
  return String(html)
    .replace(/<(script|style)[\s\S]*?<\/\1>/gi, ' ')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/(p|div|li|h[1-6]|tr)>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&quot;/gi, '"')
    .replace(/[ \t]+/g, ' ')
    .replace(/\s*\n\s*/g, '\n')
    .replace(/\n{2,}/g, '\n')
    .trim();
}

// ===== 후보 분리 =====
function parseCandidates(text) {
  return String(text)
    .split(/\n-{3,}\n?|^-{3,}$/m)
    .map(s => s.trim())
    .filter(Boolean)
    .slice(0, 3);
}

module.exports = {
  DEFAULT_MODEL,
  sanitizeReviewText,
  extractExplicitMaterial,
  buildSystemPrompt,
  callGemini,
  callOllama,
  generateWithFallback,
  parseCandidates,
  htmlToText,
};
