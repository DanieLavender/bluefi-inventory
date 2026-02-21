# 블루파이(Bluefi) 재고관리 시스템

## 프로젝트 개요
- **이름**: bluefi-inventory
- **GitHub**: https://github.com/DanieLavender/bluefi-inventory
- **목적**: 블루파이 쇼핑몰 의류 재고를 웹에서 실시간으로 관리
- **원본 데이터**: `블루파이재고_260209.xlsx` (276개 상품, 16개 브랜드)

## 기술 스택
| 구분 | 기술 | 버전 |
|------|------|------|
| 런타임 | Node.js | v24+ |
| 서버 | Express | ^4.21.0 |
| DB | MySQL 8.0 + mysql2 | ^3.17.0 |
| 인증 | bcryptjs | ^3.0.2 |
| 환경변수 | dotenv | ^16.x |
| 프론트 | Vanilla HTML/CSS/JS | - |
| 푸시 알림 | web-push | ^3.x |
| 배포 | Render.com | - |

## 프로젝트 구조
```
C:\claude\Shoppingmall\
├── CLAUDE.md              ← 이 파일 (프로젝트 컨텍스트)
├── server.js              ← Express 서버 + REST API + 동기화 API
├── database.js            ← MySQL 초기화 + 시드 데이터 + sync 테이블
├── smartstore.js          ← 네이버 커머스 API 클라이언트 (NaverCommerceClient)
├── coupang.js             ← 쿠팡 API 클라이언트 (CoupangClient)
├── sync-scheduler.js      ← 반품→재등록 동기화 + 재고 자동 반영 + 매출 수집
├── package.json           ← 의존성 정의
├── package-lock.json
├── render.yaml            ← Render.com 배포 설정 (스토어 환경변수 포함)
├── .env.example           ← 환경변수 템플릿
├── .gitignore             ← node_modules, *.db, *.xlsx, .env 제외
├── inventory.db           ← (삭제됨, 외부 MySQL로 이전)
├── inventory.html         ← 구버전 단독 HTML (사용 안 함)
├── 블루파이재고_260209.xlsx ← 원본 엑셀 (git 제외)
├── inventory_data.json    ← 엑셀→JSON 변환 중간파일 (git 제외)
└── public/
    └── index.html         ← 프론트엔드 SPA (사이드바+상단바 레이아웃)
```

## DB 스키마 (MySQL 8.0)
```sql
CREATE TABLE inventory (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name TEXT NOT NULL,                        -- 상품명 (예: "hm 캐시미어 울 라운드니트")
  color VARCHAR(255) NOT NULL,               -- 컬러 (예: "아이보리")
  qty INT NOT NULL DEFAULT 0,                -- 수량
  brand VARCHAR(10) DEFAULT '',              -- 브랜드 코드 (예: "hm", "it", "ls")
  channel_product_no VARCHAR(255) DEFAULT NULL, -- 네이버 스토어 상품번호 (점진적 연결)
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT NULL           -- 수량 변경 시 자동 갱신
);
```
- **DB 호스팅**: 회사 MySQL 서버 (DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME 환경변수)
- 초기 데이터: database.js의 seedData() 함수에 276개 하드코딩
- brand는 상품명 앞 2글자 알파벳에서 자동 추출 (extractBrand 함수)
- updated_at은 수량 변경 시만 NOW()로 갱신
- channel_product_no: 반품 동기화 시 첫 텍스트 매칭 성공 시 자동 저장 (점진적 연결)
- database.js는 `mysql2` Pool 싱글톤 패턴, 모든 DB 접근은 비동기(`async/await`)
- `key` 컬럼은 MySQL 예약어 → 항상 백틱(`` `key` ``)으로 감싸기

## API 엔드포인트
| Method | Path | 설명 |
|--------|------|------|
| GET | /api/inventory | 재고 목록 (search, brand, sort, page, limit 파라미터) |
| GET | /api/stats | 통계 (totalItems, totalQty, brands, outOfStock) |
| GET | /api/brands | 브랜드 목록 |
| POST | /api/inventory | 재고 추가 (body: name, color, qty, brand?) |
| PUT | /api/inventory/:id | 수량 수정 (body: qty) → updated_at 자동 갱신 |
| DELETE | /api/inventory/:id | 단건 삭제 |
| POST | /api/inventory/delete-bulk | 일괄 삭제 (body: ids[]) |
| GET | /api/sales/stats | 오늘 매출 요약 (어제 비교) |
| GET | /api/sales/recent | 주문 목록 (store, date, limit 파라미터) |
| POST | /api/sales/fetch | 수동 매출 데이터 수집 (resetDays 옵션) |
| GET | /api/health | 헬스체크 (서버 keep-alive용) |
| GET | /api/push/vapid-key | VAPID 공개키 반환 |
| POST | /api/push/subscribe | 푸시 구독 저장 |
| POST | /api/push/unsubscribe | 푸시 구독 해제 |
| POST | /api/push/test | 테스트 푸시 발송 |

## 프론트엔드 기능
- **SPA 라우팅**: 해시 기반 (#inventory, #sales, #log, #brands, #settings, #login)
- **재고 조회**: 30개씩 페이지네이션, 상품명/컬러 검색, 브랜드 필터, 7종 정렬 (최근 수정순 포함)
- **재고 추가**: 모달에서 브랜드 선택(기존 목록 or 직접 입력) + 상품명/컬러/수량 입력 → "브랜드 상품명" 형태로 저장
- **수량 변경**: +/- 버튼 또는 숫자 클릭→직접 입력, DB 즉시 반영
- **재고 삭제**: ⋯ 드롭다운 메뉴 → "삭제" 클릭 또는 체크박스 선택 후 일괄 삭제 (확인 모달)
- **새로고침**: 상단바 버튼 → 회전 애니메이션 + 마지막 갱신 시간 표시
- **수정일 표시**: 수량 변경된 상품에 상대 시간 표시 (방금 전, 5분 전 등), 1시간 이내는 파란색 하이라이트
- **통계 대시보드**: 4열 카드 그리드 (아이콘 + 값 + 설명), fadeUp 애니메이션
- **컬러 칩**: 한국어 색상명 → HEX 매핑 (colorMap 객체), 컬러 dot(10px) + 텍스트
- **커스텀 체크박스**: CSS :checked + SVG 체크마크 (파란 배경)
- **브랜드 배지**: CSS 클래스 기반 (.brand-ag ~ .brand-vi, 16개 + default)
- **셸 페이지**: 매출 현황, 입출고 내역, 브랜드 관리, 설정, 로그인 (UI만)

## 레이아웃
- **사이드바**: 240px 고정, `position: fixed`, 다크 배경(#111), 메뉴 + 관리자 아바타
- **상단바**: 56px sticky, 좌측(페이지 제목 + 동기화 dot + 갱신시간), 우측(내보내기/재고추가/새로고침 버튼)
- **콘텐츠**: 카드형 컨테이너 (bg #FAFAFA)

## 디자인 시스템
- **폰트**: Pretendard Variable (CDN)
- **주요 변수**: `--accent: #2563EB`, `--bg-primary: #FAFAFA`, `--bg-card: #FFFFFF`
- **버튼**: accent(파란), primary(다크), danger(빨간), outline(테두리)
- **애니메이션**: fadeUp (카드, 모달), pulse (동기화 dot), spin (새로고침)

## 반응형 디자인
- **PC (769px+)**: 사이드바 + 상단바 + 테이블 레이아웃
- **태블릿 (1024px-)**: 통계 그리드 2x2
- **모바일 (768px-)**: 사이드바 숨김 + 햄버거 토글(overlay), 카드형 레이아웃
- **초소형 (380px-)**: 컴팩트 카드, 작은 폰트/여백

## 브랜드 목록 (16개)
ag, ed, hm, ig, it, lc, ls, mo, mu, mv, ov, ps, ru, sm, ve, vi
- 각 브랜드별 고유 색상 배지 (brandColors 객체에 정의)

## 배포
- **render.yaml** 설정 완료
- Render.com에서 GitHub 연결 → 자동 배포
- Build: `npm install` / Start: `node server.js`
- 환경변수: `NODE_ENV=production`, PORT는 Render가 자동 설정

## Git 컨벤션
- 커밋 메시지: 한글, 첫 줄 요약 + 본문 상세
- Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
- master 브랜치 단일 사용

## 스마트스토어 동기화 시스템

### 개요
A 스마트스토어 반품 완료 → B 스마트스토어 상품 수량 증가 + inventory 재고 자동 반영

### 동기화 흐름
1. A 스토어 반품 완료 건 조회 (네이버 커머스 API)
2. 반품 상세 조회 (상품명, 옵션, 수량)
3. product_mapping에서 B 매핑 확인
4. 매핑 있으면 → B 수량 증가 / 없으면 → 자동 매칭 시도 or 수동 매핑 유도
5. **inventory 재고 자동 반영** (반품 → 재고 수량 증가)
6. 모든 처리 sync_log에 기록 + 푸시 알림

### 재고 자동 반영 (updateInventoryFromReturn)
반품 건이 B 스토어에 처리된 후, inventory 테이블에도 자동 반영:

```
0단계: productOrderId로 중복 체크 → 이미 처리된 반품 스킵
1단계: channel_product_no로 직접 매칭 (연결된 재고)
2단계: 텍스트 정확 매칭 → 점진적으로 channel_product_no 연결 저장
3단계: 텍스트 유사 매칭 → 점진적 연결
4단계: 매칭 실패 → 신규 등록 (channel_product_no 포함)
```

- **수동 반영 추정**: 매칭된 재고의 updated_at이 last_sync_time 이후면 수량 건드리지 않고 연결만 저장
- **점진적 연결**: 기존 276개 재고는 channel_product_no가 null, 반품 처리 시 자동 연결

### Sync API
| Method | Path | 설명 |
|--------|------|------|
| POST | /api/sync/run | 수동 즉시 동기화 |
| GET | /api/sync/status | 동기화 상태 |
| POST | /api/sync/start | 자동 스케줄러 시작 |
| POST | /api/sync/stop | 자동 스케줄러 중지 |
| GET | /api/sync/logs | 동기화 로그 (필터, 페이지네이션) |
| GET | /api/sync/config | 설정 조회 (secret 마스킹) |
| PUT | /api/sync/config | 설정 수정 (UPSERT) |
| GET | /api/sync/mappings | 상품 매핑 목록 |
| PUT | /api/sync/mappings/:id | 수동 매핑 설정 |
| POST | /api/sync/test-connection | 연결 테스트 |

### Sync DB 테이블
- **sync_log**: 동기화 이력 (run_id, type, status, product info)
  - type: `return_detect`, `qty_increase`, `product_create`, `inventory_update`, `error`
- **sync_config**: key-value 설정 (sync_enabled, sync_interval_minutes 등)
- **product_mapping**: A↔B 상품 매핑 (match_status: matched/unmatched/manual)
- **sales_orders**: 매출 주문 데이터 (store A/B/C, product_order_id UNIQUE)
- **push_subscriptions**: 웹 푸시 구독 (endpoint, p256dh, auth)

### 환경변수
```
STORE_A_CLIENT_ID / STORE_A_CLIENT_SECRET — A 스토어 API 키
STORE_B_CLIENT_ID / STORE_B_CLIENT_SECRET — B 스토어 API 키
COUPANG_ACCESS_KEY / COUPANG_SECRET_KEY / COUPANG_VENDOR_ID — 쿠팡 API 키
RENDER_GIT_COMMIT — Render 자동 설정 (앱 업데이트 감지용)
```

## 푸시 알림 (PWA Web Push)
- **VAPID 키**: 최초 실행 시 자동 생성 → sync_config에 저장
- **알림 발생 조건**:
  - 반품 → B 스토어 신규 등록 시
  - 반품 → 재고 자동 반영 시
  - 신규 매출 발생 시
  - 앱 업데이트 배포 시 (RENDER_GIT_COMMIT 변경 감지)

## 매출 수집
- **네이버 A/B 스토어** + **쿠팡** 매출 자동 수집
- 동기화 실행 시 자동 수집 (fetchSalesData)
- 수동 수집: POST /api/sales/fetch (resetDays로 재수집 가능)
- INSERT IGNORE + affectedRows 확인으로 중복 카운트 방지

## 주의사항
- **DB**: MySQL 8.0 사용, `DB_HOST/DB_USER/DB_PASSWORD/DB_NAME` 환경변수 필수
- SQL 파라미터: `?` 플레이스홀더 사용 (mysql2)
- `mysql2` 드라이버: SUM은 DECIMAL(문자열) 반환 → `Number()` 변환 필요
- `key` 컬럼: MySQL 예약어이므로 항상 `` `key` ``로 감싸야 함
- UPSERT: `ON DUPLICATE KEY UPDATE value = VALUES(value)` 패턴
- INSERT 후 결과: `RETURNING *` 미지원 → `result.insertId`로 별도 SELECT
- inventory 테이블이 비어있으면 initDb()에서 276개 시드 자동 삽입
- extractBrand()가 server.js와 database.js에 각각 존재 (중복이지만 의도적 — 모듈 독립성)
- 모든 DB 접근은 `async/await` — server.js, sync-scheduler.js 전체 비동기
- INSERT IGNORE 후 실제 삽입 확인: `result.affectedRows > 0` (매출 수집 등)
