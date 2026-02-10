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
| DB | better-sqlite3 | ^11.7.0 |
| 프론트 | Vanilla HTML/CSS/JS | - |
| 배포 | Render.com (예정) | - |

## 프로젝트 구조
```
C:\claude\Shoppingmall\
├── CLAUDE.md              ← 이 파일 (프로젝트 컨텍스트)
├── server.js              ← Express 서버 + REST API (171줄)
├── database.js            ← SQLite 초기화 + 시드 데이터 (339줄)
├── package.json           ← 의존성 정의
├── package-lock.json
├── render.yaml            ← Render.com 배포 설정
├── .gitignore             ← node_modules, *.db, *.xlsx 제외
├── inventory.db           ← SQLite DB (자동 생성, git 제외)
├── inventory.html         ← 구버전 단독 HTML (사용 안 함)
├── 블루파이재고_260209.xlsx ← 원본 엑셀 (git 제외)
├── inventory_data.json    ← 엑셀→JSON 변환 중간파일 (git 제외)
└── public/
    └── index.html         ← 프론트엔드 메인 화면 (1060줄)
```

## DB 스키마
```sql
CREATE TABLE inventory (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,          -- 상품명 (예: "hm 캐시미어 울 라운드니트")
  color TEXT NOT NULL,         -- 컬러 (예: "아이보리")
  qty INTEGER NOT NULL DEFAULT 0,  -- 수량
  brand TEXT DEFAULT '',       -- 브랜드 코드 (예: "hm", "it", "ls")
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT NULL  -- 수량 변경 시 자동 갱신
);
```
- 초기 데이터: database.js의 seedData() 함수에 276개 하드코딩
- brand는 상품명 앞 2글자 알파벳에서 자동 추출 (extractBrand 함수)
- updated_at은 수량 변경 시만 CURRENT_TIMESTAMP로 갱신

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

## 프론트엔드 기능
- **재고 조회**: 30개씩 페이지네이션, 상품명/컬러 검색, 브랜드 필터, 7종 정렬 (최근 수정순 포함)
- **재고 추가**: 모달에서 브랜드 선택(기존 목록 or 직접 입력) + 상품명/컬러/수량 입력 → "브랜드 상품명" 형태로 저장
- **수량 변경**: +/- 버튼 또는 숫자 클릭→직접 입력, DB 즉시 반영
- **재고 삭제**: 개별 X 버튼 또는 체크박스 선택 후 일괄 삭제 (확인 모달)
- **새로고침**: 버튼 클릭 → 회전 애니메이션 + 마지막 갱신 시간 표시
- **수정일 표시**: 수량 변경된 상품에 상대 시간 표시 (방금 전, 5분 전 등), 1시간 이내는 파란색 하이라이트
- **통계 대시보드**: 총 상품 종류, 총 재고 수량, 브랜드 수, 품절 상품 수

## 반응형 디자인
- **PC (769px+)**: 테이블 레이아웃
- **모바일 (768px-)**: 카드형 레이아웃, 세로 배치, 36px 터치 버튼, 2x2 통계 그리드
- **초소형 (380px-)**: 더 작은 폰트/여백

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

## 주의사항
- SQL 문자열 비교 시 빈 문자열은 `''` (쌍따옴표 아님) 사용 — SQLite에서 `""` 는 식별자
- inventory.db는 .gitignore에 포함 → 배포 시 서버에서 자동 생성 + seedData 실행
- extractBrand()가 server.js와 database.js에 각각 존재 (중복이지만 의도적 — 모듈 독립성)
- 기존 DB에 updated_at 컬럼이 없으면 initDb()에서 ALTER TABLE로 자동 마이그레이션
