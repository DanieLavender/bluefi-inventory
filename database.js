const mysql = require('mysql2/promise');

let pool = null;

function getPool() {
  if (!pool) {
    pool = mysql.createPool({
      host: process.env.DB_HOST,
      port: parseInt(process.env.DB_PORT) || 3306,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      charset: 'utf8mb4',
      timezone: '+09:00',
      waitForConnections: true,
      connectionLimit: 10,
    });
  }
  return pool;
}

async function query(sql, params) {
  const [result] = await getPool().query(sql, params || []);
  return result;
}

async function initDb() {
  await query(`
    CREATE TABLE IF NOT EXISTS inventory (
      id INT AUTO_INCREMENT PRIMARY KEY,
      name TEXT NOT NULL,
      color VARCHAR(255) NOT NULL,
      qty INT NOT NULL DEFAULT 0,
      brand VARCHAR(10) DEFAULT '',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  // inventory에 channel_product_no 컬럼 추가 (점진적 스토어↔재고 연결용)
  try {
    await query(`ALTER TABLE inventory ADD COLUMN channel_product_no VARCHAR(255) DEFAULT NULL`);
  } catch (e) {
    // 이미 존재하면 무시 (ER_DUP_FIELDNAME)
  }

  // inventory에 size 컬럼 추가
  try {
    await query(`ALTER TABLE inventory ADD COLUMN size VARCHAR(255) DEFAULT NULL`);
  } catch (e) {
    // 이미 존재하면 무시
  }

  await query(`
    CREATE TABLE IF NOT EXISTS sync_log (
      id INT AUTO_INCREMENT PRIMARY KEY,
      run_id VARCHAR(255) NOT NULL,
      type VARCHAR(50) NOT NULL,
      store_from VARCHAR(10) NOT NULL,
      store_to VARCHAR(10) DEFAULT NULL,
      product_order_id VARCHAR(255) DEFAULT NULL,
      channel_product_no VARCHAR(255) DEFAULT NULL,
      product_name TEXT DEFAULT NULL,
      product_option TEXT DEFAULT NULL,
      qty INT DEFAULT 0,
      status VARCHAR(20) NOT NULL DEFAULT 'success',
      message TEXT DEFAULT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS sync_config (
      id INT AUTO_INCREMENT PRIMARY KEY,
      \`key\` VARCHAR(255) UNIQUE NOT NULL,
      value TEXT NOT NULL,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS product_mapping (
      id INT AUTO_INCREMENT PRIMARY KEY,
      store_a_channel_product_no VARCHAR(255) NOT NULL,
      store_a_product_name TEXT NOT NULL,
      store_a_option_name VARCHAR(255) DEFAULT NULL,
      store_b_channel_product_no VARCHAR(255) DEFAULT NULL,
      store_b_product_name TEXT DEFAULT NULL,
      store_b_option_name VARCHAR(255) DEFAULT NULL,
      match_status VARCHAR(20) DEFAULT 'unmatched',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY (store_a_channel_product_no, store_a_option_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS sales_orders (
      id INT AUTO_INCREMENT PRIMARY KEY,
      store CHAR(1) NOT NULL,
      product_order_id VARCHAR(255) UNIQUE NOT NULL,
      order_date DATETIME NOT NULL,
      product_name TEXT,
      option_name VARCHAR(255) DEFAULT NULL,
      qty INT DEFAULT 1,
      unit_price INT DEFAULT 0,
      total_amount INT DEFAULT 0,
      product_order_status VARCHAR(50),
      channel_product_no VARCHAR(255) DEFAULT NULL,
      fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS push_subscriptions (
      id INT AUTO_INCREMENT PRIMARY KEY,
      endpoint VARCHAR(500) UNIQUE NOT NULL,
      p256dh VARCHAR(255) NOT NULL,
      auth VARCHAR(255) NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS return_confirmations (
      id INT AUTO_INCREMENT PRIMARY KEY,
      product_order_id VARCHAR(255) UNIQUE NOT NULL,
      store CHAR(1) NOT NULL,
      product_name TEXT DEFAULT NULL,
      option_name VARCHAR(255) DEFAULT NULL,
      qty INT DEFAULT 1,
      channel_product_no VARCHAR(255) DEFAULT NULL,
      confirmed_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  // 기존 UTC 저장 order_date를 KST로 마이그레이션 (1회성)
  const [migrated] = await getPool().query(
    "SELECT value FROM sync_config WHERE `key` = 'sales_tz_migrated'"
  ).catch(() => [[]]);
  if (!migrated || migrated.length === 0 || migrated[0]?.value !== 'true') {
    await query(
      "UPDATE sales_orders SET order_date = DATE_ADD(order_date, INTERVAL 9 HOUR) WHERE order_date IS NOT NULL"
    ).catch(() => {});
    await query(
      "INSERT INTO sync_config (`key`, value) VALUES ('sales_tz_migrated', 'true') ON DUPLICATE KEY UPDATE value = 'true'"
    ).catch(() => {});
    console.log('[DB] sales_orders order_date UTC→KST 마이그레이션 완료');
  }

  // 네이버 매출 UTC 문제 수정: 삭제 후 재수집 유도 (1회성)
  const [naverTzFixed] = await getPool().query(
    "SELECT value FROM sync_config WHERE `key` = 'naver_sales_tz_fixed'"
  ).catch(() => [[]]);
  if (!naverTzFixed || naverTzFixed.length === 0 || naverTzFixed[0]?.value !== 'true') {
    await query("DELETE FROM sales_orders WHERE store IN ('A', 'B')").catch(() => {});
    await query("UPDATE sync_config SET value = '' WHERE `key` IN ('sales_last_fetch_a', 'sales_last_fetch_b')").catch(() => {});
    await query(
      "INSERT INTO sync_config (`key`, value) VALUES ('naver_sales_tz_fixed', 'true') ON DUPLICATE KEY UPDATE value = 'true'"
    ).catch(() => {});
    console.log('[DB] 네이버 매출 데이터 삭제 (UTC→KST 재수집 유도)');
  }

  // 쿠팡 금액 0 레코드 삭제 마이그레이션 (1회성 - 가격 필드 수정 전 데이터)
  const [cpgMigrated] = await getPool().query(
    "SELECT value FROM sync_config WHERE `key` = 'cpg_price_fix_migrated'"
  ).catch(() => [[]]);
  if (!cpgMigrated || cpgMigrated.length === 0 || cpgMigrated[0]?.value !== 'true') {
    const delResult = await query(
      "DELETE FROM sales_orders WHERE store = 'C' AND total_amount = 0"
    ).catch(() => ({ affectedRows: 0 }));
    // sales_last_fetch_c 리셋하여 재수집 유도
    await query(
      "UPDATE sync_config SET value = '' WHERE `key` = 'sales_last_fetch_c'"
    ).catch(() => {});
    await query(
      "INSERT INTO sync_config (`key`, value) VALUES ('cpg_price_fix_migrated', 'true') ON DUPLICATE KEY UPDATE value = 'true'"
    ).catch(() => {});
    console.log(`[DB] 쿠팡 금액 0 레코드 삭제: ${delResult.affectedRows || 0}건, last_fetch 리셋`);
  }

  // Seed sync_config defaults
  const configDefaults = [
    ['sync_enabled', 'false'],
    ['sync_interval_minutes', '5'],
    ['product_match_mode', 'name'],
    ['last_sync_time', ''],
    ['store_b_display_status', 'SUSPENSION'],
    ['store_b_sale_status', 'SALE'],
    ['sales_last_fetch_a', ''],
    ['sales_last_fetch_b', ''],
    ['store_b_name_prefix', '(오늘출발)'],
    ['sales_last_fetch_c', ''],
    ['coupang_access_key', ''],
    ['coupang_secret_key', ''],
    ['coupang_vendor_id', ''],
    ['vapid_public_key', ''],
    ['vapid_private_key', ''],
  ];
  for (const [k, v] of configDefaults) {
    await query(
      'INSERT IGNORE INTO sync_config (`key`, value) VALUES (?, ?)',
      [k, v]
    );
  }

  // Seed inventory if empty
  const countRows = await query('SELECT COUNT(*) as cnt FROM inventory');
  if (countRows[0].cnt === 0) {
    await seedData();
  }
}

function extractBrand(name) {
  if (!name) return '';
  const trimmed = name.trim();
  const match = trimmed.match(/^([a-zA-Z]{2})\s/);
  if (match) return match[1].toLowerCase();
  return '';
}

async function seedData() {
  const rawData = [
    ["ag 빅카라 코튼 반팔 롱 원피스","스카이블루",1],
    ["ag 골지원피스 카라 텐셀 스판 봄 여름","핑크",1],
    ["ag 봄 와이드 데님팬츠 진청 블랙진","진청 S",1],
    ["ag 바스락원피스 리에 카라 코튼 반팔 셔츠원피스","핑크 M",1],
    ["ag 에르 밴딩 팬츠","베이지",1],
    ["ag 에르 밴딩 팬츠","아이보리",1],
    ["ag 봄 반팔 라운드티셔츠","아이보리",1],
    ["ag 봄 반팔 라운드티셔츠","다크베이지",1],
    ["ed 울 텐셀 터틀넥 폴라","살몬핑크 M",1],
    ["hm 팜므 폭스 울 터틀넥 홀가먼트 니트 말림","베이지",1],
    ["hm 라쿤 폭스 울 터틀넥니트] 심플 홀가먼트","크림",1],
    ["hm 캐시미어 울 터틀넥니트] 코지 홀가먼트","와인",1],
    ["hm 홀가먼트 코지 캐시미어 울 터틀넥 니트","블루",1],
    ["hm 달링 후드","그레이",0],
    ["hm 폭스 울 후드니트] 몽크 홀가먼트","연그레이",1],
    ["hm [라쿤 롱 니트가디건] 포켓 폭스 울 숄","브라운",1],
    ["hm 라쿤 울 크롭 니트가디건 마고 라운드","소라",1],
    ["hm 홀가먼트가디건 쥬드 린넨 면 오픈 니트","소라",1],
    ["hm 린넨 후드 루즈핏 긴팔 니트가디건 홀가먼트","스카이블루",1],
    ["hm 캐시미어가디건] 홀가먼트 델라","진그레이",1],
    ["hm 후드 니트가디건 홀가먼트 바닝","크림",1],
    ["hm 후드 니트가디건 홀가먼트 바닝","베이지",1],
    ["hm 라쿤 폭스 울 라운드 반팔니트 홀가먼트 쿠나","연베이지",1],
    ["hm 라쿤 폭스 울 브이넥","핑크",1],
    ["hm 라쿤니트 로지 라쿤 울 라운드 반팔 숏 크롭","연그레이",1],
    ["hm 린넨 반팔 라운드니트 홀가먼트 바이엘","브라운",1],
    ["hm 린넨 루즈 반팔 브이넥니트 홀가먼트 가넷","차콜",1],
    ["hm 린넨 카라 반팔니트 홀가먼트 러브유","핑크",1],
    ["hm 봄 박시 반팔니트 홀가먼트 망투 캐시미어 울","딥블루",1],
    ["hm 캐시미어 울 루즈핏 브이넥니트 홀가먼트 로잔","레드",1],
    ["hm 라운드니트 홀가먼트 허니콤","블루",1],
    ["hm 캐시미어 울 입술넥 반팔 루나","아이보리",1],
    ["hm 캐시미어 울 입술넥 반팔 루나","아이보리",1],
    ["hm 캐시미어 울 7부 라운드 홀가먼트 엘린","아이보리",1],
    ["hm 뉴 라쿤 폭스 울 라운드니트 베이직 긴팔 숏","크림",1],
    ["hm 라쿤 폭스 울 목폴라 롱 홀가먼트 니트 원피스","그레이",1],
    ["hm 후드니트원피스 홀가먼트 캐시미어 울 롱","크림",1],
    ["hm 캐시미어 울 터틀넥 홀가먼트원피스","베이지",1],
    ["hm 베체 캐시미어 울 맥시 홀가먼트원피스","블랙",1],
    ["hm 캐시미어 울 롱 니트원피스] 비체 홀가먼트","크림",1],
    ["hm 캐시미어 울 롱 니트원피스] 비체 홀가먼트","브라운",1],
    ["hm 린넨가디건 기본 원버튼 니트","그린",1],
    ["hm 캐시미어 울 롱 니트스커트 홀가먼트 루아","딥블루",1],
    ["hm 폭스 울 후드 니트 바라클라바 넥워머","오트밀",1],
    ["hm 달링 팬츠","그레이",0],
    ["ig 린넨가디건 이프 브이넥 반팔 숏 니트","블루그레이",1],
    ["ig 린넨가디건 썸머 긴팔 시스루","화이트",1],
    ["it 봄 여름 린넨 니트 숄 베스트 가디건 홀가먼트 볼륨","인디핑크",1],
    ["it 아르벤 볼륨 반팔 브이넥","화이트",1],
    ["it 홀가먼트니트","그레이",1],
    ["it 홀가먼트 베이직 캐시미어 울 라운드 니트","민트",1],
    ["it 캐시미어 울 반목 크랍 니트케이프 홀가먼트","베이지",1],
    ["it 캐시미어 울 반목 크롭 니트케이프 홀가먼트","연베이지",1],
    ["it 베이비알파카 울 터틀넥 니트케이프] 홀가먼트 루즈핏 판초","다크베이지",1],
    ["it 베이비알파카 울 터틀넥 니트케이프] 홀가먼트 루즈핏 판초","브라운",1],
    ["it 홀가먼트 버튼 울 55 니트 가디건","그린",1],
    ["it 와일 캐시미어 울 후드 집업 홀가먼트 니트 가디건","연베이지",1],
    ["it 캐시미어 울 루즈핏 니트베스트] 라우드","그레이",1],
    ["it 캐시미어 울 니트베스트 홀가먼트 루즈 숏","브라운",1],
    ["[캐시미어 울 루즈핏 롱 니트가디건] 오버핏 홀가먼트","그레이",1],
    ["it 울100 오픈 숏 니트가디건] 홀가먼트 헵번","블랙",1],
    ["it 니트가디건 홀가먼트 베이직 리클 캐시미어 울 루즈 브이넥","카카오",1],
    ["it 캐시 엣지 라운드 긴팔","모카",1],
    ["it 캐시미어 울 보트넥니트] 홀가먼트 레이","크림",1],
    ["it 캐시미어 울 후드니트] 홀가먼트 세컨드","연베이지",1],
    ["it 후드니트 홀가먼트 포켓 캐시미어 울","아이보리",1],
    ["it 스프링 캐시미어 울 후드 홀가먼트니트","핑크",1],
    ["it 터틀넥니트 홀가먼트 헤비","아이보리",1],
    ["it 터틀넥니트 홀가먼트 헤비","브라운",1],
    ["it 홀가먼트 라쿤 울 루즈 반목 7부 니트","모카",1],
    ["it 이태리 울100 반폴 A라인 홀가먼트니트 반목","연보라",1],
    ["it 캐시미어 울 반목니트] 로우 홀가먼트 긴팔","베이지",1],
    ["it 캐시미어 울 반목니트 홀가먼트 로우","네이비",1],
    ["it 니트스커트 홀가먼트 프리폴 캐시미어 울","연베이지",1],
    ["it 캐시미어 울 H라인 니트스커트] 헵번","베이지",1],
    ["it 캐시미어 울 니트스커트] 홀가먼트 모르크","연베이지",1],
    ["it 베이직 캐시미어 울 홀가먼트 니트 롱 스커트","모카",1],
    ["it 펜슬 캐시미어 울 홀가먼트 니트 롱 스커트","라이트그레이",1],
    ["it 니트스커트 홀가먼트 모오노 펜슬","네이비",1],
    ["it 홀가먼트 주름니트스커트 미몽 캐시미어 울","블랙",1],
    ["it 니트스커트 홀가먼트 니이코 울 100 A라인","베이지",1],
    ["it 캐시미어 울 반목 롱 니트원피스] 치노 홀가먼트","차콜",1],
    ["it 캐시미어 울 반목 롱 니트원피스] 치노 홀가먼트","블랙",1],
    ["it 니트원피스 홀가먼트 버튼 캐시미어 울 라운드 반팔","연베이지",1],
    ["it 티에 울 홀가먼트 니트 원피스","베이지",1],
    ["it 로이 캐시미어 울 라운드 홀가먼트 롱 니트원피스","그레이",1],
    ["it 캐시미어 울 니트팬츠] 세컨드","연베이지",1],
    ["it 캐시미어 울 니트팬츠] 세컨드","폴그레이",1],
    ["it 캐시미어 울 니트팬츠] 세컨드","아이보리",2],
    ["it 간절기 팬츠","연핑크",1],
    ["it 캐시 배색 조거 팬츠 로로","베이지(린넨)",1],
    ["lc 밴딩스커트 포켓 면 썸머","그린",1],
    ["ls 토미 캐시미어 울 홀가먼트 니트 벙어리 장갑","베이지",1],
    ["ls 봄 여름 린넨 케이프 니트 린다 루즈 보트넥","연카키",1],
    ["ls 린넨 케이프 니트 린다 루즈 보트넥","블랙",1],
    ["ls 썸머 코튼 브이넥니트 이중","모카",1],
    ["ls 하찌 시스루니트 긴팔 라운드 여름","브라운",1],
    ["ls 린넨 라운드니트 오프 긴팔","베이지",1],
    ["ls 린넨 코튼 골지 롱 니트가디건 뮤즈","아이보리",1],
    ["ls 니트가디건 홀가먼트 프라하 라쿤 울 원버튼","블랙",1],
    ["ls 니트베스트 홀가먼트 소피아 폭스 울 라운드","소라",1],
    ["ls 폭스 울 니트베스트 소피아 홀가먼트 라운드","핑크",1],
    ["ls 캐시미어 울 니트케이프] 라울 판쵸","베이지",1],
    ["ls 버튼 숄","베이지",1],
    ["ls 캐시미어 울 니트 숄 머플러","차콜",1],
    ["ls 울100 라운드니트 홀가먼트 폴","버건디",1],
    ["ls 폭스 울 라운드니트] 아론 래글런 긴팔","핑크",1],
    ["ls 폭스 울 라운드니트] 아론 래글런 긴팔","소라",1],
    ["ls 폭스 울 라운드니트 아론 래글런 긴팔","모카진밤",1],
    ["ls 폭스 울 라운드니트 아론 래글런 긴팔","연브라운",1],
    ["ls 커플 라쿤 울 라운드니트","아이보리M",1],
    ["ls 폭스 울 반목 반팔니트] 슈가","카키",1],
    ["ls 반팔니트 홀가먼트 마아란 린넨 라운드","핑크",1],
    ["ls 케이블 라운드니트 오베 수피마코튼 반팔 숏","브라운",1],
    ["ls [캐시미어 울 반목니트] 레떼 긴팔","아이보리",1],
    ["ls 라쿤 울 후드니트] 홀가먼트 마리","차콜",1],
    ["ls [울 후드니트] 아야 홀가먼트","아이보리",1],
    ["ls 폭스 울 터틀넥니트] 퍼니 홀가먼트","그린",1],
    ["ls 터틀넥 반팔니트 홀가먼트 드마레 폭스 울 폴라 크롭","네이비",1],
    ["ls 홀가먼트 폭스 30 메리노울 30 목폴라","청록",1],
    ["ls 터틀넥니트 홀가먼트 로맨 폭스 울","연그레이",1],
    ["ls 라쿤 울 터틀넥니트] 리프 이너 폴라티","오렌지",1],
    ["ls 일리 터틀넥","블랙",1],
    ["ls 일리 터틀넥","카멜",1],
    ["ls 일리 터틀넥","청",1],
    ["ls 라운드니트 유커 꽈배기 캐시미어 울 긴팔","네이비",1],
    ["ls 봄 케이블 라운드니트 라알프 면 긴팔","베이비블루 S",1],
    ["ls 캐시미어 울 라운드니트 홀가먼트 큐비","블루",1],
    ["ls 캐시미어 울 골지 브이넥니트 홀가먼트","아이보리",1],
    ["ls [라쿤 울 글리터 라운드니트] 투어 홀가먼트 양두","글리터카키",1],
    ["ls 울 루즈핏 반팔 브이넥니트] 메리트 홀가먼트 양두","브라운",1],
    ["ls 브이넥니트 홀가먼트 휘시 긴팔","베이지",1],
    ["ls 라쿤 울 브이넥니트 홀가먼트 루메","핑크",1],
    ["ls [울 루즈핏 반팔 브이넥니트] 메리트 홀가먼트 양두","베이지",1],
    ["ls 울 루즈 반팔 브이넥니트 홀가먼트 호앤","베이지",1],
    ["ls 캐시미어 울 스트라이프 카라니트 글로리","아이보리",1],
    ["ls 가을 보들 니트스커트 인나 밴딩","브라운",1],
    ["ls 라쿤 울 미디 니트스커트] 카아","아이보리",1],
    ["ls 크림 캐시미어 울 니트 스커트","베이지",1],
    ["ls [캐시미어 울 미디 니트스커트] 크림 밴딩","카키",2],
    ["ls 캐시미어 울 니트팬츠] 홀가먼트 릴라","아이보리",1],
    ["ls 라쿤 울 조거 니트팬츠] 홀가먼트 셀리","베이지",1],
    ["ls 보들 밴딩 니트팬츠 홀가먼트 모나코","핑크",1],
    ["ls 후드 니트원피스 홀가먼트 스웨데 라쿤 울 루즈핏","아이보리",1],
    ["ls 캐시미어 울 루즈핏 롱 니트원피스] 지나 홀가먼트 말림","차콜",1],
    ["ls 캐시미어 울 니트원피스] 홀가먼트 지나 말림 롱","아이보리",1],
    ["ls 캐시미어 울 루즈핏 롱 니트원피스] 지나 홀가먼트 말림","베이지",1],
    ["ls [캐시미어 울 루즈핏 니트원피스] 퓨어 브이 롱","브라운",1],
    ["mo 캐시미어 울 브이넥니트] 베이직","그레이",1],
    ["mu 봄 여름 스트라이프 반팔 루즈 라운드니트 이즈","베이지",1],
    ["mu 봄 여름 스트라이프 반팔 루즈 라운드니트 이즈","블랙",1],
    ["mu 썸머 루즈핏 라운드 반팔 니트원피스 맥시 롱","핑크",1],
    ["mu 쓰리피스세트 니트 민소매 롱가디건 팬츠","블랙",1],
    ["mu 카라 캐시미어 울 집업 니트 가디건","핑크",1],
    ["mu 브이넥니트 코크 울","레드",1],
    ["mu 브이넥니트 홀가먼트 루즈 캐시미어 울 반팔","브라운",1],
    ["mu 캐시 캐시미어 울 루즈핏 브이넥 반팔 홀가먼트니트","핑크",1],
    ["mu 라쿤 울 브이넥 홀가먼트 니트","딥핑크",1],
    ["mu 라쿤 울 브이넥 홀가먼트 니트 롱 원피스","그레이",1],
    ["mu 이즈 울 브이넥 홀가 니트원피스","블랙",1],
    ["mu 세일러 집업 울 카라 골지 롱 니트 원피스","베이지",1],
    ["mu 베리 캐시 스커트","블랙",1],
    ["mu 린넨 니트스커트 루즈 부클 밴딩 롱","아이보리",1],
    ["mu 린넨 니트스커트 루즈 부클 밴딩 롱","베이지",1],
    ["mu 롱슬립","화이트",4],
    ["mv [캐시 루즈핏 7부 스퀘어넥 롱 니트원피스] 홀가먼트 블랑","그레이",1],
    ["mv 캐시미어 울 반목니트] 루체 홀가먼트","자주",1],
    ["mv 루즈핏 터틀넥니트 로제 야크 울","아이보리",1],
    ["mv 바넬 말림 터틀넥","브라운",1],
    ["mv 바넬 말림 터틀넥","라벤더",1],
    ["mv 바넬 말림 터틀넥","아이보리",1],
    ["mv 바넬 말림 터틀넥","아이보리",1],
    ["mv 반팔니트 샤인 카라 스카시","옐로우",1],
    ["mv 긴팔 레이어드 딥 브이넥니트 루이","네이비",1],
    ["mv [캐시미어 울 루즈핏 크로셰 라운드니트] 홀가먼트 긴팔 크롭","브라운",1],
    ["mv 봄 루즈핏 입술넥 라운드니트 홀가먼트 지제 8부","옐로우",1],
    ["mv 울100 데일리 라운드","아이보리",1],
    ["mv 레이온 라운드 캡소매니트 오드","베이지",1],
    ["mv [핫딜/여자 7부 라운드니트] 홀가먼트 비비","네이비",1],
    ["mv 민소매니트 토오리 RN 브이넥 스퀘어넥 나시","카키",1],
    ["mv 토리 나시","아이보리",1],
    ["mv 토리 나시","베이지",1],
    ["mv 민소매니트 토오리 RN 브이넥 스퀘어넥 나시","블랙",1],
    ["mv 린넨 루즈핏 반팔 후드니트 허그(허밍후드)","네이비",1],
    ["mv 썸머 린넨 코튼 루즈핏 브이넥 긴팔 니트가디건 오르","차콜",1],
    ["mv 봄 하이넥 카라 니트가디건 페일 루즈핏","크림",1],
    ["mv 홀가 카밍 후드 가디건","카멜",1],
    ["mv 여자 린넨 코튼 후드 집업 니트가디건] 루이스","베이지",1],
    ["mv 캐시미어 울 후드 포켓 집업 니트베스트","그레이",1],
    ["mv 여자 민소매 니트베스트] 뮤에르","블랙",1],
    ["mv 울100 데일리 팬츠","차콜",1],
    ["mv 울100 데일리 팬츠","아이보리",2],
    ["ov 홀가먼트 로우 라운드 훌 나시","블랙",1],
    ["ov [여자 여름 린넨 민소매니트] 홀가먼트 로우 라운드 훌 나시","먹색",1],
    ["ov [울100 스퀘어넥 민소매니트] 홀가먼트 더캐","베이지",1],
    ["ov [여름 글리터 박시 브이넥 반팔니트] 홀가먼트","글리터샴페인",1],
    ["ov 린넨 루즈핏 반팔 라운드니트 홀가먼트","린넨 블루그레이",1],
    ["ov 린넨 루즈핏 반팔 라운드니트 홀가먼트","린넨 린넨베이지",1],
    ["ov 린넨 루즈핏 반팔 라운드니트 홀가먼트","린넨 파스텔옐로우",1],
    ["ov 봄 여름 린넨 긴팔 브이넥니트 홀가먼트 리노스","크림아이보리",1],
    ["ov [여자 린넨 모크넥 반팔 플리츠니트] 홀가먼트","차콜",1],
    ["ov 린넨 루즈 라운드 반팔 니트원피스 홀가먼트","베이지",1],
    ["ov 민소매원피스 홀가먼트 하트넥 나시 니트 롱","진베이지",1],
    ["ov 캐시미어 울 니트원피스] 홀가먼트 루나 나그랑 라운드 롱","회아이보리",1],
    ["ov 라쿤 브이넥 박시 니트 원피스","오트아이보리",1],
    ["ov [울100 스퀘어넥 민소매니트] 홀가먼트 더캐","베이지",1],
    ["ov 봄 울100 민소매니트 홀가먼트 더캐 스퀘어넥","베이지",1],
    ["ov 울100 라운드니트] 거셋세모 홀가먼트","마블핑크",1],
    ["ov 울100 라운드니트 홀가먼트 터크","멜란그레이",1],
    ["ov 캐시미어 울 하이넥니트] 세라","회아이보리",2],
    ["ov 캐시미어 울 하이넥니트] 세라","블랙",2],
    ["ov 캐시미어 울 하이넥니트] 세라","오트베이지",1],
    ["ov 라쿤 울 하이넥 반목니트 말림 홀가먼트 래글런","오트연베이지",1],
    ["ov 울100 하이 터틀넥니트] 쿠지 홀가먼트","멜란그레이",1],
    ["ov 라쿤 울 하이넥 터틀넥니트] 루즈 홀가먼트","오트연베이지",1],
    ["ov 수피마코튼 골지 반목 반팔니트 홀가먼트 타임므","퓨어아이보리",1],
    ["ov 캐시미어 울 박시 하이넥니트] 윈터 미네르바","크림아이보리",2],
    ["ov [울 수피마코튼 H라인 니트스커트] 홀가먼트 이지핏","블랙",1],
    ["ov 캐시미어 울 하이넥 니트베스트] 루나 하찌","회아이보리",1],
    ["ov 울100 터틀넥 니트 케이프 넥워머","블랙",1],
    ["ov 캐시미어 울 와이드 니트팬츠] 윈터 미네르바","딥차콜",1],
    ["ps 린넨 면 라운드니트 홀가먼트 루라 긴팔 숏","아이보리",1],
    ["ps 린넨 라운드니트 홀가먼트 퍼프 7부","오트밀",1],
    ["ps 린넨 브이넥니트 홀가먼트 로우 7부","네이비",1],
    ["ps 린넨 면 루즈 반팔 라운드니트 홀가먼트 막시 숏","오트밀",1],
    ["ps 린넨 코튼 라운드 반팔니트 홀가먼트 파리","모카브라운",1],
    ["ps 린넨 코튼 카라 반팔 홀가먼트 디어","오트밀",1],
    ["ps 한지 코튼 카라 반팔니트 홀가먼트 루루","브라운",1],
    ["ps 캐시미어 울 크롭 라운드니트] 홀가먼트","카멜",1],
    ["ps 카라니트 캐시미어 울 홀가먼트","그레이",1],
    ["ps 캐시미어 울 카라니트] 홀가먼트 타아임 긴팔","오트밀",1],
    ["ps 캐시미어 울 터틀넥니트] 사면 홀가먼트","블루",1],
    ["ps [캐시미어 울 터틀넥니트] 바네 홀가먼트","D블루",1],
    ["ps 캐시미어 울 터틀넥니트 홀가먼트 바네","오트밀",1],
    ["ps 라쿤 울 브이넥 니트가디건 바네","딥그린",1],
    ["ps 캐시미어 니트베스트 홀가먼트 트임 울 라운드 루즈","그레이멜",1],
    ["ps 캐시미어 니트베스트 홀가먼트 트임 울 라운드 루즈","블랙",1],
    ["ps 심플 라쿤 울 브이넥 니트 베스트","레드",1],
    ["ps 니트베스트 홀가먼트 베이직 캐시미어 울 브이넥 조끼","베이지",1],
    ["ps 라쿤 울 반목 니트베스트] 오픈","레드",1],
    ["ps 캐시미어 울 루즈 브이넥 니트베스트] 홀가먼트","와인",1],
    ["ps 캐시미어 울 루즈 브이넥 니트베스트 홀가먼트","카키",1],
    ["ps 캐시미어 울 후드 니트베스트","카키",1],
    ["ps 캐시미어 울 후드 집업 니트베스트","올리브카키",2],
    ["ps 캐시미어 울 후드 집업 니트가디건","오트밀",1],
    ["ru 배색 테일러 카라 민소매 니트","아이보리",1],
    ["ru 배색 테일러 카라 민소매 니트","블랙",1],
    ["ru 캐시미어 울 라운드니트] 베이직 긴팔","베이지",1],
    ["ru 캐시미어 울 긴팔 라운드니트] 나그랑 홀가먼트","연그레이",1],
    ["ru 울100 라운드니트 베이직 홀가먼트","브라운",1],
    ["ru 울100 터틀넥니트 홀가먼트 폴라 풀오버","브라운",1],
    ["ru 박시 후드니트 홀가먼트 울 풀오버","그레이멜",1],
    ["ru 봄 니트스커트 홀가먼트 셔링 캐시미어 울 롱 플레어","블랙",1],
    ["ru 린넨 스커트 홀가먼트 플레어","블랙",1],
    ["ru 터틀넥 라쿤 캐시미어 울 홀가먼트 니트 롱 원피스","카멜",1],
    ["ru 라쿤 울 라운드 롱 니트원피스 홀가먼트 박시","블랙",1],
    ["ru 캐시미어100 볼레로 니트가디건","핑크",1],
    ["ru 홀가먼트 박시 라쿤 30 울 30 라운드 니트 가디건","연카키",1],
    ["ru 라쿤 울 숄 롱 니트가디건 코트","린넨베이지",1],
    ["ru 스트링 조거 니트팬츠 홀가먼트 울","그레이멜",1],
    ["sm 봄 여름 니트원피스 홀가먼트 비비안 레이온 라운드 5부","오트밀",1],
    ["sm 반팔니트 엘프 레이온 라운드 소매 보석","그린",1],
    ["sm 캡소매니트 밸리 레이온 라운드 민소매 나시","블루",1],
    ["sm 린넨 카라 반팔니트 로체 보석 단추 썸머","그린",1],
    ["sm 로렌 린넨 브이넥 반팔 홀가먼트 니트","민트",1],
    ["sm 라운드니트 홀가먼트 오브 RN 반팔 5부 간절기","카키",1],
    ["ve 울100 골지 니트스커트] 홀가먼트 H라인","핑크",1],
    ["ve 울100 골지 니트스커트] 홀가먼트 H라인","아이보리",1],
    ["vi 반팔니트 꽈배기 면100 라운드 크롭","핑크",1],
    ["vi 반팔블라우스 레이스 핀턱 면","블랙",1],
    ["[타이즈] 블랙","블랙",6],
    ["[타이즈] 브라운","브라운",11],
    ["[타이즈] 베이지","베이지",16],
    ["[타이즈] 연그레이","연그레이",4],
    ["[타이즈] 차콜","차콜",10],
    ["[타이즈] 크림","크림",1],
  ];

  const values = [];
  const params = [];
  rawData.forEach((row) => {
    values.push('(?, ?, ?, ?)');
    params.push(row[0], row[1], row[2], extractBrand(row[0]));
  });

  await query(
    `INSERT INTO inventory (name, color, qty, brand) VALUES ${values.join(', ')}`,
    params
  );
}

module.exports = { getPool, initDb, query };
