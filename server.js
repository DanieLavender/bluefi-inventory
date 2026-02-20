require('dotenv').config();
const express = require('express');
const path = require('path');
const { getPool, initDb, query } = require('./database');
const { scheduler } = require('./sync-scheduler');
const { NaverCommerceClient } = require('./smartstore');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// --- API Routes ---

// GET /api/inventory - 전체 재고 조회 (검색, 필터, 정렬, 페이지네이션)
app.get('/api/inventory', async (req, res) => {
  try {
    const { search, brand, sort, page, limit } = req.query;
    const conditions = [];
    const params = [];

    if (search) {
      conditions.push('(name LIKE ? OR color LIKE ?)');
      params.push(`%${search}%`, `%${search}%`);
    }
    if (brand) {
      conditions.push('brand = ?');
      params.push(brand);
    }

    const where = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';

    // Count
    const countRows = await query(`SELECT COUNT(*) as total FROM inventory ${where}`, params);
    const total = countRows[0].total;

    // Sort
    let orderBy = 'ORDER BY id ASC';
    if (sort === 'name-asc') orderBy = 'ORDER BY name ASC';
    else if (sort === 'name-desc') orderBy = 'ORDER BY name DESC';
    else if (sort === 'qty-asc') orderBy = 'ORDER BY qty ASC';
    else if (sort === 'qty-desc') orderBy = 'ORDER BY qty DESC';
    else if (sort === 'color-asc') orderBy = 'ORDER BY color ASC';
    else if (sort === 'updated-desc') orderBy = 'ORDER BY updated_at DESC';

    // Pagination
    const pageNum = parseInt(page) || 1;
    const pageSize = parseInt(limit) || 30;
    const offset = (pageNum - 1) * pageSize;

    const rows = await query(
      `SELECT * FROM inventory ${where} ${orderBy} LIMIT ? OFFSET ?`,
      [...params, pageSize, offset]
    );

    res.json({
      items: rows,
      total,
      page: pageNum,
      totalPages: Math.ceil(total / pageSize)
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/stats - 통계
app.get('/api/stats', async (req, res) => {
  try {
    const totalItems = (await query('SELECT COUNT(*) as cnt FROM inventory'))[0].cnt;
    const totalQty = Number((await query('SELECT COALESCE(SUM(qty), 0) as s FROM inventory'))[0].s);
    const brands = (await query("SELECT COUNT(DISTINCT brand) as cnt FROM inventory WHERE brand != ''"))[0].cnt;
    const outOfStock = (await query('SELECT COUNT(*) as cnt FROM inventory WHERE qty = 0'))[0].cnt;
    res.json({ totalItems, totalQty, brands, outOfStock });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/brands - 브랜드 목록
app.get('/api/brands', async (req, res) => {
  try {
    const rows = await query("SELECT DISTINCT brand FROM inventory WHERE brand != '' ORDER BY brand");
    res.json(rows.map(r => r.brand));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/inventory - 재고 추가
app.post('/api/inventory', async (req, res) => {
  try {
    const { name, color, qty, brand: inputBrand } = req.body;
    if (!name || !color) {
      return res.status(400).json({ error: '상품명과 컬러는 필수입니다.' });
    }
    const brand = inputBrand || extractBrand(name);
    const result = await query(
      'INSERT INTO inventory (name, color, qty, brand) VALUES (?, ?, ?, ?)',
      [name.trim(), color.trim(), Math.max(0, parseInt(qty) || 0), brand]
    );
    const rows = await query('SELECT * FROM inventory WHERE id = ?', [result.insertId]);
    res.status(201).json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/inventory/:id - 수량 수정
app.put('/api/inventory/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { qty } = req.body;
    if (qty === undefined || qty === null) {
      return res.status(400).json({ error: '수량을 입력해주세요.' });
    }
    const result = await query(
      'UPDATE inventory SET qty = ?, updated_at = NOW() WHERE id = ?',
      [Math.max(0, parseInt(qty) || 0), id]
    );
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: '항목을 찾을 수 없습니다.' });
    }
    const rows = await query('SELECT * FROM inventory WHERE id = ?', [id]);
    res.json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/inventory/:id - 단건 삭제
app.delete('/api/inventory/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await query('DELETE FROM inventory WHERE id = ?', [id]);
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: '항목을 찾을 수 없습니다.' });
    }
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/inventory/delete-bulk - 일괄 삭제
app.post('/api/inventory/delete-bulk', async (req, res) => {
  try {
    const { ids } = req.body;
    if (!ids || !Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: '삭제할 항목을 선택해주세요.' });
    }
    const placeholders = ids.map(() => '?').join(',');
    const result = await query(`DELETE FROM inventory WHERE id IN (${placeholders})`, ids);
    res.json({ success: true, deleted: result.affectedRows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// --- Sync API Routes ---

// POST /api/sync/run - 수동 즉시 동기화
app.post('/api/sync/run', async (req, res) => {
  try {
    await initSyncClients();
    const { resetHours } = req.body || {};
    if (resetHours && resetHours > 0) {
      const resetTime = new Date(Date.now() - resetHours * 60 * 60 * 1000).toISOString();
      await scheduler.setConfig('last_sync_time', resetTime);
      console.log(`[Sync] last_sync_time 리셋: ${resetTime} (${resetHours}시간 전)`);
    }
    const result = await scheduler.runSync();
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/sync/debug - 네이버 API 원본 응답 확인 (디버그)
app.get('/api/sync/debug', async (req, res) => {
  try {
    await initSyncClients();
    const now = new Date();
    const from = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const results = {};
    const typesToCheck = ['CLAIM_REQUESTED', 'COLLECT_DONE', 'CLAIM_COMPLETED'];

    for (const changeType of typesToCheck) {
      try {
        const params = new URLSearchParams({
          lastChangedFrom: from.toISOString(),
          lastChangedTo: now.toISOString(),
          lastChangedType: changeType,
        });
        const data = await scheduler.storeA.apiCall(
          'GET',
          `/v1/pay-order/seller/product-orders/last-changed-statuses?${params}`
        );
        results[changeType] = data;
      } catch (e) {
        results[changeType] = { error: e.message };
      }
    }

    res.json({
      queryRange: { from: from.toISOString(), to: now.toISOString() },
      results
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/sync/debug-raw - 주문 상세 원본 응답
app.get('/api/sync/debug-raw', async (req, res) => {
  try {
    await initSyncClients();
    const now = new Date();
    const from = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const params = new URLSearchParams({
      lastChangedFrom: from.toISOString(),
      lastChangedTo: now.toISOString(),
      lastChangedType: 'CLAIM_COMPLETED',
    });
    const statusData = await scheduler.storeA.apiCall('GET',
      `/v1/pay-order/seller/product-orders/last-changed-statuses?${params}`);
    const returnOrders = (statusData?.data?.lastChangeStatuses || [])
      .filter(s => s.claimType === 'RETURN' && s.claimStatus === 'RETURN_DONE');
    if (returnOrders.length === 0) return res.json({ message: '반품완료 건 없음' });
    const orderIds = returnOrders.map(s => s.productOrderId);
    const details = await scheduler.storeA.apiCall('POST',
      '/v1/pay-order/seller/product-orders/query', { productOrderIds: orderIds });
    res.json(details);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/sync/debug-product - A 스토어 채널상품 상세 원본 응답
app.get('/api/sync/debug-product', async (req, res) => {
  try {
    await initSyncClients();
    const productId = req.query.id;
    if (!productId) return res.status(400).json({ error: 'id 파라미터 필요' });
    const product = await scheduler.storeA.getChannelProduct(productId);
    res.json(product);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/sync/debug-addresses - B 스토어 주소 조회 (여러 경로 시도)
app.get('/api/sync/debug-addresses', async (req, res) => {
  try { await initSyncClients(); } catch(e) { return res.status(500).json({ error: e.message }); }
  const store = req.query.store === 'A' ? scheduler.storeA : scheduler.storeB;
  const paths = [
    '/v1/seller/address-books',
    '/v2/seller/address-books',
    '/v1/seller/delivery-addresses',
    '/v1/seller/address-books/all',
    '/v1/seller/info',
  ];
  const results = {};
  for (const p of paths) {
    try {
      results[p] = await store.apiCall('GET', p);
    } catch (e) {
      results[p] = { error: e.message.slice(0, 200) };
    }
  }
  res.json(results);
});

// GET /api/sync/debug-detail - 반품 건 상세 + B 스토어 검색 결과
app.get('/api/sync/debug-detail', async (req, res) => {
  try {
    await initSyncClients();
    const now = new Date();
    const from = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const params = new URLSearchParams({
      lastChangedFrom: from.toISOString(),
      lastChangedTo: now.toISOString(),
      lastChangedType: 'CLAIM_COMPLETED',
    });
    const statusData = await scheduler.storeA.apiCall('GET',
      `/v1/pay-order/seller/product-orders/last-changed-statuses?${params}`);

    const returnOrders = (statusData?.data?.lastChangeStatuses || [])
      .filter(s => s.claimType === 'RETURN' && s.claimStatus === 'RETURN_DONE');

    if (returnOrders.length === 0) return res.json({ message: '반품완료 건 없음' });

    const orderIds = returnOrders.map(s => s.productOrderId);
    const details = await scheduler.storeA.apiCall('POST',
      '/v1/pay-order/seller/product-orders/query', { productOrderIds: orderIds });

    const results = [];
    for (const detail of (details?.data || [])) {
      const po = detail.productOrder || detail;
      const productName = po.productName || '';
      const keyword = productName.replace(/^\[?[a-zA-Z]{2}\]?\s*/, '').replace(/\[.*?\]/g, '').trim().slice(0, 20);
      let searchResults = [];
      try {
        searchResults = await scheduler.storeB.searchProducts(keyword);
      } catch (e) {
        searchResults = [{ error: e.message }];
      }
      results.push({
        productOrderId: po.productOrderId || detail.productOrderId,
        productName,
        optionName: po.optionName || null,
        quantity: po.quantity || 1,
        channelProductNo: po.channelProductNo || po.productId || po.originalProductId || null,
        searchKeyword: keyword,
        storeBSearchResults: searchResults?.slice(0, 3) || [],
      });
    }
    res.json(results);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/sync/status - 동기화 상태
app.get('/api/sync/status', async (req, res) => {
  try {
    res.json(await scheduler.getStatus());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/sync/start - 자동 스케줄러 시작
app.post('/api/sync/start', async (req, res) => {
  try {
    await initSyncClients();
    const interval = parseInt(req.body.intervalMinutes) || 5;
    await scheduler.start(interval);
    res.json({ success: true, intervalMinutes: interval });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/sync/stop - 자동 스케줄러 중지
app.post('/api/sync/stop', async (req, res) => {
  try {
    await scheduler.stop();
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/sync/logs - 동기화 로그 (페이지네이션, 필터)
app.get('/api/sync/logs', async (req, res) => {
  try {
    const { type, status, page, limit } = req.query;
    const conditions = [];
    const params = [];

    if (type) {
      conditions.push('type = ?');
      params.push(type);
    }
    if (status) {
      conditions.push('status = ?');
      params.push(status);
    }

    const where = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';

    const countRows = await query(`SELECT COUNT(*) as total FROM sync_log ${where}`, params);
    const total = countRows[0].total;

    const pageNum = parseInt(page) || 1;
    const pageSize = parseInt(limit) || 30;
    const offset = (pageNum - 1) * pageSize;

    const rows = await query(
      `SELECT * FROM sync_log ${where} ORDER BY created_at DESC LIMIT ? OFFSET ?`,
      [...params, pageSize, offset]
    );

    // Summary stats
    const totalRuns = (await query("SELECT COUNT(DISTINCT run_id) as cnt FROM sync_log"))[0].cnt;
    const totalDetected = (await query("SELECT COALESCE(SUM(qty), 0) as s FROM sync_log WHERE type = 'return_detect'"))[0].s;
    const totalErrors = (await query("SELECT COUNT(*) as cnt FROM sync_log WHERE status = 'fail'"))[0].cnt;

    res.json({
      items: rows,
      total,
      page: pageNum,
      totalPages: Math.ceil(total / pageSize),
      summary: { totalRuns, totalDetected, totalErrors }
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/sync/config - 설정 조회
app.get('/api/sync/config', async (req, res) => {
  try {
    const rows = await query('SELECT `key`, value FROM sync_config');
    const config = {};
    for (const row of rows) {
      config[row.key] = row.value;
    }
    const aId = process.env.STORE_A_CLIENT_ID || config.store_a_client_id || '';
    const aSecret = process.env.STORE_A_CLIENT_SECRET || config.store_a_client_secret || '';
    const bId = process.env.STORE_B_CLIENT_ID || config.store_b_client_id || '';
    const bSecret = process.env.STORE_B_CLIENT_SECRET || config.store_b_client_secret || '';
    config.store_a_client_id = aId ? maskSecret(aId) : '';
    config.store_a_client_secret = aSecret ? '****' : '';
    config.store_b_client_id = bId ? maskSecret(bId) : '';
    config.store_b_client_secret = bSecret ? '****' : '';
    res.json(config);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/sync/config - 설정 수정
app.put('/api/sync/config', async (req, res) => {
  try {
    const updates = req.body;
    const conn = await getPool().getConnection();
    try {
      await conn.beginTransaction();
      for (const [k, v] of Object.entries(updates)) {
        await conn.query(
          'INSERT INTO sync_config (`key`, value, updated_at) VALUES (?, ?, NOW()) ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = NOW()',
          [k, v]
        );
      }
      await conn.commit();
    } catch (e) {
      await conn.rollback();
      throw e;
    } finally {
      conn.release();
    }
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/sync/mappings - 상품 매핑 목록
app.get('/api/sync/mappings', async (req, res) => {
  try {
    const { status, page, limit } = req.query;
    const conditions = [];
    const params = [];

    if (status) {
      conditions.push('match_status = ?');
      params.push(status);
    }

    const where = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';
    const countRows = await query(`SELECT COUNT(*) as total FROM product_mapping ${where}`, params);
    const total = countRows[0].total;

    const pageNum = parseInt(page) || 1;
    const pageSize = parseInt(limit) || 30;
    const offset = (pageNum - 1) * pageSize;

    const rows = await query(
      `SELECT * FROM product_mapping ${where} ORDER BY updated_at DESC LIMIT ? OFFSET ?`,
      [...params, pageSize, offset]
    );

    res.json({ items: rows, total, page: pageNum, totalPages: Math.ceil(total / pageSize) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/sync/mappings/:id - 수동 매핑 설정
app.put('/api/sync/mappings/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { store_b_channel_product_no, store_b_product_name, store_b_option_name } = req.body;

    const result = await query(`
      UPDATE product_mapping SET
        store_b_channel_product_no = ?,
        store_b_product_name = ?,
        store_b_option_name = ?,
        match_status = 'manual',
        updated_at = NOW()
      WHERE id = ?
    `, [store_b_channel_product_no, store_b_product_name, store_b_option_name || null, id]);

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: '매핑을 찾을 수 없습니다.' });
    }
    const rows = await query('SELECT * FROM product_mapping WHERE id = ?', [id]);
    res.json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/sync/test-connection - 연결 테스트
app.post('/api/sync/test-connection', async (req, res) => {
  const { store, clientId, clientSecret } = req.body;
  if (!clientId || !clientSecret) {
    return res.status(400).json({ error: 'Client ID와 Secret을 입력해주세요.' });
  }
  const client = new NaverCommerceClient(clientId, clientSecret, store || 'test');
  const result = await client.testConnection();
  res.json(result);
});

// POST /api/sync/save-keys - 스토어 API 키 저장
app.post('/api/sync/save-keys', async (req, res) => {
  try {
    const { store_a_client_id, store_a_client_secret, store_b_client_id, store_b_client_secret } = req.body;
    const upsertSql = 'INSERT INTO sync_config (`key`, value, updated_at) VALUES (?, ?, NOW()) ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = NOW()';
    if (store_a_client_id) await query(upsertSql, ['store_a_client_id', store_a_client_id]);
    if (store_a_client_secret) await query(upsertSql, ['store_a_client_secret', store_a_client_secret]);
    if (store_b_client_id) await query(upsertSql, ['store_b_client_id', store_b_client_id]);
    if (store_b_client_secret) await query(upsertSql, ['store_b_client_secret', store_b_client_secret]);
    scheduler.storeA = null;
    scheduler.storeB = null;
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// === Helpers ===

function extractBrand(name) {
  if (!name) return '';
  const trimmed = name.trim();
  const match = trimmed.match(/^([a-zA-Z]{2})\s/);
  if (match) return match[1].toLowerCase();
  return '';
}

function maskSecret(str) {
  if (!str || str.length <= 4) return '****';
  return str.slice(0, 4) + '****';
}

async function initSyncClients() {
  if (scheduler.hasClients()) return;
  const getVal = async (key) => {
    const rows = await query('SELECT value FROM sync_config WHERE `key` = ?', [key]);
    return rows[0] ? rows[0].value : '';
  };
  const aId = process.env.STORE_A_CLIENT_ID || await getVal('store_a_client_id');
  const aSecret = process.env.STORE_A_CLIENT_SECRET || await getVal('store_a_client_secret');
  const bId = process.env.STORE_B_CLIENT_ID || await getVal('store_b_client_id');
  const bSecret = process.env.STORE_B_CLIENT_SECRET || await getVal('store_b_client_secret');
  if (!aId || !aSecret || !bId || !bSecret) {
    throw new Error('스토어 A/B API 키가 설정되지 않았습니다. Settings에서 입력해주세요.');
  }
  scheduler.initClients(aId, aSecret, bId, bSecret);
}

// Initialize DB and start server
(async () => {
  await initDb();

  // Auto-start scheduler if configured
  try {
    const enabled = await query("SELECT value FROM sync_config WHERE `key` = 'sync_enabled'");
    const interval = await query("SELECT value FROM sync_config WHERE `key` = 'sync_interval_minutes'");

    if (enabled[0] && enabled[0].value === 'true') {
      try {
        await initSyncClients();
        await scheduler.start(parseInt(interval[0]?.value) || 5);
      } catch (e) {
        console.log('[Sync] 자동 시작 실패 (API 키 미설정):', e.message);
      }
    }
  } catch (e) {
    console.log('[Sync] 설정 확인 오류:', e.message);
  }

  app.listen(PORT, () => {
    console.log(`블루파이 재고관리 서버 실행중: http://localhost:${PORT}`);
  });
})();
