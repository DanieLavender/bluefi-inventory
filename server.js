require('dotenv').config();
const express = require('express');
const path = require('path');
const { getPool, initDb, query } = require('./database');
const { scheduler } = require('./sync-scheduler');
const { NaverCommerceClient } = require('./smartstore');
const { CoupangClient } = require('./coupang');
const { ZigzagClient } = require('./zigzag');
const webpush = require('web-push');
const multer = require('multer');
const ftp = require('basic-ftp');
const XLSX = require('xlsx');
const os = require('os');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3001;

const bulkUpload = multer({ dest: os.tmpdir(), limits: { fileSize: 10 * 1024 * 1024, files: 20 } });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// --- Store A 상품 인덱스 (DB 기반 즉시 검색) ---
let indexingActive = false;
let indexingProgress = { current: 0, total: 0, startedAt: null };

// v2 상세 → DB 저장용 데이터 추출
function extractProductInfo(v2Detail, channelProductNo) {
  const origin = v2Detail.originProduct || {};
  const channel = v2Detail.smartstoreChannelProduct || {};

  const name = channel.channelProductName || origin.name || '';
  const imgUrl = (origin.images && origin.images.representativeImage && origin.images.representativeImage.url) || '';

  let salePrice = origin.salePrice || 0;
  const discount = origin.customerBenefit
    && origin.customerBenefit.immediateDiscountPolicy
    && origin.customerBenefit.immediateDiscountPolicy.discountMethod;
  if (discount) {
    if (discount.unitType === 'PERCENT') {
      salePrice = Math.round(origin.salePrice * (1 - discount.value / 100));
    } else {
      salePrice = origin.salePrice - (discount.value || 0);
    }
  }

  return {
    channelProductNo: String(channelProductNo || channel.channelProductNo || ''),
    originProductNo: String(origin.originProductNo || ''),
    name,
    salePrice,
    stockQuantity: origin.stockQuantity || 0,
    imageUrl: imgUrl,
    statusType: origin.statusType || channel.channelProductDisplayStatusType || '',
  };
}

// 빠른 체크: 새 상품이 있는지 v1 1페이지만 호출하여 확인
async function checkForNewProducts() {
  await initSyncClients();
  const data = await scheduler.storeA.apiCall('POST', '/v1/products/search', { page: 1, size: 1 });
  const apiTotal = (data && (data.totalElements || data.totalCount)) || 0;
  console.log(`[Index] v1 응답 키: ${data ? Object.keys(data).join(', ') : 'null'}, apiTotal: ${apiTotal}`);
  const dbRows = await query('SELECT COUNT(*) as cnt FROM store_a_products');
  const dbTotal = dbRows[0].cnt;
  return { apiTotal, dbTotal, hasNew: apiTotal > dbTotal };
}

// 백그라운드 인덱싱: 신규 상품만 증분 인덱싱
async function runProductIndexing(fullRefresh = false) {
  if (indexingActive) return;
  indexingActive = true;
  indexingProgress = { current: 0, total: 0, phase: 'collecting', startedAt: new Date().toISOString() };

  try {
    await initSyncClients();

    // Step 1: 빠른 체크 (API 1회) — 새 상품 있는지 확인
    if (!fullRefresh) {
      const check = await checkForNewProducts();
      console.log(`[Index] 빠른 체크: API ${check.apiTotal}개, DB ${check.dbTotal}개`);
      if (!check.hasNew) {
        console.log('[Index] 새 상품 없음 — 스킵');
        indexingActive = false;
        return;
      }
    }

    // Step 2: v1 리스트로 상품번호 + 기본정보 일괄 수집 (v2 개별 호출 불필요)
    console.log('[Index] 상품 리스트 수집 시작...');
    const allProducts = await scheduler.storeA.getAllProductsFromList();
    console.log(`[Index] 상품 ${allProducts.length}개 수집 완료`);

    // 이미 인덱싱된 상품 제외
    const existingRows = await query('SELECT channel_product_no FROM store_a_products');
    const existingSet = new Set(existingRows.map(r => r.channel_product_no));
    const allNos = allProducts.map(p => p.channelProductNo);
    const newProducts = allProducts.filter(p => !existingSet.has(p.channelProductNo));

    // DB에 있지만 API에 없는 상품 삭제 (삭제된 상품 정리)
    const apiSet = new Set(allNos);
    const deletedNos = existingRows.map(r => r.channel_product_no).filter(no => !apiSet.has(no));
    if (deletedNos.length > 0) {
      const ph = deletedNos.map(() => '?').join(',');
      await query(`DELETE FROM store_a_products WHERE channel_product_no IN (${ph})`, deletedNos);
      console.log(`[Index] 삭제된 상품 정리: ${deletedNos.length}건`);
    }

    console.log(`[Index] 신규 ${newProducts.length}개 (기존 ${existingSet.size}개)`);

    if (newProducts.length === 0) {
      console.log('[Index] 인덱싱할 신규 상품 없음');
      indexingActive = false;
      return;
    }

    indexingProgress.total = newProducts.length;
    indexingProgress.phase = 'indexing';

    // Step 3: 리스트에서 가져온 데이터를 일괄 DB 저장 (50건씩 배치)
    const batchSize = 50;
    for (let i = 0; i < newProducts.length; i += batchSize) {
      if (!indexingActive) break;

      const batch = newProducts.slice(i, i + batchSize);
      const values = [];
      const placeholders = [];

      for (const info of batch) {
        placeholders.push('(?, ?, ?, ?, 0, ?, ?, NOW())');
        values.push(info.channelProductNo, info.originProductNo, info.name, info.salePrice, info.statusType, info.imageUrl);
      }

      try {
        await query(
          `INSERT INTO store_a_products (channel_product_no, origin_product_no, name, sale_price, stock_quantity, status_type, image_url, indexed_at)
           VALUES ${placeholders.join(', ')}
           ON DUPLICATE KEY UPDATE name=VALUES(name), sale_price=VALUES(sale_price), status_type=VALUES(status_type), image_url=VALUES(image_url), indexed_at=NOW()`,
          values
        );
      } catch (e) {
        console.log(`[Index] 배치 오류 (${i}~${i + batch.length}):`, e.message.slice(0, 100));
      }

      indexingProgress.current = Math.min(i + batchSize, newProducts.length);
      if (indexingProgress.current % 200 === 0 || indexingProgress.current === newProducts.length) {
        console.log(`[Index] 진행: ${indexingProgress.current}/${newProducts.length}`);
      }
    }

    console.log(`[Index] 완료: ${indexingProgress.current}건 인덱싱됨`);
  } catch (e) {
    console.error('[Index] 오류:', e.message);
  } finally {
    indexingActive = false;
  }
}

// 자동 인덱싱 스케줄러 (6시간마다 새 상품 체크)
let autoIndexInterval = null;
function startAutoIndexing() {
  if (autoIndexInterval) return;
  const SIX_HOURS = 6 * 60 * 60 * 1000;
  autoIndexInterval = setInterval(() => {
    if (!indexingActive) {
      console.log('[Index] 자동 체크 실행');
      runProductIndexing().catch(e => console.log('[Index] 자동 체크 오류:', e.message));
    }
  }, SIX_HOURS);
  console.log('[Index] 자동 인덱싱 활성화 (6시간 간격)');
}

// --- 상품 API (products 통합 테이블) ---

const channelCols = { naver_a: 'naver_a_no', naver_b: 'naver_b_no', coupang: 'coupang_no', zigzag: 'zigzag_no' };
const storeToChannel = { A: 'naver_a_no', B: 'naver_b_no', C: 'coupang_no', D: 'zigzag_no' };
const channelToStore = { naver_a: 'A', naver_b: 'B', coupang: 'C', zigzag: 'D' };

// GET /api/master/products - 상품 목록
app.get('/api/master/products', async (req, res) => {
  try {
    const { search, brand, stockType, channel, sort, page = 1, limit = 30 } = req.query;
    const conditions = [];
    const params = [];

    if (search) {
      const keywords = search.trim().split(/\s+/);
      for (const kw of keywords) {
        conditions.push('(p.name LIKE ? OR p.sku LIKE ? OR EXISTS (SELECT 1 FROM variants v2 WHERE v2.product_id = p.id AND v2.color LIKE ?))');
        params.push(`%${kw}%`, `%${kw}%`, `%${kw}%`);
      }
    }
    if (brand) { conditions.push('p.brand = ?'); params.push(brand); }
    if (stockType) { conditions.push('p.stock_type = ?'); params.push(stockType); }

    if (channel === 'unlinked') {
      conditions.push('p.naver_a_no IS NULL AND p.naver_b_no IS NULL AND p.coupang_no IS NULL AND p.zigzag_no IS NULL');
    } else if (channelCols[channel]) {
      conditions.push(`p.${channelCols[channel]} IS NOT NULL`);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    const countRows = await query(`SELECT COUNT(*) as total FROM products p ${where}`, params);
    const total = countRows[0].total;

    let orderBy = 'p.id ASC';
    if (sort === 'updated') orderBy = 'p.updated_at DESC';
    if (sort === 'name') orderBy = 'p.name ASC';
    if (sort === 'sku') orderBy = 'p.sku ASC';
    if (sort === 'newest') orderBy = 'p.created_at DESC';
    if (sort === 'oldest') orderBy = 'p.created_at ASC';
    if (sort === 'qty_desc') orderBy = '(SELECT COALESCE(SUM(v.qty),0) FROM variants v WHERE v.product_id = p.id) DESC';

    const offset = (parseInt(page) - 1) * parseInt(limit);
    params.push(parseInt(limit), offset);
    const rows = await query(`SELECT * FROM products p ${where} ORDER BY ${orderBy} LIMIT ? OFFSET ?`, params);

    // variants 조회 (해당 페이지 상품들)
    let variantsMap = {};
    if (rows.length > 0) {
      const pIds = rows.map(r => r.id);
      const ph = pIds.map(() => '?').join(',');
      const vRows = await query(`SELECT * FROM variants WHERE product_id IN (${ph}) ORDER BY id`, pIds);
      for (const v of vRows) {
        if (!variantsMap[v.product_id]) variantsMap[v.product_id] = [];
        variantsMap[v.product_id].push(v);
      }
    }

    // 매출 데이터 집계
    let salesMap = {};
    if (rows.length > 0) {
      const allPairs = [];
      const pairToId = {};
      for (const r of rows) {
        if (r.naver_a_no) { allPairs.push({ s: 'A', c: r.naver_a_no, id: r.id }); }
        if (r.naver_b_no) { allPairs.push({ s: 'B', c: r.naver_b_no, id: r.id }); }
        if (r.coupang_no) { allPairs.push({ s: 'C', c: r.coupang_no, id: r.id }); }
        if (r.zigzag_no) { allPairs.push({ s: 'D', c: r.zigzag_no, id: r.id }); }
      }
      if (allPairs.length > 0) {
        const orConds = allPairs.map(() => '(store = ? AND channel_product_no = ?)');
        const orParams = [];
        for (const p of allPairs) { orParams.push(p.s, p.c); pairToId[`${p.s}_${p.c}`] = p.id; }
        const salesRows = await query(
          `SELECT store, channel_product_no, SUM(qty) as tq, SUM(total_amount) as ta, MIN(order_date) as fo
           FROM sales_orders WHERE ${orConds.join(' OR ')} GROUP BY store, channel_product_no`, orParams
        );
        for (const sr of salesRows) {
          const pid = pairToId[`${sr.store}_${sr.channel_product_no}`];
          if (!pid) continue;
          if (!salesMap[pid]) salesMap[pid] = { totalQty: 0, totalAmount: 0, firstOrder: null };
          salesMap[pid].totalQty += Number(sr.tq) || 0;
          salesMap[pid].totalAmount += Number(sr.ta) || 0;
          if (sr.fo && (!salesMap[pid].firstOrder || sr.fo < salesMap[pid].firstOrder)) salesMap[pid].firstOrder = sr.fo;
        }
      }
    }

    const items = rows.map(r => {
      const variants = variantsMap[r.id] || [];
      const totalQty = variants.reduce((sum, v) => sum + v.qty, 0);
      return {
        ...r,
        variants,
        totalQty,
        sales: salesMap[r.id] || { totalQty: 0, totalAmount: 0, firstOrder: null },
      };
    });

    res.json({ items, total, page: parseInt(page), limit: parseInt(limit) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/master/products/:id - 상품 상세
app.get('/api/master/products/:id', async (req, res) => {
  try {
    const rows = await query('SELECT * FROM products WHERE id = ?', [req.params.id]);
    if (rows.length === 0) return res.status(404).json({ error: '상품 없음' });
    const p = rows[0];

    // variants 조회
    const variants = await query('SELECT * FROM variants WHERE product_id = ? ORDER BY id', [p.id]);
    const totalQty = variants.reduce((sum, v) => sum + v.qty, 0);

    // 채널별 매출 집계
    let sales = { totalQty: 0, totalAmount: 0, firstOrder: null, byChannel: {} };
    const pairs = [
      { ch: 'naver_a', s: 'A', no: p.naver_a_no },
      { ch: 'naver_b', s: 'B', no: p.naver_b_no },
      { ch: 'coupang', s: 'C', no: p.coupang_no },
      { ch: 'zigzag', s: 'D', no: p.zigzag_no },
    ];
    for (const { ch, s, no } of pairs) {
      if (!no) continue;
      const sRows = await query(
        'SELECT SUM(qty) as tq, SUM(total_amount) as ta, MIN(order_date) as fo FROM sales_orders WHERE store = ? AND channel_product_no = ?',
        [s, no]
      );
      if (sRows[0] && sRows[0].tq) {
        const chSales = { qty: Number(sRows[0].tq), amount: Number(sRows[0].ta), firstOrder: sRows[0].fo };
        sales.totalQty += chSales.qty;
        sales.totalAmount += chSales.amount;
        if (chSales.firstOrder && (!sales.firstOrder || chSales.firstOrder < sales.firstOrder)) sales.firstOrder = chSales.firstOrder;
        sales.byChannel[ch] = chSales;
      }
    }

    res.json({ ...p, variants, totalQty, sales });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/master/products/:id - 상품 수정
app.put('/api/master/products/:id', async (req, res) => {
  try {
    const { name, brand, stock_type, sale_price } = req.body;
    const sets = [];
    const params = [];
    if (name !== undefined) { sets.push('name = ?'); params.push(name); }
    if (brand !== undefined) { sets.push('brand = ?'); params.push(brand); }
    if (stock_type !== undefined) { sets.push('stock_type = ?'); params.push(stock_type); }
    if (sale_price !== undefined) { sets.push('sale_price = ?'); params.push(sale_price); }
    if (sets.length > 0) {
      sets.push('updated_at = NOW()');
      params.push(req.params.id);
      await query(`UPDATE products SET ${sets.join(', ')} WHERE id = ?`, params);
    }
    const rows = await query('SELECT * FROM products WHERE id = ?', [req.params.id]);
    const variants = await query('SELECT * FROM variants WHERE product_id = ? ORDER BY id', [req.params.id]);
    res.json({ ...rows[0], variants, totalQty: variants.reduce((s, v) => s + v.qty, 0) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/master/products/:id/variants - 옵션 추가
app.post('/api/master/products/:id/variants', async (req, res) => {
  try {
    const { color, size, qty } = req.body;
    const result = await query(
      'INSERT INTO variants (product_id, color, size, qty) VALUES (?, ?, ?, ?)',
      [req.params.id, color || '', size || null, qty || 0]
    );
    await query('UPDATE products SET updated_at = NOW() WHERE id = ?', [req.params.id]);
    const rows = await query('SELECT * FROM variants WHERE id = ?', [result.insertId]);
    res.json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/master/variants/:variantId - 옵션 수정 (수량 변경 등)
app.put('/api/master/variants/:variantId', async (req, res) => {
  try {
    const { color, size, qty } = req.body;
    const sets = [];
    const params = [];
    if (color !== undefined) { sets.push('color = ?'); params.push(color); }
    if (size !== undefined) { sets.push('size = ?'); params.push(size); }
    if (qty !== undefined) { sets.push('qty = ?, updated_at = NOW()'); params.push(qty); }
    if (sets.length === 0) return res.status(400).json({ error: '수정할 필드 없음' });
    params.push(req.params.variantId);
    await query(`UPDATE variants SET ${sets.join(', ')} WHERE id = ?`, params);
    const rows = await query('SELECT * FROM variants WHERE id = ?', [req.params.variantId]);
    if (rows.length > 0) {
      await query('UPDATE products SET updated_at = NOW() WHERE id = ?', [rows[0].product_id]);
    }
    res.json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/master/variants/:variantId - 옵션 삭제
app.delete('/api/master/variants/:variantId', async (req, res) => {
  try {
    const rows = await query('SELECT * FROM variants WHERE id = ?', [req.params.variantId]);
    if (rows.length === 0) return res.status(404).json({ error: '옵션 없음' });
    await query('DELETE FROM variants WHERE id = ?', [req.params.variantId]);
    await query('UPDATE products SET updated_at = NOW() WHERE id = ?', [rows[0].product_id]);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/master/products/:id/link - 채널 연결
app.post('/api/master/products/:id/link', async (req, res) => {
  try {
    const { channel, channel_product_id } = req.body;
    const col = channelCols[channel];
    if (!col || !channel_product_id) return res.status(400).json({ error: 'channel, channel_product_id 필수' });
    await query(`UPDATE products SET ${col} = ?, updated_at = NOW() WHERE id = ?`, [channel_product_id, req.params.id]);
    const rows = await query('SELECT * FROM products WHERE id = ?', [req.params.id]);
    res.json({ success: true, ...rows[0] });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/master/products/:id/link/:channel - 채널 연결 해제
app.delete('/api/master/products/:id/link/:channel', async (req, res) => {
  try {
    const col = channelCols[req.params.channel];
    if (!col) return res.status(400).json({ error: '유효하지 않은 채널' });
    await query(`UPDATE products SET ${col} = NULL, updated_at = NOW() WHERE id = ?`, [req.params.id]);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/master/stats - 상품 통계
app.get('/api/master/stats', async (req, res) => {
  try {
    const [totalRow] = await query('SELECT COUNT(*) as cnt FROM products');
    const [invRow] = await query("SELECT COUNT(*) as cnt FROM products WHERE stock_type = 'inventory'");
    const [srcRow] = await query("SELECT COUNT(*) as cnt FROM products WHERE stock_type = 'sourcing'");
    const [naRow] = await query('SELECT COUNT(*) as cnt FROM products WHERE naver_a_no IS NOT NULL');
    const [nbRow] = await query('SELECT COUNT(*) as cnt FROM products WHERE naver_b_no IS NOT NULL');
    const [cpRow] = await query('SELECT COUNT(*) as cnt FROM products WHERE coupang_no IS NOT NULL');
    const [zzRow] = await query('SELECT COUNT(*) as cnt FROM products WHERE zigzag_no IS NOT NULL');
    const [unlinkedRow] = await query('SELECT COUNT(*) as cnt FROM products WHERE naver_a_no IS NULL AND naver_b_no IS NULL AND coupang_no IS NULL AND zigzag_no IS NULL');
    const [variantRow] = await query('SELECT COUNT(*) as cnt, COALESCE(SUM(qty), 0) as totalQty FROM variants');
    res.json({
      totalProducts: totalRow.cnt,
      inventoryProducts: invRow.cnt,
      sourcingProducts: srcRow.cnt,
      unlinkedProducts: unlinkedRow.cnt,
      totalVariants: variantRow.cnt,
      totalStockQty: Number(variantRow.totalQty),
      channelStats: { naver_a: naRow.cnt, naver_b: nbRow.cnt, coupang: cpRow.cnt, zigzag: zzRow.cnt },
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/master/next-sku - 다음 품번 조회
app.get('/api/master/next-sku', async (req, res) => {
  try {
    const supplier = (req.query.supplier || 'ETC').toUpperCase();
    const year = new Date().getFullYear();
    const prefix = `${year}${supplier}`;
    const rows = await query("SELECT sku FROM products WHERE sku LIKE ? ORDER BY sku DESC LIMIT 1", [`${prefix}%`]);
    let nextNum = 1;
    if (rows.length > 0) {
      const numPart = rows[0].sku.replace(prefix, '');
      nextNum = parseInt(numPart, 10) + 1;
    }
    res.json({ nextSku: `${prefix}${String(nextNum).padStart(3, '0')}` });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/master/products - 상품 추가
app.post('/api/master/products', async (req, res) => {
  try {
    const { name, brand, supplier, color, size, qty, stock_type, image_url, naver_a_no } = req.body;
    if (!name) return res.status(400).json({ error: '상품명 필수' });
    const sup = (supplier || brand || 'ETC').toUpperCase();
    const year = new Date().getFullYear();
    const prefix = `${year}${sup}`;
    const skuRows = await query("SELECT sku FROM products WHERE sku LIKE ? ORDER BY sku DESC LIMIT 1", [`${prefix}%`]);
    let nextNum = 1;
    if (skuRows.length > 0) {
      const numPart = skuRows[0].sku.replace(prefix, '');
      nextNum = parseInt(numPart, 10) + 1;
    }
    const sku = `${prefix}${String(nextNum).padStart(3, '0')}`;
    const result = await query(
      `INSERT INTO products (sku, name, brand, supplier, stock_type, image_url, naver_a_no) VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [sku, name, brand || '', sup, stock_type || 'sourcing', image_url || null, naver_a_no || null]
    );
    const productId = result.insertId;
    // 초기 옵션 생성
    if (color || size || qty) {
      await query(
        'INSERT INTO variants (product_id, color, size, qty) VALUES (?, ?, ?, ?)',
        [productId, color || '', size || null, qty || 0]
      );
    }
    const rows = await query('SELECT * FROM products WHERE id = ?', [productId]);
    const variants = await query('SELECT * FROM variants WHERE product_id = ? ORDER BY id', [productId]);
    res.json({ ...rows[0], variants, totalQty: variants.reduce((s, v) => s + v.qty, 0) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/master/import-from-store-a - A스토어 상품 일괄 등록
app.post('/api/master/import-from-store-a', async (req, res) => {
  try {
    const storeProducts = await query('SELECT * FROM store_a_products ORDER BY name');
    if (storeProducts.length === 0) {
      return res.json({ created: 0, skipped: 0, message: '인덱싱된 A스토어 상품이 없습니다' });
    }

    // 이미 등록된 naver_a_no 목록
    const linkedRows = await query("SELECT naver_a_no FROM products WHERE naver_a_no IS NOT NULL");
    const linkedSet = new Set(linkedRows.map(r => r.naver_a_no));

    const toImport = storeProducts.filter(p => !linkedSet.has(p.channel_product_no));
    if (toImport.length === 0) {
      return res.json({ created: 0, skipped: storeProducts.length, message: '모든 상품이 이미 등록되어 있습니다' });
    }

    // SKU 생성용 supplier별 최대 번호
    const year = new Date().getFullYear();
    const existingSkus = await query("SELECT sku FROM products WHERE sku LIKE ?", [`${year}%`]);
    const supplierMax = {};
    for (const row of existingSkus) {
      const match = row.sku.match(/^(\d{4})([A-Z]+)(\d+)$/);
      if (match) {
        supplierMax[match[2]] = Math.max(supplierMax[match[2]] || 0, parseInt(match[3], 10));
      }
    }

    let created = 0;
    for (const p of toImport) {
      const brand = extractBrand(p.name);
      const sup = (brand || 'ETC').toUpperCase();
      supplierMax[sup] = (supplierMax[sup] || 0) + 1;
      const sku = `${year}${sup}${String(supplierMax[sup]).padStart(3, '0')}`;
      try {
        const result = await query(
          `INSERT INTO products (sku, name, brand, supplier, sale_price, stock_type, image_url, naver_a_no) VALUES (?, ?, ?, ?, ?, 'sourcing', ?, ?)`,
          [sku, p.name, brand, sup, p.sale_price || 0, p.image_url || null, p.channel_product_no]
        );
        // 기본 variant 생성 (옵션 없이 빈 옵션 1개)
        await query('INSERT INTO variants (product_id, color, size, qty) VALUES (?, "", NULL, 0)', [result.insertId]);
        created++;
      } catch (e) {
        console.log(`[import] ${p.channel_product_no} 실패: ${e.message.slice(0, 100)}`);
      }
    }

    console.log(`[import] A스토어 → products: ${created}건 등록, ${storeProducts.length - toImport.length}건 기존`);
    res.json({ created, skipped: storeProducts.length - toImport.length, failed: toImport.length - created, total: storeProducts.length, message: `${created}건 등록 완료` });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// --- API Routes ---

// POST /api/deploy - 원격 배포 (git pull + 재���작)
app.post('/api/deploy', async (req, res) => {
  const { execSync } = require('child_process');
  try {
    const pullResult = execSync('git pull', { encoding: 'utf8', cwd: __dirname, timeout: 30000 }).trim();
    console.log(`[Deploy] git pull: ${pullResult}`);
    if (pullResult.includes('Already up to date')) {
      return res.json({ status: 'no_change', message: pullResult });
    }
    res.json({ status: 'updating', message: pullResult });
    // PM2가 자동 재시작
    setTimeout(() => process.exit(0), 500);
  } catch (e) {
    res.status(500).json({ status: 'error', message: e.message });
  }
});

// GET /api/health - 헬스체크 (서버 keep-alive용)
app.get('/api/health', async (req, res) => {
  const status = await scheduler.getStatus();
  res.json({
    status: 'ok',
    uptime: Math.floor(process.uptime()),
    syncActive: status.schedulerActive,
    lastSync: status.lastSyncTime,
  });
});

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
    let orderBy = 'ORDER BY COALESCE(updated_at, created_at) DESC';
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

// GET /api/brands - 브랜드 목록 (inventory + 커스텀 브랜드 병합)
app.get('/api/brands', async (req, res) => {
  try {
    const rows = await query("SELECT DISTINCT brand FROM inventory WHERE brand != '' ORDER BY brand");
    const invBrands = rows.map(r => r.brand);
    // 커스텀 브랜드 병합
    const configRows = await query("SELECT `value` FROM sync_config WHERE `key` = 'custom_brands' LIMIT 1");
    let customBrands = [];
    if (configRows.length > 0 && configRows[0].value) {
      try { customBrands = JSON.parse(configRows[0].value); } catch {}
    }
    const all = [...new Set([...invBrands, ...customBrands])].sort();
    res.json(all);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/brands - 브랜드 추가
app.post('/api/brands', async (req, res) => {
  try {
    const { brand } = req.body;
    if (!brand || !brand.trim()) return res.status(400).json({ error: '브랜드 코드를 입력해주세요.' });
    const code = brand.trim().toLowerCase();
    // 기존 커스텀 브랜드 조회
    const configRows = await query("SELECT `value` FROM sync_config WHERE `key` = 'custom_brands' LIMIT 1");
    let customBrands = [];
    if (configRows.length > 0 && configRows[0].value) {
      try { customBrands = JSON.parse(configRows[0].value); } catch {}
    }
    // 이미 inventory에 있는지 확인
    const existing = await query("SELECT brand FROM inventory WHERE brand = ? LIMIT 1", [code]);
    if (existing.length > 0 || customBrands.includes(code)) {
      return res.status(409).json({ error: '이미 존재하는 브랜드입니다.' });
    }
    customBrands.push(code);
    await query("INSERT INTO sync_config (`key`, `value`) VALUES ('custom_brands', ?) ON DUPLICATE KEY UPDATE `value` = ?",
      [JSON.stringify(customBrands), JSON.stringify(customBrands)]);
    res.json({ success: true, brand: code });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/brands/:code - 브랜드 삭제
app.delete('/api/brands/:code', async (req, res) => {
  try {
    const code = req.params.code.toLowerCase();
    // inventory에 해당 브랜드 상품이 있으면 삭제 불가
    const used = await query("SELECT COUNT(*) as cnt FROM inventory WHERE brand = ?", [code]);
    if (used[0].cnt > 0) {
      return res.status(400).json({ error: `해당 브랜드에 상품 ${used[0].cnt}개가 있어 삭제할 수 없습니다.` });
    }
    const configRows = await query("SELECT `value` FROM sync_config WHERE `key` = 'custom_brands' LIMIT 1");
    let customBrands = [];
    if (configRows.length > 0 && configRows[0].value) {
      try { customBrands = JSON.parse(configRows[0].value); } catch {}
    }
    customBrands = customBrands.filter(b => b !== code);
    await query("UPDATE sync_config SET `value` = ? WHERE `key` = 'custom_brands'", [JSON.stringify(customBrands)]);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/inventory - 재고 추가
app.post('/api/inventory', async (req, res) => {
  try {
    const { name, color, qty, brand: inputBrand, productOrderId, channelProductNo, size } = req.body;
    if (!name || !color) {
      return res.status(400).json({ error: '상품명과 컬러는 필수입니다.' });
    }
    // 중복 방지: productOrderId가 있으면 이미 등록된 건인지 체크 (재고반영/B스토어 복사 포함)
    if (productOrderId) {
      const orderIdList = productOrderId.includes(',') ? productOrderId.split(',') : [productOrderId];
      const placeholders = orderIdList.map(() => '?').join(',');
      const dupRows = await query(
        `SELECT product_order_id FROM sync_log WHERE type IN ('inventory_update', 'qty_increase', 'product_create') AND status = 'success' AND product_order_id IN (${placeholders})`,
        orderIdList.map(id => id.trim())
      );
      if (dupRows.length > 0) {
        return res.status(400).json({ error: '이미 등록된 반품 건입니다.' });
      }
    }

    const brand = inputBrand || extractBrand(name);
    const qtyVal = Math.max(0, parseInt(qty) || 0);
    const trimmedName = name.trim();
    const trimmedColor = color.trim();
    const trimmedSize = size ? size.trim() : null;

    const result = await query(
      'INSERT INTO inventory (name, color, qty, brand, channel_product_no, size) VALUES (?, ?, ?, ?, ?, ?)',
      [trimmedName, trimmedColor, qtyVal, brand, channelProductNo || null, trimmedSize]
    );
    const rows = await query('SELECT * FROM inventory WHERE id = ?', [result.insertId]);

    // 반품에서 불러온 건이면 sync_log에 기록 → 자동 동기화 중복 방지
    // 합산 선택 시 콤마 구분된 여러 productOrderId → 각각 기록
    if (productOrderId) {
      try {
        const orderIdList = productOrderId.includes(',') ? productOrderId.split(',') : [productOrderId];
        for (const oid of orderIdList) {
          const storeFrom = oid.trim().startsWith('CPG_') ? 'C' : oid.trim().startsWith('ZZG_') ? 'D' : 'A';
          const storeLabel = storeFrom === 'C' ? '쿠팡' : storeFrom === 'D' ? '지그재그' : '네이버';
          await query(
            `INSERT INTO sync_log (run_id, type, store_from, store_to, product_order_id, channel_product_no, product_name, product_option, qty, status, message)
             VALUES ('manual', 'inventory_update', ?, NULL, ?, ?, ?, ?, ?, 'success', ?)`,
            [storeFrom, oid.trim(), channelProductNo || null, trimmedName, trimmedColor, qtyVal, `수동 등록 (${storeLabel} 불러오기)`]
          );
        }
      } catch (logErr) {
        console.log('[Inventory] sync_log 기록 실패 (무시):', logErr.message);
      }
    }

    res.status(201).json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/inventory/:id - 재고 수정 (전체 필드)
app.put('/api/inventory/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { qty, name, color, size, brand, channel_product_no } = req.body;
    const sets = [];
    const params = [];
    if (name !== undefined) {
      const trimmed = name.trim();
      if (!trimmed) return res.status(400).json({ error: '상품명을 입력해주세요.' });
      sets.push('name = ?');
      params.push(trimmed);
      // brand가 명시적으로 전달되면 그 값 사용, 아니면 이름에서 추출
      const newBrand = brand !== undefined ? brand : extractBrand(trimmed);
      sets.push('brand = ?');
      params.push(newBrand);
    } else if (brand !== undefined) {
      sets.push('brand = ?');
      params.push(brand);
    }
    if (color !== undefined) {
      sets.push('color = ?');
      params.push(color.trim());
    }
    if (size !== undefined) {
      sets.push('size = ?');
      params.push(size ? size.trim() : null);
    }
    if (channel_product_no !== undefined) {
      sets.push('channel_product_no = ?');
      params.push(channel_product_no || null);
    }
    if (qty !== undefined) {
      sets.push('qty = ?');
      params.push(Math.max(0, parseInt(qty) || 0));
    }
    if (sets.length > 0) {
      sets.push('updated_at = NOW()');
    }
    if (sets.length === 0) {
      return res.status(400).json({ error: '변경할 항목이 없습니다.' });
    }
    params.push(id);
    await query(
      `UPDATE inventory SET ${sets.join(', ')} WHERE id = ?`,
      params
    );
    const rows = await query('SELECT * FROM inventory WHERE id = ?', [id]);
    if (rows.length === 0) {
      return res.status(404).json({ error: '항목을 찾을 수 없습니다.' });
    }
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

// GET /api/server-ip - 서버 아웃바운드 IP 확인 (임시)
app.get('/api/server-ip', async (req, res) => {
  try {
    const r = await fetch('https://api.ipify.org?format=json');
    const data = await r.json();
    res.json({ outboundIp: data.ip });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// --- Sales API Routes ---

// GET /api/sales/stats - 오늘 매출 요약 (어제 비교)
app.get('/api/sales/stats', async (req, res) => {
  try {
    // mysql2 timezone: +09:00 → CURDATE()가 KST 기준, order_date도 KST 저장
    const excludeStatuses = "('CANCELED', 'CANCELED_BY_NOPAYMENT', 'RETURNED', 'EXCHANGED', 'CANCELLED')";
    const today = await query(
      `SELECT COUNT(*) as orders, COALESCE(SUM(total_amount), 0) as revenue FROM sales_orders WHERE DATE(order_date) = CURDATE() AND product_order_status NOT IN ${excludeStatuses}`
    );
    const yest = await query(
      `SELECT COUNT(*) as orders, COALESCE(SUM(total_amount), 0) as revenue FROM sales_orders WHERE DATE(order_date) = CURDATE() - INTERVAL 1 DAY AND product_order_status NOT IN ${excludeStatuses}`
    );

    const todayRevenue = Number(today[0].revenue);
    const todayOrders = Number(today[0].orders);
    const yesterdayRevenue = Number(yest[0].revenue);
    const yesterdayOrders = Number(yest[0].orders);
    const avgPrice = todayOrders > 0 ? Math.round(todayRevenue / todayOrders) : 0;

    // 마지막 수집 시간 조회
    const fetchTimes = await query(
      "SELECT `key`, value FROM sync_config WHERE `key` IN ('sales_last_fetch_a', 'sales_last_fetch_b', 'sales_last_fetch_c', 'sales_last_fetch_d')"
    );
    let lastFetchTime = null;
    for (const row of fetchTimes) {
      if (row.value && (!lastFetchTime || new Date(row.value) > new Date(lastFetchTime))) {
        lastFetchTime = row.value;
      }
    }

    res.json({
      todayRevenue,
      todayOrders,
      avgPrice,
      yesterdayRevenue,
      yesterdayOrders,
      lastFetchTime,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/sales/recent - 주문 목록 (날짜 필터 지원)
app.get('/api/sales/recent', async (req, res) => {
  try {
    const { store, limit: lim, date } = req.query;
    const conditions = [];
    const params = [];

    if (store && store !== 'all') {
      conditions.push('store = ?');
      params.push(store);
    }
    if (date) {
      conditions.push('DATE(order_date) = ?');
      params.push(date);
    }

    const where = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';
    const pageSize = Math.min(parseInt(lim) || 20, 100);

    const rows = await query(
      `SELECT * FROM sales_orders ${where} ORDER BY order_date DESC LIMIT ?`,
      [...params, pageSize]
    );

    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/sales/debug - 주문 조회 디버그 (lastChangedType 생략)
app.get('/api/sales/debug', async (req, res) => {
  try {
    await initSyncClients();
    const now = new Date();
    const from = new Date(now.getTime() - 24 * 60 * 60 * 1000);

    // lastChangedType 생략하여 모든 상태 변경 조회
    const params = new URLSearchParams({
      lastChangedFrom: from.toISOString(),
      lastChangedTo: now.toISOString(),
    });
    const data = await scheduler.storeA.apiCall(
      'GET',
      `/v1/pay-order/seller/product-orders/last-changed-statuses?${params}`
    );
    const statuses = data?.data?.lastChangeStatuses || [];

    // 상태별 분포
    const statusDist = {};
    for (const s of statuses) {
      const key = s.productOrderStatus || '?';
      statusDist[key] = (statusDist[key] || 0) + 1;
    }

    // productOrderId 중복 제거 후 건수
    const uniqueIds = new Set(statuses.map(s => s.productOrderId));

    res.json({
      queryRange: { from: from.toISOString(), to: now.toISOString() },
      totalStatuses: statuses.length,
      uniqueOrders: uniqueIds.size,
      statusDistribution: statusDist,
      sample: statuses.slice(0, 3),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/sales/fetch - 수동 매출 데이터 수집
app.post('/api/sales/fetch', async (req, res) => {
  try {
    await initSyncClients();

    const { resetDays } = req.body || {};
    // 네이버 스토어
    const naverStores = [
      { key: 'A', client: scheduler.storeA, configKey: 'sales_last_fetch_a' },
      { key: 'B', client: scheduler.storeB, configKey: 'sales_last_fetch_b' },
    ];

    // 쿠팡 클라이언트 초기화
    const coupangClient = await initCoupangClient();
    // 지그재그 클라이언트 초기화
    const zigzagClient = await initZigzagClient();

    // 리셋 요청 시 기존 데이터 삭제 + last_fetch 초기화
    if (resetDays) {
      await query('DELETE FROM sales_orders');
      const resetTime = new Date(Date.now() - resetDays * 24 * 60 * 60 * 1000).toISOString();
      for (const s of naverStores) {
        await scheduler.setConfig(s.configKey, resetTime);
      }
      if (coupangClient) await scheduler.setConfig('sales_last_fetch_c', resetTime);
      if (zigzagClient) await scheduler.setConfig('sales_last_fetch_d', resetTime);
      console.log(`[Sales] 전체 리셋: 기존 데이터 삭제 + ${resetDays}일 전부터 재수집`);
    }

    let totalInserted = 0;
    let totalFound = 0;
    const errors = [];
    const storeResults = [];

    // === 네이버 수집 ===
    for (const { key, client, configKey } of naverStores) {
      try {
        const lastFetch = await scheduler.getConfig(configKey);
        const now = new Date();
        const from = (lastFetch && lastFetch.length > 0) ? new Date(lastFetch) : new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

        console.log(`[Sales] Store ${key} 수집 시작: ${from.toISOString()} ~ ${now.toISOString()}`);
        let cursor = new Date(from);
        let storeInserted = 0;
        let storeFound = 0;

        while (cursor < now) {
          const chunkEnd = new Date(Math.min(cursor.getTime() + 24 * 60 * 60 * 1000, now.getTime()));

          try {
            const orderIds = await client.getOrders(cursor.toISOString(), chunkEnd.toISOString());
            storeFound += orderIds.length;

            if (orderIds.length > 0) {
              const batchSize = 50;
              for (let i = 0; i < orderIds.length; i += batchSize) {
                const batch = orderIds.slice(i, i + batchSize);
                const details = await client.getProductOrderDetail(batch);

                for (const detail of details) {
                  const po = detail.productOrder || detail;
                  const order = detail.order || {};
                  const productOrderId = po.productOrderId || '';
                  const rawDate = order.paymentDate || order.orderDate || po.placeOrderDate || chunkEnd.toISOString();
                  const orderDate = new Date(rawDate);
                  const productName = po.productName || '';
                  const optionName = po.optionName || null;
                  const qty = po.quantity || 1;
                  const unitPrice = po.unitPrice || po.salePrice || 0;
                  const totalAmount = po.totalPaymentAmount || po.totalProductAmount || (unitPrice * qty);
                  const status = po.productOrderStatus || '';
                  const channelProductNo = String(po.channelProductNo || po.productId || '');

                  try {
                    const insertResult = await query(
                      `INSERT IGNORE INTO sales_orders (store, product_order_id, order_date, product_name, option_name, qty, unit_price, total_amount, product_order_status, channel_product_no)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                      [key, productOrderId, orderDate, productName, optionName, qty, unitPrice, totalAmount, status, channelProductNo]
                    );
                    if (insertResult.affectedRows > 0) storeInserted++;
                  } catch (dbErr) { }
                }

                if (i + batchSize < orderIds.length) {
                  await new Promise(r => setTimeout(r, 300));
                }
              }
            }
          } catch (chunkErr) {
            errors.push(`Store ${key}: ${chunkErr.message}`);
            console.log(`[Sales] Store ${key} 청크 오류 (${cursor.toISOString()}):`, chunkErr.message);
          }

          cursor = chunkEnd;
          await new Promise(r => setTimeout(r, 300));
        }

        if (!errors.some(e => e.startsWith(`Store ${key}`))) {
          await scheduler.setConfig(configKey, now.toISOString());
        }
        storeResults.push({ store: `네이버(${key})`, found: storeFound, inserted: storeInserted });
        totalInserted += storeInserted;
        totalFound += storeFound;
        console.log(`[Sales] Store ${key} 수집 완료: 발견 ${storeFound}건, 신규 ${storeInserted}건`);
      } catch (storeErr) {
        errors.push(`Store ${key}: ${storeErr.message}`);
        console.error(`[Sales] Store ${key} 오류:`, storeErr.message);
      }
    }

    // === 쿠팡 수집 ===
    if (coupangClient) {
      try {
        const configKey = 'sales_last_fetch_c';
        const lastFetch = await scheduler.getConfig(configKey);
        const now = new Date();
        const from = (lastFetch && lastFetch.length > 0) ? new Date(lastFetch) : new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

        console.log(`[Sales] Coupang 수집 시작: ${from.toISOString()} ~ ${now.toISOString()}`);
        let cursor = new Date(from);
        let storeInserted = 0;
        let storeFound = 0;

        while (cursor < now) {
          const chunkEnd = new Date(Math.min(cursor.getTime() + 24 * 60 * 60 * 1000, now.getTime()));
          try {
            const items = await coupangClient.getOrderItems(cursor.toISOString(), chunkEnd.toISOString());
            storeFound += items.length;
            for (const item of items) {
              try {
                const insertResult = await query(
                  `INSERT IGNORE INTO sales_orders (store, product_order_id, order_date, product_name, option_name, qty, unit_price, total_amount, product_order_status, channel_product_no)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                  ['C', item.productOrderId, item.orderDate, item.productName, item.optionName,
                   item.qty, item.unitPrice, item.totalAmount, item.status, item.channelProductNo]
                );
                if (insertResult.affectedRows > 0) storeInserted++;
              } catch (dbErr) { }
            }
          } catch (chunkErr) {
            errors.push(`Coupang: ${chunkErr.message}`);
            console.log(`[Sales] Coupang 청크 오류 (${cursor.toISOString()}):`, chunkErr.message);
          }
          cursor = chunkEnd;
          await new Promise(r => setTimeout(r, 300));
        }

        if (!errors.some(e => e.startsWith('Coupang'))) {
          await scheduler.setConfig(configKey, now.toISOString());
        }
        storeResults.push({ store: '쿠팡', found: storeFound, inserted: storeInserted });
        totalInserted += storeInserted;
        totalFound += storeFound;
        console.log(`[Sales] Coupang 수집 완료: 발견 ${storeFound}건, 신규 ${storeInserted}건`);
      } catch (e) {
        errors.push(`Coupang: ${e.message}`);
        console.error('[Sales] Coupang 오류:', e.message);
      }
    }

    // === 지그재그 수집 ===
    if (zigzagClient) {
      try {
        const configKey = 'sales_last_fetch_d';
        const lastFetch = await scheduler.getConfig(configKey);
        const now = new Date();
        const from = (lastFetch && lastFetch.length > 0) ? new Date(lastFetch) : new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

        console.log(`[Sales] Zigzag 수집 시작: ${from.toISOString()} ~ ${now.toISOString()}`);
        let storeInserted = 0;
        let storeFound = 0;

        const items = await zigzagClient.getOrderItems(from.toISOString(), now.toISOString());
        storeFound = items.length;
        for (const item of items) {
          try {
            const insertResult = await query(
              `INSERT IGNORE INTO sales_orders (store, product_order_id, order_date, product_name, option_name, qty, unit_price, total_amount, product_order_status, channel_product_no)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              ['D', item.productOrderId, item.orderDate, item.productName, item.optionName,
               item.qty, item.unitPrice, item.totalAmount, item.status, item.channelProductNo]
            );
            if (insertResult.affectedRows > 0) storeInserted++;
          } catch (dbErr) { }
        }

        if (!errors.some(e => e.startsWith('Zigzag'))) {
          await scheduler.setConfig(configKey, now.toISOString());
        }
        storeResults.push({ store: '지그재그', found: storeFound, inserted: storeInserted });
        totalInserted += storeInserted;
        totalFound += storeFound;
        console.log(`[Sales] Zigzag 수집 완료: 발견 ${storeFound}건, 신규 ${storeInserted}건`);
      } catch (e) {
        errors.push(`Zigzag: ${e.message}`);
        console.error('[Sales] Zigzag 오류:', e.message);
      }
    }

    // 신규 매출 푸시 알림
    if (totalInserted > 0) {
      try {
        await scheduler.sendPushNotification('신규 주문', `새 주문 ${totalInserted}건이 들어왔습니다`);
      } catch (pushErr) {
        console.log('[Sales] 푸시 알림 오류:', pushErr.message);
      }
    }

    // 매출 수집 결과를 sync_log에 기록
    const salesRunId = 'sales-manual-' + Date.now();
    for (const d of storeResults) {
      if (d.inserted > 0) {
        try {
          await query(
            `INSERT INTO sync_log (run_id, type, store_from, product_name, qty, status, message) VALUES (?, 'sales_collect', ?, ?, ?, 'success', ?)`,
            [salesRunId, d.store === '쿠팡' ? 'C' : d.store === '지그재그' ? 'D' : d.store.includes('A') ? 'A' : 'B', `${d.store} 매출 수집`, d.inserted, `${d.store} 신규 주문 ${d.inserted}건 수집`]
          );
        } catch (logErr) {
          console.log('[Sales] sync_log 기록 실패:', logErr.message);
        }
      }
    }

    const hasErrors = errors.length > 0;
    const details = storeResults || [];
    const detailMsg = details.map(d => `${d.store}: ${d.inserted}건`).join(', ');
    res.json({
      success: !hasErrors || totalInserted > 0,
      inserted: totalInserted,
      errors,
      details,
      message: hasErrors ? errors.join('; ') : `${totalInserted}건 수집 (${detailMsg})`,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// --- Sync API Routes ---

// GET /api/sync/coupang-returns-debug - 쿠팡 반품 raw 데이터 확인용 (임시)
app.get('/api/sync/coupang-returns-debug', async (req, res) => {
  try {
    await initSyncClients();
    const coupangClient = await initCoupangClient();
    if (!coupangClient) return res.json({ error: 'no coupang client' });
    const hours = parseInt(req.query.hours) || 168;
    const now = new Date();
    const from = new Date(now.getTime() - hours * 60 * 60 * 1000);
    const returns = await coupangClient.getReturnRequests(from.toISOString(), now.toISOString());
    res.json({
      total: returns.length,
      statusDist: returns.reduce((acc, r) => { acc[r.receiptStatus] = (acc[r.receiptStatus] || 0) + 1; return acc; }, {}),
      items: returns.map(r => ({
        receiptId: r.receiptId,
        orderId: r.orderId,
        receiptStatus: r.receiptStatus,
        releaseStatus: r.releaseStatus,
        returnType: r.returnType,
        cancelCount: r.cancelCount,
        buyerName: r.buyerName,
        createdAt: r.createdAt,
        itemNames: (r.returnItems || []).map(i => i.vendorItemName),
        _rawKeys: r._raw ? Object.keys(r._raw) : [],
        _rawSample: r._raw ? JSON.stringify(r._raw).slice(0, 500) : '',
      })),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/sync/returnable-items - 네이버+쿠팡+지그재그 반품/수거 완료 건 목록 (이미 등록된 건도 표시)
app.get('/api/sync/returnable-items', async (req, res) => {
  try {
    await initSyncClients();
    const hours = parseInt(req.query.hours) || 168; // 기본 7일 (반품 요청→수거완료 소요 기간 고려)
    const now = new Date();
    const from = new Date(now.getTime() - hours * 60 * 60 * 1000);

    // === 네이버 반품 조회 ===
    const returnableOrders = await scheduler.storeA.getReturnableOrders(from.toISOString(), now.toISOString());
    console.log(`[Returnable] 네이버: ${returnableOrders.length}건 감지 (${hours}시간)`);

    const items = [];
    const allProductOrderIds = [];

    if (returnableOrders.length > 0) {
      const orderIds = returnableOrders.map(o => o.productOrderId);
      allProductOrderIds.push(...orderIds);
      const statusInfoMap = {};
      for (const o of returnableOrders) {
        statusInfoMap[o.productOrderId] = {
          claimStatus: o.claimStatus,
          lastChangedDate: o.lastChangedDate,
        };
      }

      const details = await scheduler.storeA.getProductOrderDetail(orderIds);
      console.log(`[Returnable] 네이버 상세: ${details.length}건 조회`);

      const debug = req.query.debug === '1';
      for (const detail of details) {
        const po = detail.productOrder || detail;
        const order = detail.order || {};
        const productOrderId = po.productOrderId || '';
        const info = statusInfoMap[productOrderId] || {};
        const claimStatus = po.claimStatus || info.claimStatus || '';
        const productOption = po.productOption || po.optionName || null;

        const item = {
          store: 'A',
          productOrderId,
          productName: po.productName || '',
          optionName: productOption,
          qty: po.quantity || 1,
          channelProductNo: String(po.channelProductNo || po.productId || ''),
          claimStatus,
          claimType: po.claimType || '',
          lastChangedDate: info.lastChangedDate || null,
          ordererName: order.ordererName || po.ordererName || '',
        };

        if (items.length === 0) {
          console.log(`[Returnable] 첫 항목 po 키:`, Object.keys(po).join(', '));
        }
        console.log(`[Returnable] ${po.productName?.slice(0,30)} / opt=${po.optionName} / claimStatus=${claimStatus}`);

        if (debug) {
          item._debug = { po: Object.keys(po), order: Object.keys(order), poFull: po };
        }
        items.push(item);
      }
    }

    // === 쿠팡 반품 조회 ===
    try {
      const coupangClient = await initCoupangClient();
      if (coupangClient) {
        // 쿠팡 API는 조회 기간이 길면 데이터 누락 → 동일 기간 사용
        const coupangFrom = new Date(now.getTime() - hours * 60 * 60 * 1000);
        console.log(`[Returnable] 쿠팡 클라이언트 초기화 성공, 반품 조회 시작 (${Math.round(hours/24)}일)...`);
        const coupangReturns = await coupangClient.getReturnRequests(coupangFrom.toISOString(), now.toISOString());
        console.log(`[Returnable] 쿠팡: ${coupangReturns.length}건 감지`);
        // 상태별 분포 로깅
        const cDist = {};
        for (const r of coupangReturns) { cDist[r.receiptStatus] = (cDist[r.receiptStatus] || 0) + 1; }
        console.log(`[Returnable] 쿠팡 receiptStatus 분포:`, JSON.stringify(cDist));
        // 개별 아이템 로깅 (출고중지 구분 필드 확인용)
        for (const r of coupangReturns) {
          const itemNames = (r.returnItems || []).map(i => i.vendorItemName?.slice(0, 30)).join(', ');
          console.log(`[Returnable] 쿠팡 개별: receiptId=${r.receiptId} status=${r.receiptStatus} releaseStatus=${r.releaseStatus} returnType=${r.returnType} cancelCount=${r.cancelCount} buyer=${r.buyerName} items=[${itemNames}]`);
        }

        for (const ret of coupangReturns) {
          // 출고중지요청(RU)은 주문취소 건이므로 반품 리스트에서 제외
          const skipStatuses = ['RU', 'RELEASE_STOP_UNCHECKED'];
          if (skipStatuses.includes(ret.receiptStatus)) {
            console.log(`[Returnable] 쿠팡 출고중지(${ret.receiptStatus}) 건 제외: receiptId=${ret.receiptId}`);
            continue;
          }

          // 상태 매핑: 쿠팡 receiptStatus → 네이버 claimStatus 호환
          const statusMap = {
            'CNF': 'RETURN_DONE', 'RETURNS_COMPLETED': 'RETURN_DONE',
            'PR': 'WAREHOUSE_CONFIRM', 'VENDOR_WAREHOUSE_CONFIRM': 'WAREHOUSE_CONFIRM',
            'REQUEST_COUPANG_CHECK': 'WAREHOUSE_CONFIRM',
            'CC': 'COLLECT_DONE', 'UNIT_COLLECTED': 'COLLECT_DONE',
            'UC': 'COLLECTING', 'RETURNS_UNCHECKED': 'COLLECTING',
          };
          const claimStatus = statusMap[ret.receiptStatus] || 'COLLECTING';

          for (const ri of ret.returnItems) {
            const productOrderId = `CPG_RET_${ret.receiptId}_${ri.vendorItemId}`;
            allProductOrderIds.push(productOrderId);

            // vendorItemName 파싱: "ob 캐시미어 니트, 아이보리 free" → 상품명/색상/사이즈 분리
            const parsed = parseCoupangItemName(ri.vendorItemName);

            // optionName: sellerProductItemName 우선 사용 (구체적 옵션)
            // sellerProductItemName = 해당 vendorItem의 실제 옵션 (예: "아이보리 free")
            const spi = (ri.sellerProductItemName || '').trim();
            let optionName;
            if (spi) {
              // sellerProductItemName에서 색상/사이즈 분리
              const spiTokens = spi.split(/[\s/]+/).filter(t => t);
              const sizeRe = /^(free|xxl|xl|l|m|s|f)$/i;
              // 브랜드 이니셜(2글자 영문)은 제외
              const brandRe = /^[a-zA-Z]{2}$/;
              const spiColors = spiTokens.filter(t => !sizeRe.test(t) && !brandRe.test(t));
              const spiSizes = spiTokens.filter(t => sizeRe.test(t)).map(t =>
                t.toUpperCase() === 'FREE' ? 'Free' : t.toUpperCase()
              );
              const optParts = [];
              if (spiColors.length > 0) optParts.push(`색상: ${spiColors.join(' ')}`);
              if (spiSizes.length > 0) optParts.push(`사이즈: ${spiSizes[0]}`);
              else if (parsed.size) optParts.push(`사이즈: ${parsed.size}`);
              optionName = optParts.length > 0 ? optParts.join(' / ') : spi;
            } else {
              // fallback: vendorItemName 파싱 결과 사용
              const optParts = [];
              if (parsed.color) optParts.push(`색상: ${parsed.color}`);
              if (parsed.size) optParts.push(`사이즈: ${parsed.size}`);
              optionName = optParts.length > 0 ? optParts.join(' / ') : null;
            }

            items.push({
              store: 'C',
              productOrderId,
              productName: parsed.productName,
              optionName,
              brand: parsed.brand || '',
              qty: ri.returnQuantity || 1,
              channelProductNo: ri.vendorItemId,
              claimStatus,
              claimType: 'RETURN',
              lastChangedDate: ret.createdAt || null,
              ordererName: ret.buyerName || '',
              _parsed: parsed,
              sellerProductItemName: ri.sellerProductItemName || '',
              colorOptions: parsed.colorOptions || [],
              sizeOptions: parsed.sizeOptions || [],
            });
          }
        }
      } else {
        console.log(`[Returnable] 쿠팡 클라이언트 미설정 (API 키 없음)`);
      }
    } catch (coupangErr) {
      console.error(`[Returnable] 쿠팡 조회 실패:`, coupangErr.message);
    }

    // === 지그재그 반품 조회 ===
    try {
      const zigzagClient = await initZigzagClient();
      if (zigzagClient) {
        const zigzagFrom = new Date(now.getTime() - hours * 2 * 60 * 60 * 1000);
        console.log(`[Returnable] 지그재그 반품 조회 시작 (${Math.round(hours*2/24)}일)...`);
        const zigzagReturns = await zigzagClient.getReturnRequests(zigzagFrom.toISOString(), now.toISOString());
        console.log(`[Returnable] 지그재그: ${zigzagReturns.length}건 감지`);

        for (const ret of zigzagReturns) {
          const statusMap = {
            'RETURN_REQUESTED': 'COLLECTING',
            'RETURN_COLLECTING': 'COLLECTING',
            'RETURNED': 'COLLECT_DONE',
          };
          const claimStatus = statusMap[ret.receiptStatus] || 'COLLECTING';
          console.log(`[Returnable] 지그재그 개별: receiptId=${ret.receiptId} receiptStatus=${ret.receiptStatus} → claimStatus=${claimStatus} buyer=${ret.buyerName}`);

          for (const ri of ret.returnItems) {
            const productOrderId = `ZZG_RET_${ret.receiptId}_${ri.vendorItemId}`;
            allProductOrderIds.push(productOrderId);
            console.log(`[Returnable] 지그재그 아이템: ${ri.vendorItemName?.slice(0,30)} opt=${ri.sellerProductItemName} qty=${ri.returnQuantity}`);

            // 옵션 파싱
            const optionName = ri.sellerProductItemName || null;

            items.push({
              store: 'D',
              productOrderId,
              productName: ri.vendorItemName || '',
              optionName,
              qty: ri.returnQuantity || 1,
              channelProductNo: ri.vendorItemId,
              claimStatus,
              claimType: 'RETURN',
              lastChangedDate: ret.createdAt || null,
              ordererName: ret.buyerName || '',
            });
          }
        }
      } else {
        console.log(`[Returnable] 지그재그 클라이언트 미설정 (API 키 없음)`);
      }
    } catch (zigzagErr) {
      console.error(`[Returnable] 지그재그 조회 실패:`, zigzagErr.message);
    }

    // === 처리 상태 조회 (재고 반영 / B스토어 복사 분리) ===
    let inventoryIds = new Set();
    let storeIds = new Set();
    if (allProductOrderIds.length > 0) {
      const placeholders = allProductOrderIds.map(() => '?').join(',');
      const logRows = await query(
        `SELECT product_order_id, type FROM sync_log WHERE type IN ('inventory_update', 'qty_increase', 'product_create') AND status = 'success' AND product_order_id IN (${placeholders})`,
        allProductOrderIds
      );
      for (const row of logRows) {
        if (row.type === 'inventory_update') inventoryIds.add(row.product_order_id);
        if (row.type === 'qty_increase' || row.type === 'product_create') storeIds.add(row.product_order_id);
      }
    }

    // === confirmedPickup 조회 (return_confirmations) ===
    let confirmedIds = new Set();
    if (allProductOrderIds.length > 0) {
      const placeholders2 = allProductOrderIds.map(() => '?').join(',');
      const confirmRows = await query(
        `SELECT product_order_id FROM return_confirmations WHERE product_order_id IN (${placeholders2})`,
        allProductOrderIds
      );
      confirmedIds = new Set(confirmRows.map(r => r.product_order_id));
    }

    // 플래그 설정
    for (const item of items) {
      item.inventoryAdded = inventoryIds.has(item.productOrderId);
      item.storeAdded = storeIds.has(item.productOrderId);
      // 쿠팡/지그재그는 B스토어 복사 불필요 → 재고만으로 완료 판정
      item.alreadyAdded = (item.store === 'C' || item.store === 'D')
        ? item.inventoryAdded
        : (item.inventoryAdded && item.storeAdded);
      item.confirmedPickup = confirmedIds.has(item.productOrderId);
    }

    console.log(`[Returnable] 최종: ${items.length}건 (재고 ${inventoryIds.size}, 스토어 ${storeIds.size}, 실수거완료 ${confirmedIds.size})`);
    res.json(items);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

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
    // 쿠팡
    const cAccessKey = process.env.COUPANG_ACCESS_KEY || config.coupang_access_key || '';
    const cSecretKey = process.env.COUPANG_SECRET_KEY || config.coupang_secret_key || '';
    const cVendorId = process.env.COUPANG_VENDOR_ID || config.coupang_vendor_id || '';
    config.coupang_access_key = cAccessKey ? maskSecret(cAccessKey) : '';
    config.coupang_secret_key = cSecretKey ? '****' : '';
    config.coupang_vendor_id = cVendorId || '';
    // 지그재그
    const zAccessKey = process.env.ZIGZAG_ACCESS_KEY || config.zigzag_access_key || '';
    const zSecretKey = process.env.ZIGZAG_SECRET_KEY || config.zigzag_secret_key || '';
    config.zigzag_access_key = zAccessKey ? maskSecret(zAccessKey) : '';
    config.zigzag_secret_key = zSecretKey ? '****' : '';
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

// DELETE /api/sync/mappings - B스토어 매핑 전체 초기화
app.delete('/api/sync/mappings', async (req, res) => {
  try {
    const [mappingResult] = await Promise.all([
      query('DELETE FROM product_mapping'),
      query('DELETE FROM channel_product_mapping WHERE target_channel = ?', ['storeB']),
    ]);
    res.json({ deleted: mappingResult.affectedRows, message: '매핑 초기화 완료' });
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
    const { store_a_client_id, store_a_client_secret, store_b_client_id, store_b_client_secret,
            store_b_display_status, store_b_sale_status, store_b_name_prefix,
            store_b_return_fee, store_b_exchange_fee,
            sync_interval_minutes,
            coupang_access_key, coupang_secret_key, coupang_vendor_id,
            coupang_category_code, coupang_outbound_code, coupang_return_center_code, coupang_price_rate,
            zigzag_access_key, zigzag_secret_key,
            zigzag_category_id, zigzag_price_rate } = req.body;
    const upsertSql = 'INSERT INTO sync_config (`key`, value, updated_at) VALUES (?, ?, NOW()) ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = NOW()';
    if (store_a_client_id) await query(upsertSql, ['store_a_client_id', store_a_client_id]);
    if (store_a_client_secret) await query(upsertSql, ['store_a_client_secret', store_a_client_secret]);
    if (store_b_client_id) await query(upsertSql, ['store_b_client_id', store_b_client_id]);
    if (store_b_client_secret) await query(upsertSql, ['store_b_client_secret', store_b_client_secret]);
    if (store_b_display_status) await query(upsertSql, ['store_b_display_status', store_b_display_status]);
    if (store_b_sale_status) await query(upsertSql, ['store_b_sale_status', store_b_sale_status]);
    if (store_b_name_prefix !== undefined) await query(upsertSql, ['store_b_name_prefix', store_b_name_prefix]);
    if (store_b_return_fee !== undefined) await query(upsertSql, ['store_b_return_fee', store_b_return_fee]);
    if (store_b_exchange_fee !== undefined) await query(upsertSql, ['store_b_exchange_fee', store_b_exchange_fee]);
    // 동기화 주기
    if (sync_interval_minutes) await query(upsertSql, ['sync_interval_minutes', sync_interval_minutes]);
    // 쿠팡
    if (coupang_access_key) await query(upsertSql, ['coupang_access_key', coupang_access_key]);
    if (coupang_secret_key) await query(upsertSql, ['coupang_secret_key', coupang_secret_key]);
    if (coupang_vendor_id) await query(upsertSql, ['coupang_vendor_id', coupang_vendor_id]);
    if (coupang_category_code !== undefined) await query(upsertSql, ['coupang_category_code', coupang_category_code]);
    if (coupang_outbound_code !== undefined) await query(upsertSql, ['coupang_outbound_code', coupang_outbound_code]);
    if (coupang_return_center_code !== undefined) await query(upsertSql, ['coupang_return_center_code', coupang_return_center_code]);
    if (coupang_price_rate !== undefined) await query(upsertSql, ['coupang_price_rate', coupang_price_rate]);
    // 지그재그
    if (zigzag_access_key) await query(upsertSql, ['zigzag_access_key', zigzag_access_key]);
    if (zigzag_secret_key) await query(upsertSql, ['zigzag_secret_key', zigzag_secret_key]);
    if (zigzag_category_id !== undefined) await query(upsertSql, ['zigzag_category_id', zigzag_category_id]);
    if (zigzag_price_rate !== undefined) await query(upsertSql, ['zigzag_price_rate', zigzag_price_rate]);
    scheduler.storeA = null;
    scheduler.storeB = null;
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/coupang/test-connection - 쿠팡 연결 테스트
app.post('/api/coupang/test-connection', async (req, res) => {
  try {
    const { accessKey, secretKey, vendorId } = req.body;
    if (!accessKey || !secretKey || !vendorId) {
      return res.status(400).json({ error: 'Access Key, Secret Key, Vendor ID를 모두 입력해주세요.' });
    }
    const client = new CoupangClient(accessKey, secretKey, vendorId, 'Coupang-Test');
    const result = await client.testConnection();
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/zigzag/test-connection - 지그재그 연결 테스트
app.post('/api/zigzag/test-connection', async (req, res) => {
  try {
    const { accessKey, secretKey } = req.body;
    if (!accessKey || !secretKey) {
      return res.status(400).json({ error: 'Access Key, Secret Key를 모두 입력해주세요.' });
    }
    const client = new ZigzagClient(accessKey, secretKey, 'Zigzag-Test');
    const result = await client.testConnection();
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/coupang/debug-returns - 쿠팡 반품 조회 + 파싱 결과 확인
app.get('/api/coupang/debug-returns', async (req, res) => {
  try {
    const coupangClient = await initCoupangClient();
    if (!coupangClient) {
      return res.json({ error: '쿠팡 API 키 미설정', keys: { accessKey: !!process.env.COUPANG_ACCESS_KEY, secretKey: !!process.env.COUPANG_SECRET_KEY, vendorId: !!process.env.COUPANG_VENDOR_ID } });
    }

    const hours = parseInt(req.query.hours) || 168;
    const now = new Date();
    const from = new Date(now.getTime() - hours * 60 * 60 * 1000);

    console.log(`[Coupang Debug] 반품 조회: ${from.toISOString()} ~ ${now.toISOString()}`);

    const returns = await coupangClient.getReturnRequests(from.toISOString(), now.toISOString());

    // 각 아이템에 파싱 결과 추가
    const parsedReturns = returns.map(ret => ({
      ...ret,
      returnItems: ret.returnItems.map(ri => ({
        ...ri,
        _parsed: parseCoupangItemName(ri.vendorItemName),
      })),
    }));

    res.json({
      dateRange: { from: from.toISOString(), to: now.toISOString(), hours },
      totalReturns: returns.length,
      totalItems: returns.reduce((sum, r) => sum + r.returnItems.length, 0),
      returns: parsedReturns,
    });
  } catch (e) {
    res.status(500).json({ error: e.message, stack: e.stack?.split('\n').slice(0, 5) });
  }
});

// --- 실수거완료 API ---

// POST /api/returns/confirm-pickup - 실수거완료 처리 (복수 건 지원)
app.post('/api/returns/confirm-pickup', async (req, res) => {
  try {
    const { items } = req.body;
    if (!items || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: '실수거완료할 항목을 선택해주세요.' });
    }

    let confirmed = 0;
    let skipped = 0;
    for (const item of items) {
      if (!item.productOrderId) continue;
      try {
        const result = await query(
          `INSERT IGNORE INTO return_confirmations (product_order_id, store, product_name, option_name, qty, channel_product_no, orderer_name)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
          [item.productOrderId, item.store || 'A', item.productName || null,
           item.optionName || null, item.qty || 1, item.channelProductNo || null, item.ordererName || null]
        );
        if (result.affectedRows > 0) confirmed++;
        else skipped++;
      } catch (dbErr) {
        skipped++;
      }
    }

    res.json({ success: true, confirmed, skipped });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/returns/confirmed - 실수거완료 리스트 (재고 추가 여부 포함)
// ?finalized=true → 최종완료된 건만, ?finalized=false → 미완료 건만 (기본), 생략 → 전체
app.get('/api/returns/confirmed', async (req, res) => {
  try {
    const { finalized } = req.query;
    let whereClause = '';
    if (finalized === 'true') whereClause = ' WHERE finalized_at IS NOT NULL';
    else if (finalized === 'false' || finalized === undefined) whereClause = ' WHERE finalized_at IS NULL';

    const orderBy = finalized === 'true' ? ' ORDER BY finalized_at DESC' : ' ORDER BY confirmed_at DESC';
    const rows = await query('SELECT * FROM return_confirmations' + whereClause + orderBy);

    // sync_log에서 처리 완료 여부 확인 (재고/스토어 분리)
    let inventoryIds = new Set();
    let storeIds = new Set();
    if (rows.length > 0) {
      const allIds = rows.map(r => r.product_order_id);
      const placeholders = allIds.map(() => '?').join(',');
      const logRows = await query(
        `SELECT product_order_id, type FROM sync_log WHERE type IN ('inventory_update', 'qty_increase', 'product_create') AND status = 'success' AND product_order_id IN (${placeholders})`,
        allIds
      );
      for (const row of logRows) {
        if (row.type === 'inventory_update') inventoryIds.add(row.product_order_id);
        if (row.type === 'qty_increase' || row.type === 'product_create') storeIds.add(row.product_order_id);
      }
    }

    const items = rows.map(r => ({
      ...r,
      inventoryAdded: inventoryIds.has(r.product_order_id),
      storeAdded: storeIds.has(r.product_order_id),
      alreadyAdded: inventoryIds.has(r.product_order_id),
    }));

    res.json(items);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/returns/finalize - 최종완료 처리 (단건/복수)
app.post('/api/returns/finalize', async (req, res) => {
  try {
    const { productOrderIds } = req.body;
    if (!productOrderIds || !Array.isArray(productOrderIds) || productOrderIds.length === 0) {
      return res.status(400).json({ error: '최종완료할 항목을 선택해주세요.' });
    }
    const placeholders = productOrderIds.map(() => '?').join(',');
    const result = await query(
      `UPDATE return_confirmations SET finalized_at = NOW() WHERE product_order_id IN (${placeholders}) AND finalized_at IS NULL`,
      productOrderIds
    );
    res.json({ success: true, finalized: result.affectedRows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/returns/unfinalize - 최종완료 취소 (복원)
app.post('/api/returns/unfinalize', async (req, res) => {
  try {
    const { productOrderIds } = req.body;
    if (!productOrderIds || !Array.isArray(productOrderIds) || productOrderIds.length === 0) {
      return res.status(400).json({ error: '복원할 항목을 선택해주세요.' });
    }
    const placeholders = productOrderIds.map(() => '?').join(',');
    const result = await query(
      `UPDATE return_confirmations SET finalized_at = NULL WHERE product_order_id IN (${placeholders}) AND finalized_at IS NOT NULL`,
      productOrderIds
    );
    res.json({ success: true, unfinalized: result.affectedRows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// --- Product Copy API ---

// GET /api/store-a/products/search - A 스토어 상품 검색 (DB 인덱스 기반 즉시 검색)
app.get('/api/store-a/products/search', async (req, res) => {
  try {
    const { keyword } = req.query;
    if (!keyword || keyword.trim().length === 0) {
      return res.status(400).json({ error: '검색 키워드를 입력해주세요.' });
    }

    // DB에서 인덱스된 상품 수 확인
    const countRows = await query('SELECT COUNT(*) as cnt FROM store_a_products');
    const indexedCount = countRows[0].cnt;

    if (indexedCount === 0) {
      return res.json({
        items: [],
        total: 0,
        indexStatus: { indexed: 0, indexing: indexingActive, progress: indexingProgress },
        message: '상품 인덱스가 비어있습니다. "인덱싱 시작" 버튼을 눌러주세요.',
      });
    }

    // 키워드 공백 분리 → AND 조건 LIKE 검색
    const keywords = keyword.trim().split(/\s+/);
    const conditions = keywords.map(() => 'name LIKE ?');
    const params = keywords.map(k => `%${k}%`);

    const rows = await query(
      `SELECT * FROM store_a_products WHERE ${conditions.join(' AND ')} ORDER BY name LIMIT 100`,
      params
    );

    // 매핑 정보 추가
    const items = [];
    for (const row of rows) {
      let mappings = [];
      let storeBMapping = null;
      if (row.channel_product_no) {
        mappings = await query(
          'SELECT target_channel, target_product_id, copy_status FROM channel_product_mapping WHERE store_a_channel_product_no = ?',
          [row.channel_product_no]
        );
        const pmRows = await query(
          'SELECT store_b_channel_product_no FROM product_mapping WHERE store_a_channel_product_no = ? AND match_status = ? LIMIT 1',
          [row.channel_product_no, 'matched']
        );
        if (pmRows.length > 0) storeBMapping = pmRows[0].store_b_channel_product_no;
      }

      items.push({
        channelProductNo: row.channel_product_no,
        originProductNo: row.origin_product_no,
        name: row.name,
        salePrice: row.sale_price,
        stockQuantity: row.stock_quantity,
        statusType: row.status_type,
        representativeImage: row.image_url,
        channelMappings: mappings,
        storeBProductNo: storeBMapping,
      });
    }

    res.json({
      items,
      total: items.length,
      indexStatus: { indexed: indexedCount, indexing: indexingActive, progress: indexingProgress },
    });
  } catch (e) {
    console.error('[StoreA Search] 오류:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/store-a/products/index-status - 인덱스 상태
app.get('/api/store-a/products/index-status', async (req, res) => {
  try {
    const countRows = await query('SELECT COUNT(*) as cnt FROM store_a_products');
    res.json({
      indexed: countRows[0].cnt,
      indexing: indexingActive,
      progress: indexingProgress,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/store-a/products/index-start - 인덱싱 시작
app.post('/api/store-a/products/index-start', async (req, res) => {
  if (indexingActive) {
    return res.json({ message: '이미 인덱싱 중입니다.', indexing: true, progress: indexingProgress });
  }
  // DB가 비어있으면 fullRefresh=true로 전체 인덱싱
  const dbRows = await query('SELECT COUNT(*) as cnt FROM store_a_products');
  const dbCount = dbRows[0].cnt;
  const fullRefresh = dbCount === 0;
  runProductIndexing(fullRefresh).catch(e => console.error('[Index] 오류:', e.message));
  res.json({ message: fullRefresh ? '전체 인덱싱을 시작했습니다.' : '증분 인덱싱을 시작했습니다.', indexing: true });
});

// POST /api/store-a/products/index-stop - 인덱싱 중지
app.post('/api/store-a/products/index-stop', (req, res) => {
  indexingActive = false;
  res.json({ message: '인덱싱을 중지합니다.' });
});

// GET /api/store-a/products/:id - A 스토어 상품 상세
app.get('/api/store-a/products/:id', async (req, res) => {
  try {
    await initSyncClients();
    const { id } = req.params;
    const product = await scheduler.storeA.getChannelProduct(id);
    res.json(product);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/products/copy - 멀티채널 상품 복사
app.post('/api/products/copy', async (req, res) => {
  try {
    await initSyncClients();
    const { channelProductNo, targets, options } = req.body;

    if (!channelProductNo) {
      return res.status(400).json({ error: 'channelProductNo가 필요합니다.' });
    }
    if (!targets || !Array.isArray(targets) || targets.length === 0) {
      return res.status(400).json({ error: '복사 대상 채널을 선택해주세요.' });
    }

    const validTargets = ['storeB', 'coupang', 'zigzag'];
    const invalidTargets = targets.filter(t => !validTargets.includes(t));
    if (invalidTargets.length > 0) {
      return res.status(400).json({ error: `지원하지 않는 채널: ${invalidTargets.join(', ')}` });
    }

    const result = await scheduler.copyToChannels(channelProductNo, targets, options || {});
    res.json(result);
  } catch (e) {
    console.error('[ProductCopy] 오류:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/products/copy-bulk - 여러 상품 일괄 복사
app.post('/api/products/copy-bulk', async (req, res) => {
  try {
    await initSyncClients();
    const { products, targets, options } = req.body;

    if (!products || !Array.isArray(products) || products.length === 0) {
      return res.status(400).json({ error: '복사할 상품을 선택해주세요.' });
    }
    if (!targets || !Array.isArray(targets) || targets.length === 0) {
      return res.status(400).json({ error: '복사 대상 채널을 선택해주세요.' });
    }

    const results = [];
    for (const channelProductNo of products) {
      try {
        const result = await scheduler.copyToChannels(String(channelProductNo), targets, options || {});
        results.push(result);
      } catch (e) {
        results.push({
          source: { channelProductNo },
          results: {},
          error: e.message,
        });
      }
      // API rate limit 방지
      await new Promise(r => setTimeout(r, 1000));
    }

    const summary = {
      total: products.length,
      success: results.filter(r => !r.error && Object.values(r.results || {}).some(v => v.success)).length,
      failed: results.filter(r => r.error || Object.values(r.results || {}).every(v => !v.success)).length,
    };

    res.json({ summary, details: results });
  } catch (e) {
    console.error('[BulkCopy] 오류:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/products/copy-history - 복사 이력 조회
app.get('/api/products/copy-history', async (req, res) => {
  try {
    const { page, limit: lim } = req.query;
    const pageNum = parseInt(page) || 1;
    const pageSize = parseInt(lim) || 30;
    const offset = (pageNum - 1) * pageSize;

    const countRows = await query('SELECT COUNT(*) as total FROM channel_product_mapping');
    const total = countRows[0].total;

    const rows = await query(
      'SELECT * FROM channel_product_mapping ORDER BY updated_at DESC LIMIT ? OFFSET ?',
      [pageSize, offset]
    );

    res.json({ items: rows, total, page: pageNum, totalPages: Math.ceil(total / pageSize) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/returns/copy-to-store - 수동 B스토어 복사
app.post('/api/returns/copy-to-store', async (req, res) => {
  try {
    await initSyncClients();
    const { productOrderId } = req.body;
    if (!productOrderId) {
      return res.status(400).json({ error: 'productOrderId가 필요합니다.' });
    }

    // 중복 체크
    const dupCheck = await query(
      "SELECT id FROM sync_log WHERE type IN ('qty_increase', 'product_create') AND status = 'success' AND product_order_id = ? LIMIT 1",
      [productOrderId]
    );
    if (dupCheck.length > 0) {
      return res.status(400).json({ error: '이미 스토어에 등록된 건입니다.' });
    }

    // 네이버 상품 상세 조회
    const details = await scheduler.storeA.getProductOrderDetail([productOrderId]);
    if (!details || details.length === 0) {
      return res.status(404).json({ error: '주문 정보를 찾을 수 없습니다.' });
    }

    const detail = details[0];
    const runId = 'manual-store-' + Date.now();
    const productName = scheduler.extractProductName(detail);
    const optionName = scheduler.extractOptionName(detail);
    const qty = scheduler.extractQty(detail);
    const channelProductNo = scheduler.extractChannelProductNo(detail);

    // product_mapping 확인 → B스토어 복사
    const safeOptionName = optionName || '';
    const mappingRows = await query(
      'SELECT * FROM product_mapping WHERE store_a_channel_product_no = ? AND store_a_option_name = ?',
      [channelProductNo, safeOptionName]
    );
    const mapping = mappingRows[0];

    if (mapping && mapping.match_status !== 'unmatched' && mapping.store_b_channel_product_no) {
      try {
        await scheduler.increaseStoreB(runId, mapping.store_b_channel_product_no, productName, optionName, qty, productOrderId);
      } catch (e) {
        const isNotFound = e.message && (e.message.includes('404') || e.message.includes('not found'));
        if (isNotFound) {
          await scheduler.copyAndCreateInStoreB(runId, detail, channelProductNo, productName, optionName, qty, productOrderId);
        } else {
          throw e;
        }
      }
    } else {
      await scheduler.copyAndCreateInStoreB(runId, detail, channelProductNo, productName, optionName, qty, productOrderId);
    }

    res.json({ success: true, message: 'B스토어에 등록되었습니다.' });
  } catch (e) {
    console.error('[CopyToStore] 오류:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/returns/confirm-pickup/:productOrderId - 실수거완료 취소
app.delete('/api/returns/confirm-pickup/:productOrderId', async (req, res) => {
  try {
    const { productOrderId } = req.params;
    const result = await query(
      'DELETE FROM return_confirmations WHERE product_order_id = ?',
      [productOrderId]
    );
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: '해당 실수거완료 건을 찾을 수 없습니다.' });
    }
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// --- Push Notification API ---

// VAPID 키 초기화 헬퍼
async function getVapidKeys() {
  const pub = await scheduler.getConfig('vapid_public_key');
  const priv = await scheduler.getConfig('vapid_private_key');
  if (pub && priv) return { publicKey: pub, privateKey: priv };
  // 자동 생성
  const keys = webpush.generateVAPIDKeys();
  await scheduler.setConfig('vapid_public_key', keys.publicKey);
  await scheduler.setConfig('vapid_private_key', keys.privateKey);
  return keys;
}

// GET /api/push/vapid-key - VAPID 공개키 반환
app.get('/api/push/vapid-key', async (req, res) => {
  try {
    const keys = await getVapidKeys();
    res.json({ publicKey: keys.publicKey });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/push/subscribe - 푸시 구독 저장
app.post('/api/push/subscribe', async (req, res) => {
  try {
    const { endpoint, keys } = req.body;
    if (!endpoint || !keys?.p256dh || !keys?.auth) {
      return res.status(400).json({ error: '유효하지 않은 구독 정보입니다.' });
    }
    await query(
      `INSERT INTO push_subscriptions (endpoint, p256dh, auth) VALUES (?, ?, ?)
       ON DUPLICATE KEY UPDATE p256dh = VALUES(p256dh), auth = VALUES(auth), created_at = NOW()`,
      [endpoint, keys.p256dh, keys.auth]
    );
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/push/unsubscribe - 푸시 구독 해제
app.post('/api/push/unsubscribe', async (req, res) => {
  try {
    const { endpoint } = req.body;
    if (endpoint) {
      await query('DELETE FROM push_subscriptions WHERE endpoint = ?', [endpoint]);
    }
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/push/test - 테스트 푸시 발송
app.post('/api/push/test', async (req, res) => {
  try {
    const keys = await getVapidKeys();
    webpush.setVapidDetails('mailto:bluefi@example.com', keys.publicKey, keys.privateKey);

    const subs = await query('SELECT * FROM push_subscriptions');
    if (subs.length === 0) {
      return res.json({ success: false, message: '등록된 구독이 없습니다. 알림을 먼저 허용해주세요.' });
    }

    const payload = JSON.stringify({ title: '블루파이', body: '✅ 푸시 알림 테스트 성공!' });
    let sent = 0;
    const errors = [];
    for (const sub of subs) {
      try {
        await webpush.sendNotification({ endpoint: sub.endpoint, keys: { p256dh: sub.p256dh, auth: sub.auth } }, payload);
        sent++;
      } catch (e) {
        console.log(`[Push Test] 발송 실패 (sub ${sub.id}): status=${e.statusCode}, ${e.message}`);
        errors.push(`sub${sub.id}: ${e.statusCode || 'unknown'}`);
        if (e.statusCode === 404 || e.statusCode === 410) {
          await query('DELETE FROM push_subscriptions WHERE id = ?', [sub.id]);
        }
      }
    }
    res.json({
      success: sent > 0,
      message: sent > 0 ? `${sent}/${subs.length}개 기기에 발송 완료` : `발송 실패 (${subs.length}개 구독 중 0개 성공)`,
      errors: errors.length > 0 ? errors : undefined,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// === Bulk Registration (대량 등록) ===

// GET /api/bulk/config - 대량등록 템플릿 설정 조회
app.get('/api/bulk/config', async (req, res) => {
  try {
    const rows = await query("SELECT `key`, value FROM sync_config WHERE `key` LIKE 'bulk_%'");
    const config = {};
    for (const row of rows) {
      config[row.key] = row.value;
    }
    // FTP 비밀번호 마스킹
    if (config.bulk_ftp_password) {
      const pw = config.bulk_ftp_password;
      config.bulk_ftp_password = pw.length > 4 ? '****' + pw.slice(-4) : '****';
    }
    res.json(config);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/bulk/config - 대량등록 템플릿 설정 저장
app.put('/api/bulk/config', async (req, res) => {
  try {
    const entries = req.body;
    if (!entries || typeof entries !== 'object') {
      return res.status(400).json({ error: '설정 데이터가 필요합니다.' });
    }
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      for (const [key, value] of Object.entries(entries)) {
        if (!key.startsWith('bulk_')) continue;
        // 마스킹된 비밀번호는 무시
        if (key === 'bulk_ftp_password' && value && value.startsWith('****')) continue;
        await conn.query(
          "INSERT INTO sync_config (`key`, value, updated_at) VALUES (?, ?, NOW()) ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = NOW()",
          [key, value || '']
        );
      }
      await conn.commit();
      res.json({ success: true });
    } finally {
      conn.release();
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/bulk/upload-images - 이미지 업로드 → FTP 전송 → URL 반환
app.post('/api/bulk/upload-images', bulkUpload.array('images', 20), async (req, res) => {
  const tempFiles = (req.files || []).map(f => f.path);
  try {
    // FTP 설정 읽기
    const configRows = await query("SELECT `key`, value FROM sync_config WHERE `key` LIKE 'bulk_ftp_%'");
    const cfg = {};
    for (const r of configRows) cfg[r.key] = r.value;

    const ftpHost = process.env.BULK_FTP_HOST || cfg.bulk_ftp_host;
    const ftpPort = parseInt(process.env.BULK_FTP_PORT || cfg.bulk_ftp_port) || 21;
    const ftpUser = process.env.BULK_FTP_USER || cfg.bulk_ftp_user;
    const ftpPass = process.env.BULK_FTP_PASSWORD || cfg.bulk_ftp_password;
    const ftpPath = process.env.BULK_FTP_PATH || cfg.bulk_ftp_path || '/';
    const ftpUrlBase = process.env.BULK_FTP_URL_BASE || cfg.bulk_ftp_url_base || '';

    if (!ftpHost || !ftpUser || !ftpPass) {
      return res.status(400).json({ error: 'FTP 설정이 필요합니다. 템플릿 설정에서 FTP 정보를 입력해주세요.' });
    }

    const client = new ftp.Client();
    client.ftp.verbose = false;
    try {
      await client.access({ host: ftpHost, port: ftpPort, user: ftpUser, password: ftpPass, secure: false });
      await client.ensureDir(ftpPath);

      const urls = [];
      const timestamp = Date.now();
      for (let i = 0; i < req.files.length; i++) {
        const file = req.files[i];
        const ext = path.extname(file.originalname).toLowerCase() || '.jpg';
        const remoteName = `bulk_${timestamp}_${String(i + 1).padStart(2, '0')}${ext}`;
        const remotePath = `${ftpPath}/${remoteName}`.replace(/\/+/g, '/');

        await client.uploadFrom(file.path, remotePath);
        const url = `${ftpUrlBase.replace(/\/$/, '')}/${remoteName}`;
        urls.push(url);
      }
      client.close();
      res.json({ urls });
    } catch (ftpErr) {
      client.close();
      throw new Error(`FTP 업로드 실패: ${ftpErr.message}`);
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  } finally {
    // 임시 파일 정리
    for (const f of tempFiles) {
      fs.unlink(f, () => {});
    }
  }
});

// A 스토어 클라이언트만 가져오기 (B 스토어 없어도 동작)
async function getStoreAClient() {
  if (scheduler.storeA) return scheduler.storeA;
  const getVal = async (key) => {
    const rows = await query('SELECT value FROM sync_config WHERE `key` = ?', [key]);
    return rows[0] ? rows[0].value : '';
  };
  const aId = process.env.STORE_A_CLIENT_ID || await getVal('store_a_client_id');
  const aSecret = process.env.STORE_A_CLIENT_SECRET || await getVal('store_a_client_secret');
  if (!aId || !aSecret) throw new Error('A 스토어 API 키가 설정되지 않았습니다.');
  return new NaverCommerceClient(aId, aSecret, 'StoreA');
}

// GET /api/bulk/categories - 네이버 카테고리 조회
app.get('/api/bulk/categories', async (req, res) => {
  try {
    const client = await getStoreAClient();
    const parentId = req.query.parentId || '';

    // 하위 카테고리가 있는 것들 (네이버 API가 hasChildren을 안 주는 경우 대비)
    const knownParents = new Set(['50021279','50021359','50000814']); // 니트, 아우터, 점퍼

    if (!parentId) {
      const subs = await client.apiCall('GET', '/v1/categories/50000167/sub-categories');
      res.json(subs.map(c => ({ id: String(c.id), name: c.name, hasChildren: knownParents.has(String(c.id)) || !!c.hasChildren })));
    } else {
      const subs = await client.apiCall('GET', '/v1/categories/' + parentId + '/sub-categories');
      // 하위가 있는지 한번 더 체크
      const results = [];
      for (const c of subs) {
        let hasChild = !!c.hasChildren;
        if (!hasChild) {
          try {
            const check = await client.apiCall('GET', '/v1/categories/' + c.id + '/sub-categories');
            hasChild = Array.isArray(check) && check.length > 0;
          } catch(e) {}
        }
        results.push({ id: String(c.id), name: c.name, hasChildren: hasChild });
      }
      res.json(results);
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/bulk/categories/search - 카테고리 이름 검색
app.get('/api/bulk/categories/search', async (req, res) => {
  try {
    const client = await getStoreAClient();
    const keyword = req.query.keyword || '';
    if (!keyword) return res.json([]);

    const data = await client.apiCall('GET', '/v1/categories?name=' + encodeURIComponent(keyword));
    const results = (Array.isArray(data) ? data : []).slice(0, 20).map(c => ({
      id: c.id,
      name: c.wholeCategoryName || c.name,
      hasChildren: c.hasChildren || false,
    }));
    res.json(results);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/bulk/next-sku - 다음 품번 조회
app.post('/api/bulk/next-sku', async (req, res) => {
  try {
    const { brand, category, count = 1 } = req.body;
    const year = new Date().getFullYear().toString().slice(-2);
    const br = (brand || 'XX').toUpperCase().slice(0, 2);
    // 카테고리 ID → 약어 변환
    const catIdToSku = {
      '50021299':'KN','50021319':'CD','50021261':'VT','50021260':'TN',
      '50000804':'BL','50000803':'TS','50000807':'OP','50000808':'SK',
      '50000810':'PT','50000567':'SF','50021360':'JK','50021419':'TC',
      '50021439':'SC','50021459':'LC','50021321':'PD','50021441':'OV',
    };
    const cat = (catIdToSku[category] || category || 'KN').toUpperCase().slice(0, 2);
    const prefix = `${year}${br}${cat}`;

    // sync_config에서 마지막 번호 조회
    const key = `bulk_sku_last_${prefix}`;
    const rows = await query('SELECT value FROM sync_config WHERE `key` = ?', [key]);
    let lastNum = rows.length > 0 ? parseInt(rows[0].value) || 0 : 0;

    const skus = [];
    for (let i = 0; i < count; i++) {
      lastNum++;
      skus.push(`${prefix}${String(lastNum).padStart(3, '0')}`);
    }

    // 마지막 번호 저장
    await query(
      "INSERT INTO sync_config (`key`, value, updated_at) VALUES (?, ?, NOW()) ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = NOW()",
      [key, String(lastNum)]
    );

    res.json({ skus, prefix, lastNum });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/bulk/generate-excel - 대량등록 엑셀 생성
app.post('/api/bulk/generate-excel', async (req, res) => {
  try {
    const { products } = req.body;
    if (!products || !Array.isArray(products) || products.length === 0) {
      return res.status(400).json({ error: '상품 데이터가 필요합니다.' });
    }

    // 템플릿 설정 읽기
    const configRows = await query("SELECT `key`, value FROM sync_config WHERE `key` LIKE 'bulk_%'");
    const cfg = {};
    for (const r of configRows) cfg[r.key] = r.value;

    // 원본 엑셀 템플릿 읽기
    const templatePath = path.join(__dirname, 'ExcelSaveTemplate_20260309.xlsx');
    const templateWb = XLSX.readFile(templatePath);
    const templateWs = templateWb.Sheets[templateWb.SheetNames[0]];
    const templateData = XLSX.utils.sheet_to_json(templateWs, { header: 1, defval: '' });

    // 행0: 그룹헤더, 행1: 컬럼명만 유지 (행2: 필수여부, 행3~5: 예시/도움말 → 제거)
    // 네이버 대량등록은 행1(컬럼명) 다음부터 데이터로 인식
    const headerRows = templateData.slice(0, 2);

    // 상품 데이터 행 생성
    const dataRows = [];
    for (const product of products) {
      const row = new Array(93).fill('');

      // 카테고리 매핑 (네이버 실제 카테고리 ID)
      const categoryMap = {
        // 패션의류 > 여성의류 > 니트
        'KN': '50021299',   // 니트 > 풀오버
        'CD': '50021319',   // 니트 > 카디건
        'VT': '50021261',   // 니트 > 베스트
        'TN': '50021260',   // 니트 > 터틀넥
        // 패션의류 > 여성의류
        'BL': '50000804',   // 블라우스/셔츠
        'TS': '50000803',   // 티셔츠
        'OP': '50000807',   // 원피스
        'SK': '50000808',   // 스커트
        'PT': '50000810',   // 바지
        // 패션잡화
        'SF': '50000567',   // 숄
      };

      // [0] 판매자 상품코드 (품번) — 자동 또는 수동
      row[0] = product.sku || '';
      // [1] 카테고리코드 — 필수
      const catCode = product.category || cfg.bulk_category_code || '50021299';
      row[1] = categoryMap[catCode] || catCode; // 약어면 변환, 숫자ID면 그대로
      // [2] 상품명 — 필수
      row[2] = product.name || '';
      // [3] 상품상태
      row[3] = '신상품';
      // [4] 판매가 — 필수
      row[4] = product.price || '';
      // [9] 부가세
      row[9] = cfg.bulk_tax_type || '과세상품';
      // [11] 재고수량 — 필수
      row[11] = product.qty || cfg.bulk_default_qty || 5;

      // 옵션 (컬러 + 사이즈 조합형)
      const colors = product.colors || [];
      const sizes = product.sizes || (cfg.bulk_default_sizes || 'Free').split(',').map(s => s.trim()).filter(Boolean);
      const totalQty = product.qty || cfg.bulk_default_qty || 5;

      if (colors.length > 0) {
        if (sizes.length > 0) {
          // 조합형: 색상 × 사이즈
          row[12] = '조합형';
          row[13] = '색상\n사이즈';
          row[14] = colors.join(',') + '\n' + sizes.join(',');
          const combCount = colors.length * sizes.length;
          const perCombQty = Math.max(1, Math.floor(totalQty / combCount));
          row[16] = Array(combCount).fill(perCombQty).join(',');
        } else {
          // 단독형: 색상만
          row[12] = '단독형';
          row[13] = '색상';
          row[14] = colors.join(',');
          const perColorQty = Math.max(1, Math.floor(totalQty / colors.length));
          row[16] = colors.map(() => perColorQty).join(',');
        }
      }

      // [22] 대표이미지 — 필수
      const imageUrls = product.imageUrls || [];
      if (imageUrls.length > 0) {
        row[22] = imageUrls[0];
      }
      // [23] 추가이미지 (줄바꿈 구분, 최대 9장)
      if (imageUrls.length > 1) {
        row[23] = imageUrls.slice(1, 10).join('\n');
      }

      // [24] 상세설명 — 이미지 나열 HTML 자동 생성
      const detailImages = imageUrls.map(url =>
        `<img src="${url}" style="max-width:860px;width:100%;" />`
      ).join('<br/>');
      row[24] = `<div style="text-align:center;max-width:860px;margin:0 auto;">${detailImages}</div>`;

      // [25] 브랜드, [26] 제조사 — 상품명에서 자동 추출
      const brand = extractBrand(product.name);
      row[25] = brand ? brand.toUpperCase() : (cfg.bulk_brand || '');
      row[26] = cfg.bulk_manufacturer || '상세설명참조';

      // [29] 원산지코드 — 필수
      row[29] = cfg.bulk_origin_code || '00';
      // [31] 복수원산지여부
      row[31] = 'N';
      // [33] 미성년자 구매
      row[33] = 'Y';

      // 배송 정보
      if (cfg.bulk_delivery_template_code) {
        row[34] = cfg.bulk_delivery_template_code;
      } else {
        row[35] = cfg.bulk_delivery_method || '택배, 소포, 등기';
        row[36] = cfg.bulk_courier_code || 'CJGLS';
        row[37] = cfg.bulk_delivery_fee_type || '무료';
        row[38] = cfg.bulk_delivery_fee || '';
        row[39] = cfg.bulk_delivery_payment || '';
        row[40] = cfg.bulk_free_condition || '';
        row[46] = cfg.bulk_return_fee || '4000';
        row[47] = cfg.bulk_exchange_fee || '8000';
      }
      row[49] = 'N'; // 별도설치비

      // 상품정보제공고시 — 템플릿코드 없으면 WEAR 타입 자동 입력
      if (cfg.bulk_product_info_code) {
        row[50] = cfg.bulk_product_info_code;
      } else {
        row[51] = '상품상세참조';  // 품명
        row[52] = '상품상세참조';  // 모델명
        row[53] = '상품상세참조';  // 인증허가사항
        row[54] = '상품상세참조';  // 제조자
      }

      // A/S 정보
      if (cfg.bulk_as_template_code) {
        row[55] = cfg.bulk_as_template_code;
      }
      row[56] = cfg.bulk_as_phone || '01046680439';
      row[57] = cfg.bulk_as_info || '상세 참조';

      // 즉시할인
      if (cfg.bulk_discount_value) {
        row[59] = cfg.bulk_discount_value;
        row[60] = cfg.bulk_discount_unit || '%';
        row[61] = cfg.bulk_discount_value;   // 모바일 동일
        row[62] = cfg.bulk_discount_unit || '%';
      }

      // 수입산인 경우 수입국/수입사 필수 (원산지코드 02xx = 수입산)
      const originCode = row[29] || '';
      if (originCode.startsWith('02') || originCode.startsWith('03')) {
        row[30] = cfg.bulk_importer || '상세설명참조';
        row[32] = cfg.bulk_origin_text || '';
      }

      dataRows.push(row);
    }

    // 엑셀 생성 (헤더 3행 + 데이터)
    const output = [...headerRows, ...dataRows];
    const ws = XLSX.utils.aoa_to_sheet(output);

    // 컬럼 너비 설정
    ws['!cols'] = [
      { wch: 15 }, { wch: 12 }, { wch: 30 }, { wch: 8 }, { wch: 10 },
      { wch: 6 }, { wch: 8 }, { wch: 6 }, { wch: 8 }, { wch: 8 },
      { wch: 10 }, { wch: 8 }, { wch: 8 }, { wch: 10 }, { wch: 20 },
      { wch: 10 }, { wch: 15 }, { wch: 12 }, { wch: 12 }, { wch: 15 },
      { wch: 10 }, { wch: 10 }, { wch: 40 }, { wch: 40 }, { wch: 50 },
    ];

    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, '일괄등록');
    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename=bulk_upload_${new Date().toISOString().slice(0,10)}.xlsx`);
    res.send(buffer);
  } catch (e) {
    console.error('[대량등록] 엑셀 생성 오류:', e);
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

// 쿠팡 vendorItemName 파싱: "ob 캐시미어 니트, 아이보리 free" → { productName, brand, color, size }
// 콤마 앞 = 상품명 (브랜드 이니셜 포함), 콤마 뒤 = 옵션 (색상 + 사이즈)
function parseCoupangItemName(vendorItemName) {
  if (!vendorItemName) return { productName: '', brand: '', color: '', size: '', colorOptions: [], sizeOptions: [] };

  const commaIdx = vendorItemName.indexOf(',');
  if (commaIdx === -1) {
    return { productName: vendorItemName.trim(), brand: extractBrand(vendorItemName), color: '', size: '', colorOptions: [], sizeOptions: [] };
  }

  let productName = vendorItemName.slice(0, commaIdx).trim();
  const optionPart = vendorItemName.slice(commaIdx + 1).trim();
  const tokens = optionPart.split(/\s+/).filter(t => t);

  // 브랜드: 상품명 앞 또는 끝에서 2글자 영문 이니셜 추출
  let brand = extractBrand(productName);
  if (!brand) {
    // 끝에 브랜드가 있는 경우: "... 블랙 ob" → brand = "ob" (상품명은 변경하지 않음)
    const endMatch = productName.match(/\s([a-zA-Z]{2})$/);
    if (endMatch) {
      brand = endMatch[1].toLowerCase();
    }
  }

  // 첫 토큰이 2글자 영문이면 옵션 쪽 브랜드 — 상품명에 없으면 prepend용
  let startIdx = 0;
  if (tokens.length > 0 && /^[a-zA-Z]{2}$/.test(tokens[0])) {
    const optionBrand = tokens[0].toLowerCase();
    if (!brand) {
      brand = optionBrand;
    }
    startIdx = 1;
  }

  // 마지막 토큰이 사이즈 키워드면 추출
  let size = '';
  let endIdx = tokens.length;
  if (tokens.length > startIdx && /^(free|xxl|xl|l|m|s|f)$/i.test(tokens[tokens.length - 1])) {
    size = tokens[tokens.length - 1];
    size = size.toUpperCase() === 'FREE' ? 'Free' : size.toUpperCase();
    endIdx = tokens.length - 1;
  }

  // 중간 토큰에서 색상/사이즈 분리 (사이즈 키워드가 섞여있을 수 있음)
  const middleTokens = tokens.slice(startIdx, endIdx);
  const sizePattern = /^(free|xxl|xl|l|m|s|f)$/i;
  const colorOptions = middleTokens.filter(t => !sizePattern.test(t));
  const extraSizes = middleTokens.filter(t => sizePattern.test(t)).map(t =>
    t.toUpperCase() === 'FREE' ? 'Free' : t.toUpperCase()
  );

  const color = middleTokens.join(' ');
  const allSizes = [...new Set([...(size ? [size] : []), ...extraSizes])];
  const sizeOptions = allSizes.length > 0 ? allSizes : [];

  return { productName, brand, color, size, colorOptions, sizeOptions };
}

function maskSecret(str) {
  if (!str || str.length <= 4) return '****';
  return str.slice(0, 4) + '****';
}

async function initSyncClients() {
  if (!scheduler.hasClients()) {
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

  // 쿠팡/지그재그 클라이언트도 scheduler에 전달 (아직 없으면)
  if (!scheduler.coupangClient) {
    try {
      const coupang = await initCoupangClient();
      if (coupang) {
        scheduler.setCoupangClient(coupang);
        console.log('[Sync] 쿠팡 클라이언트 연결됨');
      }
    } catch (e) { console.log('[Sync] 쿠팡 클라이언트 초기화 실패:', e.message); }
  }
  if (!scheduler.zigzagClient) {
    try {
      const zigzag = await initZigzagClient();
      if (zigzag) {
        scheduler.setZigzagClient(zigzag);
        console.log('[Sync] 지그재그 클라이언트 연결됨');
      }
    } catch (e) { console.log('[Sync] 지그재그 클라이언트 초기화 실패:', e.message); }
  }
}

async function initCoupangClient() {
  const getVal = async (key) => {
    const rows = await query('SELECT value FROM sync_config WHERE `key` = ?', [key]);
    return rows[0] ? rows[0].value : '';
  };
  const accessKey = process.env.COUPANG_ACCESS_KEY || await getVal('coupang_access_key');
  const secretKey = process.env.COUPANG_SECRET_KEY || await getVal('coupang_secret_key');
  const vendorId = process.env.COUPANG_VENDOR_ID || await getVal('coupang_vendor_id');
  if (!accessKey || !secretKey || !vendorId) return null;
  return new CoupangClient(accessKey, secretKey, vendorId);
}

async function initZigzagClient() {
  const getVal = async (key) => {
    const rows = await query('SELECT value FROM sync_config WHERE `key` = ?', [key]);
    return rows[0] ? rows[0].value : '';
  };
  const accessKey = process.env.ZIGZAG_ACCESS_KEY || await getVal('zigzag_access_key');
  const secretKey = process.env.ZIGZAG_SECRET_KEY || await getVal('zigzag_secret_key');
  if (!accessKey || !secretKey) return null;
  return new ZigzagClient(accessKey, secretKey);
}

// Initialize DB and start server
(async () => {
  app.listen(PORT, () => {
    console.log(`블루파이 재고관리 서버 실행중: http://localhost:${PORT}`);

    // 자동 인덱싱 시작 (6시간마다 새 상품 체크)
    startAutoIndexing();
  });

  // DB 초기화 (포트 열린 후 백그라운드)
  await initDb();

  // Auto-start scheduler if configured
  try {
    const enabled = await query("SELECT value FROM sync_config WHERE `key` = 'sync_enabled'");
    const interval = await query("SELECT value FROM sync_config WHERE `key` = 'sync_interval_minutes'");

    if (enabled[0] && enabled[0].value === 'true') {
      const intervalMin = parseInt(interval[0]?.value) || 5;
      console.log(`[Sync] 자동 시작 설정 감지: enabled=true, interval=${intervalMin}분`);
      try {
        await initSyncClients();
        await scheduler.start(intervalMin);
        console.log(`[Sync] 자동 시작 성공 — ${intervalMin}분 간격, 30초 후 첫 실행`);
      } catch (e) {
        console.log('[Sync] 자동 시작 실패 (API 키 미설정):', e.message);
      }
    } else {
      console.log('[Sync] 자동 시작 비활성화 (sync_enabled != true)');
    }
  } catch (e) {
    console.log('[Sync] 설정 확인 오류:', e.message);
  }

  // 앱 업데이트 감지 → 푸시 알림 (커밋 메시지 포함)
  try {
    const { execSync } = require('child_process');
    let currentVersion = null;
    try {
      currentVersion = execSync('git rev-parse HEAD', { encoding: 'utf8', cwd: __dirname }).trim();
    } catch (e) {
      // git 미설치 또는 .git 없는 환경
    }
    if (currentVersion) {
      const storedVersion = await scheduler.getConfig('app_version');
      if (storedVersion !== currentVersion) {
        const shortHash = currentVersion.slice(0, 7);
        let commitMsg = '';
        try {
          commitMsg = execSync('git log -1 --pretty=%s', { encoding: 'utf8', cwd: __dirname }).trim();
        } catch (e) {
          console.log('[Update] 커밋 메시지 조회 실패:', e.message);
        }
        const body = commitMsg
          ? `${commitMsg} (${shortHash})`
          : `새 버전으로 업데이트되었습니다. (${shortHash})`;
        console.log(`[Update] 새 버전 감지: ${storedVersion?.slice(0,7) || 'none'} → ${shortHash} — ${commitMsg || '(메시지 없음)'}`);
        try {
          await scheduler.sendPushNotification('앱 업데이트', body);
          console.log('[Update] 푸시 알림 발송 완료');
        } catch (pushErr) {
          console.error('[Update] 푸시 알림 발송 실패:', pushErr.message);
        }
      } else {
        console.log(`[Update] 버전 동일: ${currentVersion.slice(0,7)}`);
      }
      await scheduler.setConfig('app_version', currentVersion);
    } else {
      console.log('[Update] git 정보 없음 (버전 감지 스킵)');
    }
  } catch (e) {
    console.log('[Update] 버전 확인 오류:', e.message);
  }

})();
