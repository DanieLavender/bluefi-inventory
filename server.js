const express = require('express');
const path = require('path');
const { getDb, initDb } = require('./database');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// --- API Routes ---

// GET /api/inventory - 전체 재고 조회 (검색, 필터, 정렬, 페이지네이션)
app.get('/api/inventory', (req, res) => {
  const db = getDb();
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
    const countRow = db.prepare(`SELECT COUNT(*) as total FROM inventory ${where}`).get(...params);
    const total = countRow.total;

    // Sort
    let orderBy = 'ORDER BY id ASC';
    if (sort === 'name-asc') orderBy = 'ORDER BY name ASC';
    else if (sort === 'name-desc') orderBy = 'ORDER BY name DESC';
    else if (sort === 'qty-asc') orderBy = 'ORDER BY qty ASC';
    else if (sort === 'qty-desc') orderBy = 'ORDER BY qty DESC';
    else if (sort === 'color-asc') orderBy = 'ORDER BY color ASC';
    else if (sort === 'updated-desc') orderBy = 'ORDER BY updated_at DESC NULLS LAST';

    // Pagination
    const pageNum = parseInt(page) || 1;
    const pageSize = parseInt(limit) || 30;
    const offset = (pageNum - 1) * pageSize;

    const rows = db.prepare(
      `SELECT * FROM inventory ${where} ${orderBy} LIMIT ? OFFSET ?`
    ).all(...params, pageSize, offset);

    res.json({
      items: rows,
      total,
      page: pageNum,
      totalPages: Math.ceil(total / pageSize)
    });
  } finally {
    db.close();
  }
});

// GET /api/stats - 통계
app.get('/api/stats', (req, res) => {
  const db = getDb();
  try {
    const totalItems = db.prepare('SELECT COUNT(*) as cnt FROM inventory').get().cnt;
    const totalQty = db.prepare('SELECT COALESCE(SUM(qty), 0) as s FROM inventory').get().s;
    const brands = db.prepare("SELECT COUNT(DISTINCT brand) as cnt FROM inventory WHERE brand != ''").get().cnt;
    const outOfStock = db.prepare('SELECT COUNT(*) as cnt FROM inventory WHERE qty = 0').get().cnt;
    res.json({ totalItems, totalQty, brands, outOfStock });
  } finally {
    db.close();
  }
});

// GET /api/brands - 브랜드 목록
app.get('/api/brands', (req, res) => {
  const db = getDb();
  try {
    const rows = db.prepare("SELECT DISTINCT brand FROM inventory WHERE brand != '' ORDER BY brand").all();
    res.json(rows.map(r => r.brand));
  } finally {
    db.close();
  }
});

// POST /api/inventory - 재고 추가
app.post('/api/inventory', (req, res) => {
  const db = getDb();
  try {
    const { name, color, qty, brand: inputBrand } = req.body;
    if (!name || !color) {
      return res.status(400).json({ error: '상품명과 컬러는 필수입니다.' });
    }
    const brand = inputBrand || extractBrand(name);
    const result = db.prepare(
      'INSERT INTO inventory (name, color, qty, brand) VALUES (?, ?, ?, ?)'
    ).run(name.trim(), color.trim(), Math.max(0, parseInt(qty) || 0), brand);
    const item = db.prepare('SELECT * FROM inventory WHERE id = ?').get(result.lastInsertRowid);
    res.status(201).json(item);
  } finally {
    db.close();
  }
});

// PUT /api/inventory/:id - 수량 수정
app.put('/api/inventory/:id', (req, res) => {
  const db = getDb();
  try {
    const { id } = req.params;
    const { qty } = req.body;
    if (qty === undefined || qty === null) {
      return res.status(400).json({ error: '수량을 입력해주세요.' });
    }
    const result = db.prepare('UPDATE inventory SET qty = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?').run(Math.max(0, parseInt(qty) || 0), id);
    if (result.changes === 0) {
      return res.status(404).json({ error: '항목을 찾을 수 없습니다.' });
    }
    const item = db.prepare('SELECT * FROM inventory WHERE id = ?').get(id);
    res.json(item);
  } finally {
    db.close();
  }
});

// DELETE /api/inventory/:id - 단건 삭제
app.delete('/api/inventory/:id', (req, res) => {
  const db = getDb();
  try {
    const { id } = req.params;
    const result = db.prepare('DELETE FROM inventory WHERE id = ?').run(id);
    if (result.changes === 0) {
      return res.status(404).json({ error: '항목을 찾을 수 없습니다.' });
    }
    res.json({ success: true });
  } finally {
    db.close();
  }
});

// POST /api/inventory/delete-bulk - 일괄 삭제
app.post('/api/inventory/delete-bulk', (req, res) => {
  const db = getDb();
  try {
    const { ids } = req.body;
    if (!ids || !Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: '삭제할 항목을 선택해주세요.' });
    }
    const placeholders = ids.map(() => '?').join(',');
    const result = db.prepare(`DELETE FROM inventory WHERE id IN (${placeholders})`).run(...ids);
    res.json({ success: true, deleted: result.changes });
  } finally {
    db.close();
  }
});

function extractBrand(name) {
  if (!name) return '';
  const trimmed = name.trim();
  const match = trimmed.match(/^([a-zA-Z]{2})\s/);
  if (match) return match[1].toLowerCase();
  return '';
}

// Initialize DB and start server
initDb();
app.listen(PORT, () => {
  console.log(`블루파이 재고관리 서버 실행중: http://localhost:${PORT}`);
});
