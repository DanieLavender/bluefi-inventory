const crypto = require('crypto');
const webpush = require('web-push');
const { NaverCommerceClient } = require('./smartstore');
const { CoupangClient } = require('./coupang');
const { query } = require('./database');

class SyncScheduler {
  constructor() {
    this.storeA = null;
    this.storeB = null;
    this.intervalHandle = null;
    this.isRunning = false;
    this.lastRunResult = null;
    this.storeBDeliveryInfo = null;
  }

  // === Client initialization ===

  initClients(storeAId, storeASecret, storeBId, storeBSecret) {
    this.storeA = new NaverCommerceClient(storeAId, storeASecret, 'Store-A');
    this.storeB = new NaverCommerceClient(storeBId, storeBSecret, 'Store-B');
  }

  hasClients() {
    return !!(this.storeA && this.storeB);
  }

  // === Config helpers ===

  async getConfig(key) {
    const rows = await query('SELECT value FROM sync_config WHERE `key` = ?', [key]);
    return rows[0] ? rows[0].value : null;
  }

  async setConfig(key, value) {
    await query(
      'INSERT INTO sync_config (`key`, value, updated_at) VALUES (?, ?, NOW()) ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = NOW()',
      [key, value]
    );
  }

  // === Scheduler control ===

  async start(intervalMinutes) {
    if (this.intervalHandle) {
      clearInterval(this.intervalHandle);
    }
    const ms = (intervalMinutes || 5) * 60 * 1000;
    this.intervalHandle = setInterval(() => this.runSync(), ms);
    await this.setConfig('sync_enabled', 'true');
    await this.setConfig('sync_interval_minutes', String(intervalMinutes || 5));
    console.log(`[Sync] 스케줄러 시작 (${intervalMinutes}분 간격)`);
  }

  async stop() {
    if (this.intervalHandle) {
      clearInterval(this.intervalHandle);
      this.intervalHandle = null;
    }
    await this.setConfig('sync_enabled', 'false');
    console.log('[Sync] 스케줄러 중지');
  }

  async getStatus() {
    return {
      schedulerActive: !!this.intervalHandle,
      isRunning: this.isRunning,
      hasClients: this.hasClients(),
      lastSyncTime: await this.getConfig('last_sync_time') || null,
      syncEnabled: (await this.getConfig('sync_enabled')) === 'true',
      intervalMinutes: parseInt(await this.getConfig('sync_interval_minutes')) || 5,
      lastRunResult: this.lastRunResult,
    };
  }

  // === Main sync logic ===

  async runSync() {
    if (this.isRunning) {
      console.log('[Sync] 이미 실행 중, 스킵');
      return { skipped: true, message: '이미 실행 중입니다.' };
    }
    if (!this.hasClients()) {
      return { skipped: true, message: '스토어 API 키가 설정되지 않았습니다.' };
    }

    this.isRunning = true;
    const runId = crypto.randomUUID();
    const result = { runId, detected: 0, processed: 0, errors: 0, skipped: 0 };

    try {
      const lastSync = await this.getConfig('last_sync_time');
      const now = new Date();
      const from = lastSync ? new Date(lastSync) : new Date(now.getTime() - 24 * 60 * 60 * 1000);
      const fromStr = from.toISOString();
      const toStr = now.toISOString();

      console.log(`[Sync] 실행 시작 (${fromStr} ~ ${toStr})`);

      const returnedOrderIds = await this.storeA.getReturnedOrders(fromStr, toStr);
      result.detected = returnedOrderIds.length;

      const pendingRetry = await this.getPendingRetryOrders();
      if (pendingRetry.length > 0) {
        console.log(`[Sync] 이전 실패 건 ${pendingRetry.length}개 재시도`);
        for (const orderId of pendingRetry) {
          if (!returnedOrderIds.includes(orderId)) {
            returnedOrderIds.push(orderId);
          }
        }
        result.detected = returnedOrderIds.length;
      }

      if (returnedOrderIds.length === 0) {
        console.log('[Sync] 반품 관련 건 없음');
        await this.logSync(runId, 'return_detect', 'A', null, null, null,
          `조회 기간: ${fromStr.slice(0,16)} ~ ${toStr.slice(0,16)}`, null, 0, 'success', '반품 관련 건 없음');
        await this.setConfig('last_sync_time', toStr);
        this.lastRunResult = result;
        return result;
      }

      await this.logSync(runId, 'return_detect', 'A', null, null, null,
        `반품 완료 ${returnedOrderIds.length}건 감지`, null, returnedOrderIds.length, 'success');

      const batchSize = 50;
      const allDetails = [];
      for (let i = 0; i < returnedOrderIds.length; i += batchSize) {
        const batch = returnedOrderIds.slice(i, i + batchSize);
        const details = await this.storeA.getProductOrderDetail(batch);
        allDetails.push(...details);
        if (i + batchSize < returnedOrderIds.length) {
          await this.sleep(500);
        }
      }

      const failedOrderIds = [];
      for (const detail of allDetails) {
        try {
          await this.processReturnedItem(runId, detail);
          result.processed++;
          const orderId = detail.productOrderId || detail.productOrder?.productOrderId || '';
          await this.removePendingRetryOrder(orderId);
        } catch (e) {
          result.errors++;
          const orderId = detail.productOrderId || detail.productOrder?.productOrderId || '';
          if (orderId) failedOrderIds.push(orderId);
          const productName = this.extractProductName(detail);
          await this.logSync(runId, 'error', 'A', 'B',
            orderId, null, productName, null, 0, 'fail', e.message);
          console.error(`[Sync] 처리 오류: ${productName}`, e.message);
        }
        await this.sleep(500);
      }

      if (failedOrderIds.length > 0) {
        await this.addPendingRetryOrders(failedOrderIds);
        console.log(`[Sync] 실패 ${failedOrderIds.length}건 → 다음 실행 시 재시도`);
      }

      await this.setConfig('last_sync_time', toStr);

      // 매출 데이터 자동 수집
      try {
        await this.fetchSalesData();
      } catch (salesErr) {
        console.error('[Sync] 매출 수집 오류:', salesErr.message);
      }

      this.lastRunResult = result;
      console.log(`[Sync] 완료 — 감지: ${result.detected}, 처리: ${result.processed}, 오류: ${result.errors}`);
      return result;

    } catch (e) {
      result.errors++;
      await this.logSync(runId, 'error', 'A', null, null, null, null, null, 0, 'fail', e.message);
      console.error('[Sync] 전체 오류:', e.message);
      this.lastRunResult = result;
      return result;
    } finally {
      this.isRunning = false;
    }
  }

  // === Process a single returned item ===

  async processReturnedItem(runId, detail) {
    const productName = this.extractProductName(detail);
    const optionName = this.extractOptionName(detail);
    const qty = this.extractQty(detail);
    const productOrderId = detail.productOrderId || detail.productOrder?.productOrderId || '';
    const channelProductNo = this.extractChannelProductNo(detail);

    const safeOptionName = optionName || '';
    const rows = await query(
      'SELECT * FROM product_mapping WHERE store_a_channel_product_no = ? AND store_a_option_name = ?',
      [channelProductNo, safeOptionName]
    );
    const mapping = rows[0];

    if (mapping && mapping.match_status !== 'unmatched' && mapping.store_b_channel_product_no) {
      try {
        await this.increaseStoreB(runId, mapping.store_b_channel_product_no, productName, optionName, qty, productOrderId);
      } catch (e) {
        const isNotFound = e.message && (e.message.includes('404') || e.message.includes('not found') || e.message.includes('존재하지'));
        if (isNotFound) {
          console.log(`[Sync] B 상품 없음 (삭제됨?) → 매핑 초기화 후 신규 생성: ${productName}`);
          await this.resetMapping(channelProductNo, safeOptionName);
          await this.copyAndCreateInStoreB(runId, detail, channelProductNo, productName, optionName, qty, productOrderId);
        } else {
          throw e;
        }
      }
    } else {
      await this.copyAndCreateInStoreB(runId, detail, channelProductNo, productName, optionName, qty, productOrderId);
    }

    // B 스토어 처리 성공 후 → inventory 테이블에 재고 반영
    try {
      const invResult = await this.updateInventoryFromReturn(runId, productOrderId, channelProductNo, productName, optionName, qty);
      if (invResult && invResult.action !== 'skipped') {
        const invMsg = invResult.action === 'created'
          ? `신규 재고: ${invResult.name} (${invResult.color}) ${invResult.qty}개`
          : `재고 반영: ${productName} (${optionName || ''}) +${qty}개`;
        await this.sendPushNotification('재고 자동 반영', invMsg);
      }
    } catch (invErr) {
      console.error(`[Sync→Inventory] 재고 반영 실패 (무시): ${invErr.message}`);
    }
  }

  // === Update inventory from return ===

  async updateInventoryFromReturn(runId, productOrderId, channelProductNo, productName, optionName, qty) {
    const color = (optionName || '').trim();
    if (!productName) {
      console.log(`[Sync→Inventory] 상품명 없음, 스킵`);
      return null;
    }

    try {
      // 0단계: productOrderId 중복 체크 — 이미 처리된 반품이면 스킵
      if (productOrderId) {
        const dupCheck = await query(
          "SELECT id FROM sync_log WHERE type = 'inventory_update' AND product_order_id = ? AND status = 'success' LIMIT 1",
          [productOrderId]
        );
        if (dupCheck.length > 0) {
          console.log(`[Sync→Inventory] 이미 처리된 반품, 스킵: ${productOrderId}`);
          return { action: 'skipped', reason: 'already_processed' };
        }
      }

      // 스토어 상품명 정규화: [hm], (오늘출발) 등 제거
      const cleanName = productName
        .replace(/\[.*?\]/g, '')
        .replace(/\(.*?\)/g, '')
        .trim();

      // 브랜드 코드 추출 (상품명 앞 2글자 알파벳)
      const brandMatch = cleanName.match(/^([a-zA-Z]{2})\s/);
      const brand = brandMatch ? brandMatch[1].toLowerCase() : '';

      // 핵심 키워드 (브랜드 제거)
      const keywords = cleanName.replace(/^[a-zA-Z]{2}\s+/, '').trim();

      // 수동 반영 추정을 위한 last_sync_time
      const lastSyncTime = await this.getConfig('last_sync_time');

      // 1단계: channel_product_no로 직접 매칭 (이미 연결된 재고)
      if (channelProductNo) {
        let rows;
        if (color) {
          rows = await query(
            'SELECT * FROM inventory WHERE channel_product_no = ? AND color = ? LIMIT 1',
            [channelProductNo, color]
          );
        }
        if (!rows || rows.length === 0) {
          rows = await query(
            'SELECT * FROM inventory WHERE channel_product_no = ? LIMIT 1',
            [channelProductNo]
          );
        }
        if (rows.length > 0) {
          const item = rows[0];
          // 수동 반영 추정: last_sync_time 이후에 수정된 항목이면 스킵
          if (lastSyncTime && item.updated_at && new Date(item.updated_at) > new Date(lastSyncTime)) {
            await this.logSync(runId, 'inventory_update', null, null, productOrderId, channelProductNo,
              productName, optionName, qty, 'success',
              `수동 반영 추정 → ${item.name} (${item.color}) 이미 수정됨, 스킵`);
            console.log(`[Sync→Inventory] 수동 반영 추정, 스킵: ${item.name} (${item.color})`);
            return { action: 'skipped', reason: 'manual_update_detected', inventoryId: item.id };
          }
          const newQty = item.qty + qty;
          await query('UPDATE inventory SET qty = ?, updated_at = NOW() WHERE id = ?', [newQty, item.id]);
          await this.logSync(runId, 'inventory_update', null, null, productOrderId, channelProductNo,
            productName, optionName, qty, 'success',
            `직접 매칭 → ${item.name} (${item.color}) ${item.qty}→${newQty}`);
          console.log(`[Sync→Inventory] 직접 매칭: ${item.name} (${item.color}) ${item.qty}→${newQty}`);
          return { action: 'updated', matchType: 'direct', inventoryId: item.id, oldQty: item.qty, newQty };
        }
      }

      // 2단계: 텍스트 정확 매칭 (아직 연결 안 된 기존 재고)
      let rows = await query(
        'SELECT * FROM inventory WHERE name = ? AND color = ? LIMIT 1',
        [cleanName, color]
      );

      if (rows.length > 0) {
        const item = rows[0];
        // 수동 반영 추정
        if (lastSyncTime && item.updated_at && new Date(item.updated_at) > new Date(lastSyncTime)) {
          // 수량은 건드리지 않고 channel_product_no만 연결
          if (channelProductNo && !item.channel_product_no) {
            await query('UPDATE inventory SET channel_product_no = ? WHERE id = ?', [channelProductNo, item.id]);
          }
          await this.logSync(runId, 'inventory_update', null, null, productOrderId, channelProductNo,
            productName, optionName, qty, 'success',
            `수동 반영 추정 → ${item.name} (${item.color}) 이미 수정됨, 연결만 저장`);
          console.log(`[Sync→Inventory] 수동 반영 추정, 연결만 저장: ${item.name} (${item.color})`);
          return { action: 'skipped', reason: 'manual_update_detected', inventoryId: item.id };
        }
        const newQty = item.qty + qty;
        // 수량 증가 + channel_product_no 점진적 연결
        const updateSql = channelProductNo && !item.channel_product_no
          ? 'UPDATE inventory SET qty = ?, updated_at = NOW(), channel_product_no = ? WHERE id = ?'
          : 'UPDATE inventory SET qty = ?, updated_at = NOW() WHERE id = ?';
        const updateParams = channelProductNo && !item.channel_product_no
          ? [newQty, channelProductNo, item.id]
          : [newQty, item.id];
        await query(updateSql, updateParams);
        await this.logSync(runId, 'inventory_update', null, null, productOrderId, channelProductNo,
          productName, optionName, qty, 'success',
          `정확 매칭 → ${item.name} (${item.color}) ${item.qty}→${newQty}${!item.channel_product_no && channelProductNo ? ' [연결 저장]' : ''}`);
        console.log(`[Sync→Inventory] 정확 매칭: ${item.name} (${item.color}) ${item.qty}→${newQty}`);
        return { action: 'updated', matchType: 'exact', inventoryId: item.id, oldQty: item.qty, newQty };
      }

      // 3단계: 텍스트 유사 매칭
      if (keywords && color) {
        rows = await query(
          'SELECT * FROM inventory WHERE name LIKE ? AND color LIKE ? LIMIT 1',
          [`%${keywords}%`, `%${color}%`]
        );

        if (rows.length > 0) {
          const item = rows[0];
          // 수동 반영 추정
          if (lastSyncTime && item.updated_at && new Date(item.updated_at) > new Date(lastSyncTime)) {
            if (channelProductNo && !item.channel_product_no) {
              await query('UPDATE inventory SET channel_product_no = ? WHERE id = ?', [channelProductNo, item.id]);
            }
            await this.logSync(runId, 'inventory_update', null, null, productOrderId, channelProductNo,
              productName, optionName, qty, 'success',
              `수동 반영 추정 → ${item.name} (${item.color}) 이미 수정됨, 연결만 저장`);
            console.log(`[Sync→Inventory] 수동 반영 추정, 연결만 저장: ${item.name} (${item.color})`);
            return { action: 'skipped', reason: 'manual_update_detected', inventoryId: item.id };
          }
          const newQty = item.qty + qty;
          const updateSql = channelProductNo && !item.channel_product_no
            ? 'UPDATE inventory SET qty = ?, updated_at = NOW(), channel_product_no = ? WHERE id = ?'
            : 'UPDATE inventory SET qty = ?, updated_at = NOW() WHERE id = ?';
          const updateParams = channelProductNo && !item.channel_product_no
            ? [newQty, channelProductNo, item.id]
            : [newQty, item.id];
          await query(updateSql, updateParams);
          await this.logSync(runId, 'inventory_update', null, null, productOrderId, channelProductNo,
            productName, optionName, qty, 'success',
            `유사 매칭 → ${item.name} (${item.color}) ${item.qty}→${newQty}${!item.channel_product_no && channelProductNo ? ' [연결 저장]' : ''}`);
          console.log(`[Sync→Inventory] 유사 매칭: ${item.name} (${item.color}) ${item.qty}→${newQty}`);
          return { action: 'updated', matchType: 'fuzzy', inventoryId: item.id, oldQty: item.qty, newQty };
        }
      }

      // 4단계: 매칭 실패 → 신규 등록 (channel_product_no 포함)
      const newName = cleanName || productName.trim();
      const newBrand = brand || '';
      if (newBrand) {
        const existingBrands = await query("SELECT DISTINCT brand FROM inventory WHERE brand != ''");
        const brandSet = new Set(existingBrands.map(r => r.brand));
        if (!brandSet.has(newBrand)) {
          console.log(`[Sync→Inventory] 새 브랜드 등록: ${newBrand}`);
        }
      }

      const result = await query(
        'INSERT INTO inventory (name, color, qty, brand, channel_product_no) VALUES (?, ?, ?, ?, ?)',
        [newName, color || '기본', qty, newBrand, channelProductNo || null]
      );
      await this.logSync(runId, 'inventory_update', null, null, productOrderId, channelProductNo,
        productName, optionName, qty, 'success',
        `신규 등록 → ${newName} (${color || '기본'}) ${qty}개${newBrand ? ` [${newBrand}]` : ''}`);
      console.log(`[Sync→Inventory] 신규 등록: ${newName} (${color || '기본'}) ${qty}개`);
      return { action: 'created', inventoryId: result.insertId, name: newName, color: color || '기본', qty };

    } catch (e) {
      await this.logSync(runId, 'inventory_update', null, null, productOrderId, channelProductNo,
        productName, optionName, qty, 'fail', `재고 반영 오류: ${e.message}`);
      console.error(`[Sync→Inventory] 오류: ${productName}`, e.message);
      return null;
    }
  }

  // === Increase Store B stock ===

  async increaseStoreB(runId, storeBProductNo, productName, optionName, qty, productOrderId) {
    try {
      const fullResponse = await this.storeB.getChannelProduct(storeBProductNo);
      const originProduct = fullResponse.originProduct || fullResponse;
      const smartstoreCP = fullResponse.smartstoreChannelProduct || null;
      const currentStock = originProduct.stockQuantity || 0;
      const newStock = currentStock + qty;

      await this.storeB.updateProductStock(storeBProductNo, originProduct, newStock, smartstoreCP);

      await this.logSync(runId, 'qty_increase', 'A', 'B', productOrderId, storeBProductNo,
        productName, optionName, qty, 'success',
        `수량 ${currentStock} → ${newStock} (+${qty})`);

      console.log(`[Sync] B 수량 증가: ${productName} ${currentStock}→${newStock}`);
    } catch (e) {
      await this.logSync(runId, 'qty_increase', 'A', 'B', productOrderId, storeBProductNo,
        productName, optionName, qty, 'fail', e.message);
      throw e;
    }
  }

  // === Get Store B delivery info template ===

  async getStoreBDeliveryInfo(sourceDeliveryInfo) {
    if (this.storeBDeliveryInfo) return this.storeBDeliveryInfo;

    try {
      try {
        const results = await this.storeB.searchProducts('');
        if (results && results.length > 0) {
          const productNo = results[0].channelProductNo || results[0].id;
          if (productNo) {
            const product = await this.storeB.getChannelProduct(String(productNo));
            if (product?.originProduct?.deliveryInfo) {
              this.storeBDeliveryInfo = product.originProduct.deliveryInfo;
              console.log('[Sync] B 스토어 배송정보 캐시 완료 (기존 상품 참조)');
              return this.storeBDeliveryInfo;
            }
          }
        }
      } catch (e) {
        console.log('[Sync] B 스토어 상품 검색 실패:', e.message);
      }

      try {
        const addresses = await this.storeB.getDeliveryAddresses();
        if (addresses && (Array.isArray(addresses) ? addresses.length > 0 : addresses.data)) {
          const addrList = Array.isArray(addresses) ? addresses : (addresses.data || []);
          const addressId = addrList[0]?.id || addrList[0]?.addressId;
          if (addressId && sourceDeliveryInfo) {
            const di = JSON.parse(JSON.stringify(sourceDeliveryInfo));
            delete di.deliveryBundleGroupId;
            di.deliveryBundleGroupUsable = false;
            if (di.claimDeliveryInfo) {
              di.claimDeliveryInfo.shippingAddressId = addressId;
              di.claimDeliveryInfo.returnAddressId = addressId;
            }
            this.storeBDeliveryInfo = di;
            console.log(`[Sync] B 스토어 배송정보 구성 (주소 ID: ${addressId})`);
            return this.storeBDeliveryInfo;
          }
        }
      } catch (e) {
        console.log('[Sync] B 스토어 주소 API 조회 실패:', e.message);
      }

      const addressId = await this.getConfig('store_b_address_id');
      if (addressId && sourceDeliveryInfo) {
        const di = JSON.parse(JSON.stringify(sourceDeliveryInfo));
        delete di.deliveryBundleGroupId;
        di.deliveryBundleGroupUsable = false;
        if (di.claimDeliveryInfo) {
          di.claimDeliveryInfo.shippingAddressId = parseInt(addressId);
          di.claimDeliveryInfo.returnAddressId = parseInt(addressId);
        }
        this.storeBDeliveryInfo = di;
        console.log(`[Sync] B 스토어 배송정보 구성 (수동 주소 ID: ${addressId})`);
        return this.storeBDeliveryInfo;
      }
    } catch (e) {
      console.log('[Sync] B 스토어 배송정보 조회 실패:', e.message);
    }
    return null;
  }

  // === Copy product from Store A and create in Store B ===

  async copyAndCreateInStoreB(runId, detail, channelProductNo, productName, optionName, qty, productOrderId) {
    try {
      const po = detail.productOrder || detail;
      const productId = String(po.productId || channelProductNo || '');
      const originalProductId = String(po.originalProductId || '');

      let sourceProduct = null;
      if (productId) {
        try {
          sourceProduct = await this.storeA.getChannelProduct(productId);
        } catch (e) {
          console.log(`[Sync] 채널상품 조회 실패 (${productId}), 원상품으로 시도...`);
        }
      }
      if (!sourceProduct && originalProductId) {
        try {
          sourceProduct = await this.storeA.getOriginProduct(originalProductId);
        } catch (e) {
          console.log(`[Sync] 원상품 조회도 실패 (${originalProductId})`);
        }
      }
      if (!sourceProduct) {
        await this.logSync(runId, 'product_create', 'A', 'B', productOrderId, channelProductNo,
          productName, optionName, qty, 'fail', `A 스토어 상품 조회 실패 (productId=${productId}, originalProductId=${originalProductId})`);
        return;
      }

      const namePrefix = await this.getConfig('store_b_name_prefix') ?? '(오늘출발)';
      const copyData = NaverCommerceClient.buildProductCopyData(sourceProduct, qty, namePrefix);

      // B 스토어 상품 상태 설정 적용
      const bDisplayStatus = await this.getConfig('store_b_display_status') || 'ON';
      const bSaleStatus = await this.getConfig('store_b_sale_status') || 'SALE';
      copyData.smartstoreChannelProduct.channelProductDisplayStatusType = bDisplayStatus;
      copyData.originProduct.statusType = bSaleStatus;

      const optInfo = copyData.originProduct?.detailAttribute?.optionInfo;
      if (optInfo?.optionCombinations && optionName) {
        for (const opt of optInfo.optionCombinations) {
          const combinedName = [opt.optionName1, opt.optionName2, opt.optionName3]
            .filter(Boolean).join('/');
          if (combinedName === optionName || opt.optionName1 === optionName) {
            opt.stockQuantity = qty;
            break;
          }
        }
      }

      const sourceDelivery = sourceProduct?.originProduct?.deliveryInfo || sourceProduct?.deliveryInfo;
      const storeBDelivery = await this.getStoreBDeliveryInfo(sourceDelivery);
      if (storeBDelivery) {
        copyData.originProduct.deliveryInfo = JSON.parse(JSON.stringify(storeBDelivery));
        // A 스토어의 반품/교환 배송비를 B 상품에 반영 (캐시된 B 배송정보가 0일 수 있음)
        if (sourceDelivery?.claimDeliveryInfo) {
          if (!copyData.originProduct.deliveryInfo.claimDeliveryInfo) {
            copyData.originProduct.deliveryInfo.claimDeliveryInfo = {};
          }
          const src = sourceDelivery.claimDeliveryInfo;
          const dst = copyData.originProduct.deliveryInfo.claimDeliveryInfo;
          if (src.returnDeliveryFee != null) dst.returnDeliveryFee = src.returnDeliveryFee;
          if (src.exchangeDeliveryFee != null) dst.exchangeDeliveryFee = src.exchangeDeliveryFee;
        }
      }

      const created = await this.storeB.createProduct(copyData);
      console.log('[Sync] B 스토어 상품 생성 응답:', JSON.stringify(created).slice(0, 500));
      const newProductNo = created?.smartstoreChannelProductNo
        || created?.smartstoreChannelProduct?.channelProductNo
        || created?.channelProductNo
        || created?.originProductNo
        || '';

      if (newProductNo) {
        await this.sleep(1000);
        try {
          const bProduct = await this.storeB.getChannelProduct(String(newProductNo));
          const currentStock = bProduct?.originProduct?.stockQuantity || 0;
          if (currentStock !== qty) {
            console.log(`[Sync] B 재고 보정: ${currentStock} → ${qty}`);
            const bOrigin = bProduct.originProduct || bProduct;
            const bChannel = bProduct.smartstoreChannelProduct || null;
            await this.storeB.updateProductStock(String(newProductNo), bOrigin, qty, bChannel);
          }
        } catch (e) {
          console.log(`[Sync] B 재고 보정 실패 (무시): ${e.message}`);
        }
      }

      const storeBName = copyData.smartstoreChannelProduct?.channelProductName || productName;
      await this.saveMapping(channelProductNo, productName, optionName,
        String(newProductNo), storeBName, optionName, 'matched');

      await this.logSync(runId, 'product_create', 'A', 'B', productOrderId, String(newProductNo),
        productName, optionName, qty, 'success',
        `B 스토어에 신규 등록 완료 (수량: ${qty})`);

      console.log(`[Sync] B 스토어 신규 등록: ${productName} (수량: ${qty})`);

      // 푸시 알림
      await this.sendPushNotification('B스토어 신규 상품 등록', `${productName} (${optionName || '옵션없음'}) ${qty}개`);
    } catch (e) {
      await this.logSync(runId, 'product_create', 'A', 'B', productOrderId, channelProductNo,
        productName, optionName, qty, 'fail', e.message);
      console.error(`[Sync] B 스토어 상품 생성 오류: ${productName}`, e.message);
      throw e;
    }
  }

  // === Push notification ===

  async sendPushNotification(title, body) {
    try {
      const pub = await this.getConfig('vapid_public_key');
      const priv = await this.getConfig('vapid_private_key');
      if (!pub || !priv) return;

      webpush.setVapidDetails('mailto:bluefi@example.com', pub, priv);
      const subs = await query('SELECT * FROM push_subscriptions');
      if (subs.length === 0) return;

      const payload = JSON.stringify({ title, body });
      for (const sub of subs) {
        try {
          await webpush.sendNotification(
            { endpoint: sub.endpoint, keys: { p256dh: sub.p256dh, auth: sub.auth } },
            payload
          );
        } catch (e) {
          if (e.statusCode === 404 || e.statusCode === 410) {
            await query('DELETE FROM push_subscriptions WHERE id = ?', [sub.id]);
          }
        }
      }
      console.log(`[Push] ${subs.length}개 기기에 알림 발송`);
    } catch (e) {
      console.log(`[Push] 오류: ${e.message}`);
    }
  }

  // === Sales data fetch ===

  async fetchSalesData() {
    if (!this.hasClients()) return;
    let totalNewOrders = 0;
    let totalNewAmount = 0;

    const stores = [
      { key: 'A', client: this.storeA, configKey: 'sales_last_fetch_a' },
      { key: 'B', client: this.storeB, configKey: 'sales_last_fetch_b' },
    ];

    for (const { key, client, configKey } of stores) {
      try {
        const lastFetch = await this.getConfig(configKey);
        const now = new Date();
        const from = lastFetch ? new Date(lastFetch) : new Date(now.getTime() - 24 * 60 * 60 * 1000);

        let cursor = new Date(from);
        let inserted = 0;

        while (cursor < now) {
          const chunkEnd = new Date(Math.min(cursor.getTime() + 24 * 60 * 60 * 1000, now.getTime()));

          try {
            // lastChangedType 생략 → 모든 상태 변경 포함
            const orderIds = await client.getOrders(cursor.toISOString(), chunkEnd.toISOString());

            if (orderIds.length > 0) {
              const batchSize = 50;
              for (let i = 0; i < orderIds.length; i += batchSize) {
                const batch = orderIds.slice(i, i + batchSize);
                const details = await client.getProductOrderDetail(batch);

                for (const detail of details) {
                  const po = detail.productOrder || detail;
                  const order = detail.order || {};
                  const rawDate = order.paymentDate || order.orderDate || po.placeOrderDate || chunkEnd.toISOString();
                  const orderDate = new Date(rawDate).toISOString();
                  try {
                    const insertResult = await query(
                      `INSERT IGNORE INTO sales_orders (store, product_order_id, order_date, product_name, option_name, qty, unit_price, total_amount, product_order_status, channel_product_no)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                      [
                        key,
                        po.productOrderId || '',
                        orderDate,
                        po.productName || '',
                        po.optionName || null,
                        po.quantity || 1,
                        po.unitPrice || po.salePrice || 0,
                        po.totalPaymentAmount || po.totalProductAmount || ((po.unitPrice || 0) * (po.quantity || 1)),
                        po.productOrderStatus || '',
                        String(po.channelProductNo || po.productId || ''),
                      ]
                    );
                    if (insertResult.affectedRows > 0) inserted++;
                  } catch (dbErr) {
                    // duplicate ignored
                  }
                }

                if (i + batchSize < orderIds.length) await this.sleep(300);
              }
            }
          } catch (chunkErr) {
            console.log(`[Sales] Store ${key} 청크 오류:`, chunkErr.message);
          }

          cursor = chunkEnd;
          await this.sleep(300);
        }

        await this.setConfig(configKey, now.toISOString());
        if (inserted > 0) {
          totalNewOrders += inserted;
          console.log(`[Sales] Store ${key} 자동 수집: ${inserted}건`);
        }
      } catch (e) {
        console.error(`[Sales] Store ${key} 수집 오류:`, e.message);
      }
    }

    // 쿠팡 자동 수집
    try {
      const getVal = async (key) => {
        const rows = await query('SELECT value FROM sync_config WHERE `key` = ?', [key]);
        return rows[0] ? rows[0].value : '';
      };
      const cAccessKey = process.env.COUPANG_ACCESS_KEY || await getVal('coupang_access_key');
      const cSecretKey = process.env.COUPANG_SECRET_KEY || await getVal('coupang_secret_key');
      const cVendorId = process.env.COUPANG_VENDOR_ID || await getVal('coupang_vendor_id');

      if (cAccessKey && cSecretKey && cVendorId) {
        const coupang = new CoupangClient(cAccessKey, cSecretKey, cVendorId);
        const configKey = 'sales_last_fetch_c';
        const lastFetch = await this.getConfig(configKey);
        const now = new Date();
        const from = lastFetch ? new Date(lastFetch) : new Date(now.getTime() - 24 * 60 * 60 * 1000);

        let cursor = new Date(from);
        let inserted = 0;

        while (cursor < now) {
          const chunkEnd = new Date(Math.min(cursor.getTime() + 24 * 60 * 60 * 1000, now.getTime()));
          try {
            const items = await coupang.getOrderItems(cursor.toISOString(), chunkEnd.toISOString());
            for (const item of items) {
              try {
                const insertResult = await query(
                  `INSERT IGNORE INTO sales_orders (store, product_order_id, order_date, product_name, option_name, qty, unit_price, total_amount, product_order_status, channel_product_no)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                  ['C', item.productOrderId, item.orderDate, item.productName, item.optionName,
                   item.qty, item.unitPrice, item.totalAmount, item.status, item.channelProductNo]
                );
                if (insertResult.affectedRows > 0) inserted++;
              } catch (dbErr) { }
            }
          } catch (chunkErr) {
            console.log('[Sales] Coupang 청크 오류:', chunkErr.message);
          }
          cursor = chunkEnd;
          await this.sleep(300);
        }

        await this.setConfig(configKey, now.toISOString());
        if (inserted > 0) {
          totalNewOrders += inserted;
          console.log(`[Sales] Coupang 자동 수집: ${inserted}건`);
        }
      }
    } catch (e) {
      console.error('[Sales] Coupang 수집 오류:', e.message);
    }

    // 신규 매출 푸시 알림
    if (totalNewOrders > 0) {
      await this.sendPushNotification('신규 주문', `새 주문 ${totalNewOrders}건이 들어왔습니다`);
    }
  }

  // === Pending retry helpers ===

  async getPendingRetryOrders() {
    const raw = await this.getConfig('pending_retry_orders');
    if (!raw) return [];
    try { return JSON.parse(raw); } catch { return []; }
  }

  async addPendingRetryOrders(orderIds) {
    const existing = await this.getPendingRetryOrders();
    const merged = [...new Set([...existing, ...orderIds])];
    await this.setConfig('pending_retry_orders', JSON.stringify(merged));
  }

  async removePendingRetryOrder(orderId) {
    if (!orderId) return;
    const existing = await this.getPendingRetryOrders();
    const filtered = existing.filter(id => id !== orderId);
    if (filtered.length !== existing.length) {
      await this.setConfig('pending_retry_orders', JSON.stringify(filtered));
    }
  }

  // === DB helpers ===

  async saveMapping(storeANo, storeAName, storeAOption, storeBNo, storeBName, storeBOption, status) {
    const safeAOption = storeAOption || '';
    const safeBOption = storeBOption || '';
    await query(`
      INSERT INTO product_mapping (store_a_channel_product_no, store_a_product_name, store_a_option_name,
        store_b_channel_product_no, store_b_product_name, store_b_option_name, match_status, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
      ON DUPLICATE KEY UPDATE
        store_b_channel_product_no = VALUES(store_b_channel_product_no),
        store_b_product_name = VALUES(store_b_product_name),
        store_b_option_name = VALUES(store_b_option_name),
        match_status = VALUES(match_status),
        updated_at = NOW()
    `, [storeANo, storeAName, safeAOption, storeBNo, storeBName, safeBOption, status]);
  }

  async resetMapping(storeANo, storeAOption) {
    const safeAOption = storeAOption || '';
    await query(
      'DELETE FROM product_mapping WHERE store_a_channel_product_no = ? AND store_a_option_name = ?',
      [storeANo, safeAOption]
    );
    console.log(`[Sync] 매핑 삭제: A상품=${storeANo}, 옵션=${safeAOption || '(없음)'}`);
  }

  async saveUnmatchedMapping(storeANo, storeAName, storeAOption) {
    const safeAOption = storeAOption || '';
    await query(`
      INSERT IGNORE INTO product_mapping (store_a_channel_product_no, store_a_product_name, store_a_option_name, match_status)
      VALUES (?, ?, ?, 'unmatched')
    `, [storeANo, storeAName, safeAOption]);
  }

  async logSync(runId, type, storeFrom, storeTo, productOrderId, channelProductNo, productName, productOption, qty, status, message) {
    await query(`
      INSERT INTO sync_log (run_id, type, store_from, store_to, product_order_id,
        channel_product_no, product_name, product_option, qty, status, message)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [runId, type, storeFrom, storeTo, productOrderId, channelProductNo,
      productName, productOption, qty || 0, status, message || null]);
  }

  // === Data extraction helpers ===

  extractProductName(detail) {
    if (detail.productOrder) return detail.productOrder.productName || '';
    return detail.productName || '';
  }

  extractOptionName(detail) {
    if (detail.productOrder) return detail.productOrder.optionName || null;
    return detail.optionName || null;
  }

  extractQty(detail) {
    if (detail.productOrder) return detail.productOrder.quantity || 1;
    return detail.quantity || 1;
  }

  extractChannelProductNo(detail) {
    const po = detail.productOrder || detail;
    return String(po.channelProductNo || po.productId || po.originalProductId || '');
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

const scheduler = new SyncScheduler();

module.exports = { scheduler };
