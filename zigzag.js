class ZigzagClient {
  constructor(accessKey, secretKey, storeName = 'Zigzag') {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.storeName = storeName;
    this.graphqlUrl = 'https://zigzag.kr/_openapi/graphql';
  }

  // === 인증 헤더 ===

  buildAuthHeaders() {
    return {
      'Content-Type': 'application/json',
      'x-zigzag-access-key': this.accessKey,
      'x-zigzag-secret-key': this.secretKey,
    };
    // 대안 헤더 (위 실패 시):
    // 'Authorization': `Bearer ${this.accessKey}`, 'x-api-key': this.secretKey
    // 'access-key': this.accessKey, 'secret-key': this.secretKey
  }

  // === GraphQL API 호출 (429 재시도 + GraphQL 에러 체크) ===

  async apiCall(queryStr, variables = {}, retryCount = 0) {
    const res = await fetch(this.graphqlUrl, {
      method: 'POST',
      headers: this.buildAuthHeaders(),
      body: JSON.stringify({ query: queryStr, variables }),
    });

    if (res.status === 429 && retryCount < 3) {
      const delay = Math.pow(2, retryCount) * 1000;
      console.log(`[${this.storeName}] Rate limited, ${delay}ms 후 재시도...`);
      await this.sleep(delay);
      return this.apiCall(queryStr, variables, retryCount + 1);
    }

    if (!res.ok) {
      const errBody = await res.text();
      throw new Error(`[${this.storeName}] API 오류 (${res.status}): ${errBody.slice(0, 300)}`);
    }

    const json = await res.json();

    if (json.errors && json.errors.length > 0) {
      const msg = json.errors.map(e => e.message).join('; ');
      throw new Error(`[${this.storeName}] GraphQL 오류: ${msg.slice(0, 300)}`);
    }

    return json.data;
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // ISO → YYYYMMDD (KST 기준)
  formatYmd(isoDate) {
    const d = new Date(isoDate);
    const kst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
    const y = kst.getUTCFullYear();
    const m = String(kst.getUTCMonth() + 1).padStart(2, '0');
    const day = String(kst.getUTCDate()).padStart(2, '0');
    return `${y}${m}${day}`;
  }

  // === 주문(매출) 조회 ===

  async getOrderItems(fromDate, toDate) {
    const allItems = [];
    const dateFrom = this.formatYmd(fromDate);
    const dateTo = this.formatYmd(toDate);

    const query = `query($date_ymd_from: String, $date_ymd_to: String, $limit_count: Int, $skip_count: Int) {
      order_item_list(date_ymd_from: $date_ymd_from, date_ymd_to: $date_ymd_to,
        limit_count: $limit_count, skip_count: $skip_count) {
        total_count
        item_list {
          order_item_number
          quantity
          unit_price
          product_name
          option_name
          product_id
          status
          order {
            order_number
            date_created
            date_paid
            orderer { name }
          }
        }
      }
    }`;

    let skip = 0;
    const limit = 50;

    do {
      let data;
      try {
        data = await this.apiCall(query, {
          date_ymd_from: dateFrom,
          date_ymd_to: dateTo,
          limit_count: limit,
          skip_count: skip,
        });
      } catch (e) {
        console.log(`[${this.storeName}] 주문 조회 오류 (skip=${skip}): ${e.message.slice(0, 200)}`);
        break;
      }

      const result = data && data.order_item_list;
      if (!result || !result.item_list || result.item_list.length === 0) break;

      for (const item of result.item_list) {
        const order = item.order || {};
        const orderNumber = order.order_number || '';
        const itemNumber = item.order_item_number || '';
        const rawDate = order.date_paid || order.date_created || toDate;
        const qty = item.quantity || 1;
        const unitPrice = Number(item.unit_price) || 0;

        allItems.push({
          productOrderId: `ZZG_${orderNumber}_${itemNumber}`,
          orderDate: new Date(rawDate),
          productName: item.product_name || '',
          optionName: item.option_name || null,
          qty,
          unitPrice,
          totalAmount: unitPrice * qty,
          status: item.status || '',
          channelProductNo: String(item.product_id || ''),
        });
      }

      const totalCount = result.total_count || 0;
      skip += limit;
      if (skip >= totalCount) break;
      await this.sleep(150);
    } while (true);

    return allItems;
  }

  // === 반품 조회 ===

  async getReturnRequests(fromDate, toDate) {
    const allReturns = [];
    const dateFrom = this.formatYmd(fromDate);
    const dateTo = this.formatYmd(toDate);

    const query = `query($date_requested_ymd_from: String, $date_requested_ymd_to: String,
      $request_type: OrderItemRequestType, $limit_count: Int, $skip_count: Int) {
      requested_order_item_list(date_requested_ymd_from: $date_requested_ymd_from,
        date_requested_ymd_to: $date_requested_ymd_to, request_type: $request_type,
        limit_count: $limit_count, skip_count: $skip_count) {
        total_count
        item_list {
          order_item_number
          quantity
          unit_price
          product_name
          option_name
          product_id
          status
          order {
            order_number
            date_created
            orderer { name }
          }
          active_request_list {
            order_item_request_number
            type
            status
            requested_quantity
            date_requested
          }
        }
      }
    }`;

    let skip = 0;
    const limit = 50;

    do {
      let data;
      try {
        data = await this.apiCall(query, {
          date_requested_ymd_from: dateFrom,
          date_requested_ymd_to: dateTo,
          request_type: 'RETURN',
          limit_count: limit,
          skip_count: skip,
        });
      } catch (e) {
        console.log(`[${this.storeName}] 반품 조회 오류 (skip=${skip}): ${e.message.slice(0, 200)}`);
        break;
      }

      const result = data && data.requested_order_item_list;
      if (!result || !result.item_list || result.item_list.length === 0) break;

      if (allReturns.length === 0) {
        console.log(`[${this.storeName}] 반품 첫 건 키:`, Object.keys(result.item_list[0]).join(', '));
      }

      for (const item of result.item_list) {
        const order = item.order || {};
        const requests = item.active_request_list || [];
        const primaryReq = requests[0] || {};

        allReturns.push({
          receiptId: primaryReq.order_item_request_number || item.order_item_number || '',
          orderId: order.order_number || '',
          receiptStatus: primaryReq.status || item.status || '',
          buyerName: (order.orderer && order.orderer.name) || '',
          returnItems: [{
            vendorItemId: String(item.product_id || ''),
            vendorItemName: item.product_name || '',
            returnQuantity: primaryReq.requested_quantity || item.quantity || 1,
            sellerProductItemName: item.option_name || '',
            _raw: item,
          }],
          createdAt: primaryReq.date_requested || order.date_created || '',
        });
      }

      const totalCount = result.total_count || 0;
      skip += limit;
      if (skip >= totalCount) break;
      await this.sleep(150);
    } while (true);

    // receiptId 기준 중복 제거
    const seen = new Set();
    const unique = allReturns.filter(r => {
      if (seen.has(r.receiptId)) return false;
      seen.add(r.receiptId);
      return true;
    });
    if (unique.length < allReturns.length) {
      console.log(`[${this.storeName}] 반품 중복 제거: ${allReturns.length}건 → ${unique.length}건`);
    }
    return unique;
  }

  // === 연결 테스트 (여러 URL + 헤더 조합 시도) ===

  async testConnection() {
    const urls = [
      'https://api.zigzag.kr/graphql',
      'https://api.kakaostyle.com/graphql',
      'https://partners.kakaostyle.com/graphql',
      'https://openapi.zigzag.kr/graphql',
      'https://zigzag.kr/_openapi/graphql',
    ];
    const headerSets = [
      { 'Content-Type': 'application/json', 'zigzag-access-key': this.accessKey, 'zigzag-secret-key': this.secretKey },
      { 'Content-Type': 'application/json', 'x-zigzag-access-key': this.accessKey, 'x-zigzag-secret-key': this.secretKey },
      { 'Content-Type': 'application/json', 'access-key': this.accessKey, 'secret-key': this.secretKey },
    ];

    const testQuery = JSON.stringify({ query: 'query { shop { shop_id shop_name } }' });
    const errors = [];

    for (const url of urls) {
      for (const headers of headerSets) {
        try {
          const res = await fetch(url, { method: 'POST', headers, body: testQuery });
          const status = res.status;
          if (status === 404 || status === 403) continue; // 해당 URL/헤더 조합 아님

          const text = await res.text();
          let json;
          try { json = JSON.parse(text); } catch { continue; }

          if (json.data && json.data.shop) {
            // 성공! URL + 헤더 저장
            this.graphqlUrl = url;
            const hdrKeys = Object.keys(headers).filter(k => k !== 'Content-Type');
            const shop = json.data.shop;
            return { success: true, message: `${this.storeName} 연결 성공 (${shop.shop_name || shop.shop_id}) [${url}]`, url, headerKeys: hdrKeys };
          }
          if (json.data) {
            this.graphqlUrl = url;
            return { success: true, message: `${this.storeName} 연결 성공 [${url}]`, url };
          }
          if (json.errors) {
            // GraphQL 응답은 오지만 에러 → URL은 맞음, 쿼리/인증 문제
            errors.push(`${url} (${Object.keys(headers).filter(k=>k!=='Content-Type').join(',')}): ${json.errors[0]?.message || JSON.stringify(json.errors).slice(0,100)}`);
          }
        } catch (e) {
          errors.push(`${url}: ${e.message.slice(0, 80)}`);
        }
      }
    }
    return { success: false, message: `모든 URL/헤더 조합 실패:\n${errors.join('\n')}` };
  }
}

module.exports = { ZigzagClient };
