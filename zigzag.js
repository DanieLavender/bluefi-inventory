const crypto = require('crypto');

const ZIGZAG_API_URL = 'https://openapi.zigzag.kr/1/graphql';

class ZigzagClient {
  constructor(accessKey, secretKey, storeName = 'Zigzag') {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.storeName = storeName;
  }

  // === HMAC 인증 (CEA 방식) ===

  buildAuthorization(queryStr) {
    // signed-date: 밀리초 단위 UTC 타임스탬프
    const signedDate = String(Date.now());

    // 쿼리 정규화: 연속 공백 → 단일 공백
    const normalizedQuery = queryStr.replace(/\s+/g, ' ').trim();

    // 메시지: signedDate.normalizedQuery
    const message = signedDate + '.' + normalizedQuery;

    // HMAC-SHA1 서명 (공식 문서 Python 예제 기준)
    const signature = crypto
      .createHmac('sha1', this.secretKey)
      .update(message, 'utf-8')
      .digest('hex');

    return `CEA algorithm=HmacSHA256, access-key=${this.accessKey}, signed-date=${signedDate}, signature=${signature}`;
  }

  // === GraphQL API 호출 (429 재시도 + GraphQL 에러 체크) ===

  async apiCall(queryStr, variables = {}, retryCount = 0) {
    const body = JSON.stringify({ query: queryStr, variables });
    // 서명 대상은 GraphQL 쿼리 문자열 (JSON body가 아님)
    const authorization = this.buildAuthorization(queryStr);

    const res = await fetch(ZIGZAG_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': authorization,
      },
      body,
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

  // ISO → YYYYMMDD 정수 (KST 기준, 스키마 타입 Int)
  formatYmd(isoDate) {
    const d = new Date(isoDate);
    const kst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
    const y = kst.getUTCFullYear();
    const m = String(kst.getUTCMonth() + 1).padStart(2, '0');
    const day = String(kst.getUTCDate()).padStart(2, '0');
    return parseInt(`${y}${m}${day}`, 10);
  }

  // CrTimestamp (Unix timestamp) → Date 변환
  parseTimestamp(ts) {
    if (!ts) return null;
    // 초 단위 vs 밀리초 단위 판별 (2000년 이전 = 초 단위)
    const num = Number(ts);
    if (num < 1e12) return new Date(num * 1000);
    return new Date(num);
  }

  // === 주문(매출) 조회 ===

  async getOrderItems(fromDate, toDate) {
    const allItems = [];
    const dateFrom = this.formatYmd(fromDate);
    const dateTo = this.formatYmd(toDate);

    const query = `query($date_ymd_from: Int, $date_ymd_to: Int, $limit_count: Int, $skip_count: Int) {
      order_item_list(date_ymd_from: $date_ymd_from, date_ymd_to: $date_ymd_to,
        limit_count: $limit_count, skip_count: $skip_count) {
        total_count
        item_list {
          order_item_number
          quantity
          unit_price
          product_id
          status
          product_info { name options }
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
        const pinfo = item.product_info || {};
        const orderNumber = order.order_number || '';
        const itemNumber = item.order_item_number || '';
        const rawDate = order.date_paid || order.date_created;
        const qty = item.quantity || 1;
        const unitPrice = Number(item.unit_price) || 0;

        allItems.push({
          productOrderId: `ZZG_${orderNumber}_${itemNumber}`,
          orderDate: rawDate ? this.parseTimestamp(rawDate) : new Date(toDate),
          productName: pinfo.name || '',
          optionName: pinfo.options || null,
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

    const query = `query($date_requested_ymd_from: Int, $date_requested_ymd_to: Int,
      $request_type: OrderItemRequestType, $limit_count: Int, $skip_count: Int) {
      requested_order_item_list(date_requested_ymd_from: $date_requested_ymd_from,
        date_requested_ymd_to: $date_requested_ymd_to, request_type: $request_type,
        limit_count: $limit_count, skip_count: $skip_count) {
        total_count
        item_list {
          order_item_number
          quantity
          unit_price
          product_id
          status
          product_info { name options }
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
        const pinfo = item.product_info || {};
        const requests = item.active_request_list || [];
        const primaryReq = requests[0] || {};

        allReturns.push({
          receiptId: item.order_item_number || primaryReq.order_item_request_number || '',
          orderId: order.order_number || '',
          receiptStatus: primaryReq.status || item.status || '',
          buyerName: (order.orderer && order.orderer.name) || '',
          returnItems: [{
            vendorItemId: String(item.product_id || ''),
            vendorItemName: pinfo.name || '',
            returnQuantity: primaryReq.requested_quantity || item.quantity || 1,
            sellerProductItemName: pinfo.options || '',
            _raw: item,
          }],
          createdAt: this.parseTimestamp(primaryReq.date_requested || order.date_created) || '',
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

  // === 연결 테스트 ===

  async testConnection() {
    try {
      const data = await this.apiCall('query { shop { shop_id shop_name } }');
      const shop = data && data.shop;
      if (shop) {
        return { success: true, message: `${this.storeName} 연결 성공 (${shop.shop_name || shop.shop_id})` };
      }
      return { success: true, message: `${this.storeName} 연결 성공` };
    } catch (e) {
      return { success: false, message: e.message };
    }
  }
}

module.exports = { ZigzagClient };
