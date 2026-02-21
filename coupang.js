const crypto = require('crypto');

const COUPANG_API_BASE = 'https://api-gateway.coupang.com';

class CoupangClient {
  constructor(accessKey, secretKey, vendorId, storeName = 'Coupang') {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.vendorId = vendorId;
    this.storeName = storeName;
  }

  // === HMAC-SHA256 인증 ===

  formatDatetime(date = new Date()) {
    const y = String(date.getUTCFullYear()).slice(2);
    const M = String(date.getUTCMonth() + 1).padStart(2, '0');
    const d = String(date.getUTCDate()).padStart(2, '0');
    const H = String(date.getUTCHours()).padStart(2, '0');
    const m = String(date.getUTCMinutes()).padStart(2, '0');
    const s = String(date.getUTCSeconds()).padStart(2, '0');
    return `${y}${M}${d}T${H}${m}${s}Z`;
  }

  generateSignature(method, fullPath, datetime) {
    // 쿠팡 HMAC: message = datetime + method + path + querystring (? 제외)
    const [path, qs] = fullPath.split('?');
    const message = datetime + method + path + (qs || '');
    return crypto
      .createHmac('sha256', this.secretKey)
      .update(message)
      .digest('hex');
  }

  buildAuthHeader(method, fullPath) {
    const datetime = this.formatDatetime();
    const signature = this.generateSignature(method, fullPath, datetime);
    return `CEA algorithm=HmacSHA256, access-key=${this.accessKey}, signed-date=${datetime}, signature=${signature}`;
  }

  // === API 호출 (429 재시도 포함) ===

  async apiCall(method, path, body = null, retryCount = 0) {
    const url = `${COUPANG_API_BASE}${path}`;
    const authorization = this.buildAuthHeader(method, path);

    const options = {
      method,
      headers: {
        'Authorization': authorization,
        'Content-Type': 'application/json;charset=UTF-8',
        'X-Requested-By': 'bluefi-inventory',
      },
    };
    if (body) {
      options.body = JSON.stringify(body);
    }

    const res = await fetch(url, options);

    if (res.status === 429 && retryCount < 3) {
      const delay = Math.pow(2, retryCount) * 1000;
      console.log(`[${this.storeName}] Rate limited, ${delay}ms 후 재시도...`);
      await this.sleep(delay);
      return this.apiCall(method, path, body, retryCount + 1);
    }

    if (!res.ok) {
      const errBody = await res.text();
      throw new Error(`[${this.storeName}] API 오류 (${res.status}): ${errBody.slice(0, 300)}`);
    }

    const text = await res.text();
    return text ? JSON.parse(text) : null;
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // 쿠팡 날짜 형식: yyyy-MM-dd (ISO → yyyy-MM-dd 변환)
  formatCoupangDate(isoDate) {
    const d = new Date(isoDate);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }

  // === 주문 조회 (아이템 단위 평탄화) ===

  async getOrderItems(fromDate, toDate) {
    const allItems = [];
    // 쿠팡 주문 상태별로 조회 (status 필수)
    const statuses = ['ACCEPT', 'INSTRUCT', 'DEPARTURE', 'DELIVERING', 'FINAL_DELIVERY', 'NONE_TRACKING'];
    const from = this.formatCoupangDate(fromDate);
    const to = this.formatCoupangDate(toDate);

    for (const status of statuses) {
      let nextToken = null;
      do {
        const params = new URLSearchParams({
          createdAtFrom: from,
          createdAtTo: to,
          status,
          maxPerPage: '50',
        });
        if (nextToken) {
          params.set('nextToken', nextToken);
        }

        const basePath = `/v2/providers/openapi/apis/api/v4/vendors/${this.vendorId}/ordersheets`;
        const fullPath = `${basePath}?${params.toString()}`;

        let data;
        try {
          data = await this.apiCall('GET', fullPath);
        } catch (e) {
          // 특정 상태에 주문 없으면 에러 무시
          console.log(`[${this.storeName}] ${status} 조회: ${e.message.slice(0, 100)}`);
          break;
        }
        if (!data || !data.data) break;

      for (const order of data.data) {
        const orderedAt = order.orderedAt || order.paidAt || toDate;
        const status = order.status || '';
        const shipmentBoxId = String(order.shipmentBoxId || '');

        if (order.orderItems && Array.isArray(order.orderItems)) {
          for (const item of order.orderItems) {
            const vendorItemId = String(item.vendorItemId || '');
            allItems.push({
              productOrderId: `CPG_${shipmentBoxId}_${vendorItemId}`,
              orderDate: new Date(orderedAt),
              productName: item.vendorItemName || '',
              optionName: item.sellerProductItemName || null,
              qty: item.shippingCount || 1,
              unitPrice: item.salesPrice?.units || 0,
              totalAmount: (item.salesPrice?.units || 0) * (item.shippingCount || 1),
              status,
              channelProductNo: vendorItemId,
            });
          }
        }
      }

        nextToken = data.nextToken || null;
        if (nextToken) await this.sleep(150);
      } while (nextToken);

      await this.sleep(100);
    }

    return allItems;
  }

  // === 연결 테스트 ===

  async testConnection() {
    try {
      const now = new Date();
      const from = this.formatCoupangDate(new Date(now.getTime() - 24 * 60 * 60 * 1000));
      const to = this.formatCoupangDate(now);
      const params = new URLSearchParams({
        createdAtFrom: from,
        createdAtTo: to,
        status: 'FINAL_DELIVERY',
        maxPerPage: '1',
      });
      const basePath = `/v2/providers/openapi/apis/api/v4/vendors/${this.vendorId}/ordersheets`;
      const fullPath = `${basePath}?${params.toString()}`;
      await this.apiCall('GET', fullPath);
      return { success: true, message: `${this.storeName} 연결 성공` };
    } catch (e) {
      return { success: false, message: e.message };
    }
  }
}

module.exports = { CoupangClient };
