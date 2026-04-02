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

  // 쿠팡 날짜 형식: yyyy-MM-dd (쿠팡 API는 KST 기준이므로 UTC→KST 변환)
  formatCoupangDate(isoDate) {
    const d = new Date(isoDate);
    const kst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
    const y = kst.getUTCFullYear();
    const m = String(kst.getUTCMonth() + 1).padStart(2, '0');
    const day = String(kst.getUTCDate()).padStart(2, '0');
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
          const rawOrderedAt = order.orderedAt || order.paidAt || toDate;
          // 쿠팡 API는 KST 기준 시간 반환 — timezone 표기 없으면 +09:00 추가
          const orderedAt = (typeof rawOrderedAt === 'string' && !rawOrderedAt.includes('Z') && !rawOrderedAt.includes('+'))
            ? rawOrderedAt + '+09:00'
            : rawOrderedAt;
          const orderStatus = order.status || '';
          const shipmentBoxId = String(order.shipmentBoxId || '');

          if (order.orderItems && Array.isArray(order.orderItems)) {
            for (const item of order.orderItems) {
              const vendorItemId = String(item.vendorItemId || '');
              const unitPrice = item.orderPrice || item.salesPrice || 0;
              const qty = item.shippingCount || 1;
              allItems.push({
                productOrderId: `CPG_${shipmentBoxId}_${vendorItemId}`,
                orderDate: new Date(orderedAt),
                productName: item.vendorItemName || '',
                optionName: item.sellerProductItemName || null,
                qty,
                unitPrice: Number(unitPrice) || 0,
                totalAmount: (Number(unitPrice) || 0) * qty,
                status: orderStatus,
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

  // === 반품 조회 ===

  async getReturnRequests(fromDate, toDate) {
    const allReturns = [];
    const from = this.formatCoupangDate(fromDate);
    const to = this.formatCoupangDate(toDate);
    // 쿠팡 반품 상태별 조회 (status 필수, 허용값: RU/CC/PR/UC)
    // RU=출고중지요청, UC=반품접수, CC=수거완료, PR=입고완료
    // 응답의 receiptStatus는 RETURNS_COMPLETED 등 상세값으로 반환됨
    const statuses = ['UC', 'CC', 'PR'];

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

        const basePath = `/v2/providers/openapi/apis/api/v4/vendors/${this.vendorId}/returnRequests`;
        const fullPath = `${basePath}?${params.toString()}`;

        let data;
        try {
          data = await this.apiCall('GET', fullPath);
        } catch (e) {
          console.log(`[${this.storeName}] 반품 ${status} 조회: ${e.message.slice(0, 500)}`);
          break;
        }
        if (!data || !data.data) break;

        for (const ret of data.data) {
          // 디버그: 전체 raw 데이터 로깅 (출고중지 건 구분 필드 확인용)
          if (allReturns.length < 3) {
            console.log(`[${this.storeName}] 반품 raw[${allReturns.length}]:`, JSON.stringify(ret, null, 0).slice(0, 1500));
          }
          allReturns.push({
            receiptId: ret.receiptId,
            orderId: ret.orderId,
            receiptStatus: ret.receiptStatus || status,
            releaseStatus: ret.releaseStatus || '',
            cancelCount: ret.cancelCount || 0,
            returnType: ret.returnType || '',
            buyerName: ret.buyerName || ret.ordererName || ret.requesterName || '',
            returnItems: (ret.returnItems || []).map(item => ({
              vendorItemId: String(item.vendorItemId || ''),
              vendorItemName: item.vendorItemName || '',
              returnQuantity: item.returnQuantity || 1,
              sellerProductItemName: item.sellerProductItemName || '',
              cancelCount: item.cancelCount || 0,
              purchaseCount: item.purchaseCount || 0,
              _raw: item,
            })),
            createdAt: ret.createdAt || '',
            _raw: ret,
          });
        }

        nextToken = data.nextToken || null;
        if (nextToken) await this.sleep(150);
      } while (nextToken);

      await this.sleep(100);
    }

    // receiptId 기준 중복 제거 (여러 status 쿼리에서 같은 건이 반환될 수 있음)
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

  // === 상품 등록/관리 ===

  /**
   * 상품 등록
   * @param {Object} productData - 쿠팡 상품 등록 데이터
   * @returns {Object} 등록 결과
   */
  async createProduct(productData) {
    const path = `/v2/providers/seller_api/apis/api/v1/marketplace/seller-products`;
    return this.apiCall('POST', path, productData);
  }

  /**
   * 상품 조회 (sellerProductId 기준)
   * @param {string} sellerProductId
   * @returns {Object} 상품 상세
   */
  async getProduct(sellerProductId) {
    const path = `/v2/providers/seller_api/apis/api/v1/marketplace/seller-products/${sellerProductId}`;
    return this.apiCall('GET', path);
  }

  /**
   * 카테고리별 메타정보 조회 (필수 속성, 공지사항 타입 등)
   * @param {number} categoryCode
   * @returns {Object} 카테고리 메타
   */
  async getCategoryMeta(categoryCode) {
    const path = `/v2/providers/seller_api/apis/api/v1/marketplace/meta/category-related-metas-by-categoryId?categoryId=${categoryCode}`;
    return this.apiCall('GET', path);
  }

  /**
   * A 스토어(네이버) 상품 데이터를 쿠팡 등록용으로 변환
   * @param {Object} sourceProduct - NaverCommerceClient.getChannelProduct() 결과
   * @param {Object} options - { vendorId, categoryCode, priceRate, outboundCode, returnCenterCode, namePrefix }
   * @returns {Object} 쿠팡 상품 등록 요청 body
   */
  static buildCoupangProductData(sourceProduct, options = {}) {
    const origin = sourceProduct.originProduct || sourceProduct;
    const baseName = origin.name || '';
    const {
      vendorId,
      categoryCode = 0,
      priceRate = 0.85,
      outboundCode = '',
      returnCenterCode = '',
      namePrefix = '',
    } = options;

    // 가격 계산
    let actualPrice = origin.salePrice || 0;
    const discount = origin.customerBenefit?.immediateDiscountPolicy?.discountMethod;
    if (discount) {
      if (discount.unitType === 'PERCENT') {
        actualPrice = Math.round(origin.salePrice * (1 - discount.value / 100));
      } else {
        actualPrice = origin.salePrice - (discount.value || 0);
      }
    }
    const salePrice = Math.floor(actualPrice * priceRate / 10) * 10;
    const originalPrice = Math.floor(actualPrice / 10) * 10;

    // 상품명
    const productName = namePrefix && !baseName.startsWith(namePrefix)
      ? `${namePrefix} ${baseName}` : baseName;

    // 이미지 추출 (네이버 → 쿠팡 형식)
    const images = [];
    if (origin.images) {
      if (origin.images.representativeImage?.url) {
        images.push({
          imageOrder: 0,
          imageType: 'REPRESENTATION',
          vendorPath: origin.images.representativeImage.url,
        });
      }
      if (origin.images.optionalImages) {
        origin.images.optionalImages.forEach((img, i) => {
          if (img.url) {
            images.push({
              imageOrder: i + 1,
              imageType: 'DETAIL',
              vendorPath: img.url,
            });
          }
        });
      }
    }

    // 기본 아이템 (옵션 없이 단일 상품)
    const item = {
      itemName: productName,
      originalPrice,
      salePrice,
      maximumBuyCount: 999,
      unitCount: 1,
      images: images.length > 0 ? images : undefined,
    };

    return {
      displayCategoryCode: categoryCode,
      sellerProductName: productName,
      vendorId: vendorId,
      brand: '',
      generalProductName: productName,
      productGroup: '',
      deliveryInfo: {
        deliveryType: 'NORMAL',
        deliveryAttributeType: 'NORMAL',
        deliveryCompanyCode: 'CJGLS',
        deliveryChargeType: 'FREE',
        deliveryCharge: 0,
        freeShipOverAmount: 0,
        deliveryChargeOnReturn: 5000,
        outboundShippingPlaceCode: outboundCode ? parseInt(outboundCode) : undefined,
        returnCenterCode: returnCenterCode || undefined,
      },
      returnCharge: 5000,
      items: [item],
      requiredDocuments: [],
      extraInfoMessage: '',
      manufacture: '',
      statusType: 'SALE',
    };
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
