const bcrypt = require('bcryptjs');

const COMMERCE_API_BASE = 'https://api.commerce.naver.com/external';

class NaverCommerceClient {
  constructor(clientId, clientSecret, storeName = '') {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.storeName = storeName;
    this.accessToken = null;
    this.tokenExpiry = 0;
  }

  // === Authentication ===

  async getToken() {
    // Reuse token if still valid (1 min buffer)
    if (this.accessToken && Date.now() < this.tokenExpiry - 60000) {
      return this.accessToken;
    }

    const timestamp = Date.now();
    // bcrypt signature: password = clientId + _ + timestamp, salt = clientSecret
    const password = `${this.clientId}_${timestamp}`;
    const hashed = bcrypt.hashSync(password, this.clientSecret);
    const signature = Buffer.from(hashed).toString('base64');

    const params = new URLSearchParams({
      client_id: this.clientId,
      timestamp: timestamp.toString(),
      client_secret_sign: signature,
      grant_type: 'client_credentials',
      type: 'SELF',
    });

    const res = await fetch(`${COMMERCE_API_BASE}/v1/oauth2/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: params.toString(),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`[${this.storeName}] 토큰 발급 실패 (${res.status}): ${body}`);
    }

    const data = await res.json();
    this.accessToken = data.access_token;
    // expires_in is in seconds
    this.tokenExpiry = Date.now() + (data.expires_in || 600) * 1000;
    return this.accessToken;
  }

  // === Common API caller with retry ===

  async apiCall(method, path, body = null, retryCount = 0) {
    const token = await this.getToken();
    const url = `${COMMERCE_API_BASE}${path}`;

    const options = {
      method,
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
    };
    if (body) {
      options.body = JSON.stringify(body);
    }

    const res = await fetch(url, options);

    // 401 → refresh token and retry once
    if (res.status === 401 && retryCount < 1) {
      this.accessToken = null;
      this.tokenExpiry = 0;
      return this.apiCall(method, path, body, retryCount + 1);
    }

    // 429 → exponential backoff (max 3 retries)
    if (res.status === 429 && retryCount < 3) {
      const delay = Math.pow(2, retryCount) * 1000;
      console.log(`[${this.storeName}] Rate limited, retrying in ${delay}ms...`);
      await this.sleep(delay);
      return this.apiCall(method, path, body, retryCount + 1);
    }

    if (!res.ok) {
      const errBody = await res.text();
      throw new Error(`[${this.storeName}] API 오류 (${res.status}) ${method} ${path}: ${errBody}`);
    }

    const text = await res.text();
    return text ? JSON.parse(text) : null;
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // === Store A: Return order queries ===

  /**
   * Get returned orders in a time range
   * @param {string} fromDate ISO datetime
   * @param {string} toDate ISO datetime
   * @returns {Array} product order IDs with RETURNED status
   */
  async getReturnedOrders(fromDate, toDate) {
    // 여러 lastChangedType으로 조회하여 반품 관련 건 모두 수집
    const typesToCheck = ['CLAIM_REQUESTED', 'COLLECT_DONE', 'CLAIM_COMPLETED'];
    const allStatuses = [];

    for (const changeType of typesToCheck) {
      try {
        const params = new URLSearchParams({
          lastChangedFrom: fromDate,
          lastChangedTo: toDate,
          lastChangedType: changeType,
        });

        const data = await this.apiCall(
          'GET',
          `/v1/pay-order/seller/product-orders/last-changed-statuses?${params}`
        );

        if (data?.data?.lastChangeStatuses) {
          for (const s of data.data.lastChangeStatuses) {
            s._queriedType = changeType; // 디버깅용
            allStatuses.push(s);
          }
        }
      } catch (e) {
        console.log(`[${this.storeName}] ${changeType} 조회 오류 (무시):`, e.message);
      }
      await this.sleep(300);
    }

    // 디버깅: 전체 응답 로깅
    if (allStatuses.length > 0) {
      console.log(`[${this.storeName}] 클레임 변경 건 ${allStatuses.length}개 감지:`);
      allStatuses.forEach(s => {
        console.log(`  - queriedType=${s._queriedType} productOrderStatus=${s.productOrderStatus} claimType=${s.claimType} claimStatus=${s.claimStatus} orderId=${s.productOrderId}`);
      });
    }

    // 반품 완료 건만 필터: claimType=RETURN & claimStatus=RETURN_DONE
    const returnStatuses = allStatuses.filter(s => {
      const claimType = (s.claimType || '').toUpperCase();
      const claimStatus = (s.claimStatus || '').toUpperCase();
      const orderStatus = (s.productOrderStatus || '').toUpperCase();
      // 반품 완료 (RETURN_DONE)
      return claimType === 'RETURN' && (claimStatus === 'RETURN_DONE' || orderStatus === 'RETURNED');
    });

    // 중복 제거
    const seen = new Set();
    return returnStatuses
      .filter(s => {
        if (seen.has(s.productOrderId)) return false;
        seen.add(s.productOrderId);
        return true;
      })
      .map(s => s.productOrderId);
  }

  /**
   * Get product order details by IDs
   * @param {string[]} productOrderIds
   * @returns {Array} order detail objects
   */
  async getProductOrderDetail(productOrderIds) {
    if (!productOrderIds || productOrderIds.length === 0) return [];

    const data = await this.apiCall(
      'POST',
      '/v1/pay-order/seller/product-orders/query',
      { productOrderIds }
    );

    if (!data || !data.data) return [];
    return data.data;
  }

  // === Orders: All status changes query ===

  /**
   * 모든 상태 변경 주문 조회 (max 24h per call)
   * lastChangedType 생략 → PAYED/DELIVERING/DELIVERED/PURCHASE_DECIDED 모두 포함
   * @param {string} fromDate ISO datetime
   * @param {string} toDate ISO datetime
   * @returns {Array} product order IDs (deduplicated)
   */
  async getOrders(fromDate, toDate) {
    const params = new URLSearchParams({
      lastChangedFrom: fromDate,
      lastChangedTo: toDate,
    });

    const data = await this.apiCall(
      'GET',
      `/v1/pay-order/seller/product-orders/last-changed-statuses?${params}`
    );

    const statuses = data?.data?.lastChangeStatuses || [];
    if (statuses.length > 0) {
      // 상태별 분포 로깅
      const dist = {};
      for (const s of statuses) dist[s.productOrderStatus] = (dist[s.productOrderStatus] || 0) + 1;
      console.log(`[${this.storeName}] 전체 상태 변경 ${statuses.length}건:`, JSON.stringify(dist));
    }

    // productOrderId 중복 제거
    const seen = new Set();
    return statuses
      .filter(s => { if (seen.has(s.productOrderId)) return false; seen.add(s.productOrderId); return true; })
      .map(s => s.productOrderId);
  }

  // === Store B: Product queries and updates ===

  /**
   * Search products by keyword
   * @param {string} keyword
   * @returns {Array} matching products
   */
  async searchProducts(keyword) {
    const params = new URLSearchParams({
      searchKeyword: keyword,
    });

    const data = await this.apiCall(
      'GET',
      `/v2/products/search?${params}`
    );

    if (!data || !data.contents) return [];
    return data.contents;
  }

  /**
   * Get channel product detail
   * @param {string} channelProductNo
   * @returns {Object} product detail
   */
  async getChannelProduct(channelProductNo) {
    return this.apiCall(
      'GET',
      `/v2/products/channel-products/${channelProductNo}`
    );
  }

  /**
   * Get origin product detail
   * @param {string} originProductNo
   * @returns {Object} product detail
   */
  async getOriginProduct(originProductNo) {
    return this.apiCall(
      'GET',
      `/v2/products/origin-products/${originProductNo}`
    );
  }

  /**
   * Update product stock quantity
   * @param {string} channelProductNo
   * @param {Object} originProduct - the originProduct part from getChannelProduct response
   * @param {number} newStockQty - new total stock quantity
   * @param {Object} [smartstoreChannelProduct] - smartstoreChannelProduct from getChannelProduct response
   * @returns {Object} update result
   */
  async updateProductStock(channelProductNo, originProduct, newStockQty, smartstoreChannelProduct) {
    // Deep copy to avoid mutating the original
    const origin = JSON.parse(JSON.stringify(originProduct));

    // Remove read-only / server-computed fields that cause 400 BAD_REQUEST
    const readOnlyKeys = [
      'originProductNo', 'channelProducts', 'channelProductNo',
      'registrationType', 'createdDate', 'modifiedDate',
      'wishlisted', 'purchaseReviewCount', 'brandStoreInfo',
      'knowledgeShoppingProductRegistration', 'productLogistics',
      'commentCount', 'bestProductInfo', 'sellerManagementCode',
    ];
    for (const key of readOnlyKeys) {
      delete origin[key];
    }

    // Remove read-only nested fields
    if (origin.detailAttribute) {
      delete origin.detailAttribute.productInfoProvidedNoticeV2;
      delete origin.detailAttribute.certifications;
      delete origin.detailAttribute.isbnInfo;
    }
    if (origin.deliveryInfo) {
      delete origin.deliveryInfo.deliveryBundleGroupId;
    }

    // Set new stock quantity
    origin.stockQuantity = newStockQty;

    // optionStandards 재고도 갱신 — 네이버 API는 옵션별 재고 합계로 전체 재고를 계산
    const optInfo = origin.detailAttribute?.optionInfo;
    if (optInfo?.optionStandards && optInfo.optionStandards.length > 0) {
      // 모든 옵션 재고를 0으로 리셋 후, 첫 번째 옵션에 전체 수량 설정
      for (const opt of optInfo.optionStandards) {
        opt.stockQuantity = 0;
      }
      optInfo.optionStandards[0].stockQuantity = newStockQty;
    }
    if (optInfo?.optionCombinations && optInfo.optionCombinations.length > 0) {
      for (const opt of optInfo.optionCombinations) {
        opt.stockQuantity = 0;
      }
      optInfo.optionCombinations[0].stockQuantity = newStockQty;
    }

    const channelProductName = smartstoreChannelProduct?.channelProductName || origin.name || '';
    const displayStatus = smartstoreChannelProduct?.channelProductDisplayStatusType || 'ON';

    // smartstoreChannelProduct: 기존 값 복사 + 필수 필드 보장
    const channelProduct = {};
    if (smartstoreChannelProduct) {
      // 기존 채널 상품 정보를 복사 (read-only 필드 제외)
      const cpReadOnly = ['channelProductNo', 'categoryChannelProductNo', 'registerDate', 'modifyDate'];
      for (const [k, v] of Object.entries(smartstoreChannelProduct)) {
        if (!cpReadOnly.includes(k) && v !== undefined && v !== null) {
          channelProduct[k] = v;
        }
      }
    }
    // 필수 필드 보장
    channelProduct.channelProductName = channelProductName;
    channelProduct.channelProductDisplayStatusType = displayStatus;
    channelProduct.storeKeepExclusiveProduct = channelProduct.storeKeepExclusiveProduct ?? false;
    channelProduct.naverShoppingRegistration = channelProduct.naverShoppingRegistration ?? true;

    const updateBody = {
      originProduct: origin,
      smartstoreChannelProduct: channelProduct,
    };

    console.log(`[${this.storeName}] 수량 업데이트 요청: ${channelProductNo}, stockQty=${newStockQty}`);

    return this.apiCall(
      'PUT',
      `/v2/products/channel-products/${channelProductNo}`,
      updateBody
    );
  }

  /**
   * Create a new product
   * @param {Object} productData - product creation data
   * @returns {Object} created product
   */
  async createProduct(productData) {
    return this.apiCall(
      'POST',
      '/v2/products',
      productData
    );
  }

  /**
   * 리스트 내 stockQuantity를 모두 0으로 초기화 (deep copy)
   */
  static resetStockInList(info) {
    if (!info) return info;
    const copy = JSON.parse(JSON.stringify(info));
    if (Array.isArray(copy)) {
      for (const item of copy) {
        if (item.stockQuantity !== undefined) item.stockQuantity = 0;
      }
    }
    return copy;
  }

  /**
   * A 스토어 상품 정보를 복사하여 B 스토어용 등록 데이터로 변환
   * @param {Object} sourceProduct - A 스토어 getChannelProduct() 결과
   * @param {number} stockQty - 초기 재고 수량
   * @returns {Object} B 스토어 상품 등록 요청 body
   */
  static buildProductCopyData(sourceProduct, stockQty = 1, namePrefix = '(오늘출발)') {
    const origin = sourceProduct.originProduct || sourceProduct;
    const baseName = origin.name || '';
    const channelName = sourceProduct.channelProductName || baseName;
    // 접두어가 이미 있으면 중복 방지
    const prefixedName = namePrefix && !baseName.startsWith(namePrefix)
      ? `${namePrefix} ${baseName}` : baseName;
    const prefixedChannelName = namePrefix && !channelName.startsWith(namePrefix)
      ? `${namePrefix} ${channelName}` : channelName;

    // 기본 상품 정보 복사
    const newProduct = {
      originProduct: {
        statusType: 'SALE',
        saleType: origin.saleType || 'NEW',
        leafCategoryId: origin.leafCategoryId || '',
        name: prefixedName,
        detailContent: origin.detailContent || '',
        stockQuantity: stockQty,
        detailAttribute: {},
      },
      smartstoreChannelProduct: {
        channelProductName: prefixedChannelName,
        storeKeepExclusiveProduct: false,
        channelProductDisplayStatusType: 'ON',
        naverShoppingRegistration: true,
      },
    };

    const o = newProduct.originProduct;

    // 이미지 복사
    if (origin.images) {
      o.images = origin.images;
    }

    // 상세 속성 복사
    if (origin.detailAttribute) {
      const da = origin.detailAttribute;

      // B 스토어 상품은 옵션 없이 단순 상품으로 생성
      // (standardOptionGroups를 포함하면 네이버 API가 A 스토어의 옵션별 재고를 자동 복사함)
      let optionInfo = undefined;

      // 카탈로그 매칭 해제 — A 스토어 재고가 B 상품에 연동되는 것 방지
      let searchInfo = da.naverShoppingSearchInfo ? { ...da.naverShoppingSearchInfo } : undefined;
      if (searchInfo) {
        delete searchInfo.matchedCatalogId;
        searchInfo.catalogMatchingYn = false;
      }

      o.detailAttribute = {
        naverShoppingSearchInfo: searchInfo,
        afterServiceInfo: da.afterServiceInfo || undefined,
        originAreaInfo: da.originAreaInfo || undefined,
        sellerCodeInfo: da.sellerCodeInfo || undefined,
        optionInfo,
        supplementProductInfo: da.supplementProductInfo ? NaverCommerceClient.resetStockInList(da.supplementProductInfo) : undefined,
        purchaseQuantityInfo: da.purchaseQuantityInfo || undefined,
        minorPurchasable: da.minorPurchasable !== undefined ? da.minorPurchasable : true,
        seoInfo: da.seoInfo || undefined,
        productInfoProvidedNotice: da.productInfoProvidedNotice || undefined,
        productAttributes: da.productAttributes || undefined,
        taxType: da.taxType || undefined,
      };

      // 가격 복사
      if (da.saleStartDate) o.detailAttribute.saleStartDate = da.saleStartDate;
      if (da.saleEndDate) o.detailAttribute.saleEndDate = da.saleEndDate;
    }

    // B 스토어 가격 설정: A 할인가 × 85% (10원 단위 절사)
    if (origin.salePrice !== undefined) {
      let actualPrice = origin.salePrice;
      // 즉시할인이 있으면 할인 적용된 실제 판매가 계산
      const discount = origin.customerBenefit?.immediateDiscountPolicy?.discountMethod;
      if (discount) {
        if (discount.unitType === 'PERCENT') {
          actualPrice = Math.round(origin.salePrice * (1 - discount.value / 100));
        } else {
          actualPrice = origin.salePrice - (discount.value || 0);
        }
      }
      // 85% 적용 후 10원 단위 절사
      o.salePrice = Math.floor(actualPrice * 0.85 / 10) * 10;
    }

    // 배송 정보: A 스토어의 기본 구조만 복사 (주소 ID는 B 스토어 것으로 교체 필요)
    // deliveryInfo는 copyAndCreateInStoreB에서 B 스토어 설정으로 교체됨

    // undefined 필드 정리
    Object.keys(o.detailAttribute).forEach(k => {
      if (o.detailAttribute[k] === undefined) delete o.detailAttribute[k];
    });
    Object.keys(o).forEach(k => {
      if (o[k] === undefined) delete o[k];
    });

    return newProduct;
  }

  /**
   * Get seller's registered delivery addresses
   * @returns {Array} address list
   */
  async getDeliveryAddresses() {
    return this.apiCall('GET', '/v1/seller/addresses');
  }

  // === Display status update ===

  /**
   * 상품 전시 상태 변경 (ON → SUSPENSION 등)
   * @param {string} channelProductNo
   * @param {string} statusType - 'ON', 'SUSPENSION', 'CLOSE'
   */
  async updateDisplayStatus(channelProductNo, statusType) {
    const fullResponse = await this.getChannelProduct(channelProductNo);
    const originProduct = fullResponse.originProduct || fullResponse;
    const smartstoreCP = fullResponse.smartstoreChannelProduct || null;

    // Deep copy
    const origin = JSON.parse(JSON.stringify(originProduct));

    // Remove read-only fields
    const readOnlyKeys = [
      'originProductNo', 'channelProducts', 'channelProductNo',
      'registrationType', 'createdDate', 'modifiedDate',
      'wishlisted', 'purchaseReviewCount', 'brandStoreInfo',
      'knowledgeShoppingProductRegistration', 'productLogistics',
      'commentCount', 'bestProductInfo', 'sellerManagementCode',
    ];
    for (const key of readOnlyKeys) {
      delete origin[key];
    }
    if (origin.detailAttribute) {
      delete origin.detailAttribute.productInfoProvidedNoticeV2;
      delete origin.detailAttribute.certifications;
      delete origin.detailAttribute.isbnInfo;
    }
    if (origin.deliveryInfo) {
      delete origin.deliveryInfo.deliveryBundleGroupId;
    }

    // Build channel product
    const channelProduct = {};
    if (smartstoreCP) {
      const cpReadOnly = ['channelProductNo', 'categoryChannelProductNo', 'registerDate', 'modifyDate'];
      for (const [k, v] of Object.entries(smartstoreCP)) {
        if (!cpReadOnly.includes(k) && v !== undefined && v !== null) {
          channelProduct[k] = v;
        }
      }
    }
    channelProduct.channelProductName = smartstoreCP?.channelProductName || origin.name || '';
    channelProduct.channelProductDisplayStatusType = statusType;
    channelProduct.storeKeepExclusiveProduct = channelProduct.storeKeepExclusiveProduct ?? false;
    channelProduct.naverShoppingRegistration = channelProduct.naverShoppingRegistration ?? true;

    console.log(`[${this.storeName}] 전시 상태 변경: ${channelProductNo} → ${statusType}`);

    return this.apiCall(
      'PUT',
      `/v2/products/channel-products/${channelProductNo}`,
      { originProduct: origin, smartstoreChannelProduct: channelProduct }
    );
  }

  // === Store A: Returnable orders (반품완료 + 수거완료) ===

  /**
   * Get returnable orders (RETURN_DONE + COLLECT_DONE) in a time range
   * Automatically chunks into 24h segments (Naver API limit)
   * @param {string} fromDate ISO datetime
   * @param {string} toDate ISO datetime
   * @returns {Array} { productOrderId, claimStatus } objects
   */
  async getReturnableOrders(fromDate, toDate) {
    const typesToCheck = ['COLLECT_DONE', 'CLAIM_COMPLETED'];
    const allStatuses = [];
    const fromMs = new Date(fromDate).getTime();
    const toMs = new Date(toDate).getTime();
    const DAY = 24 * 60 * 60 * 1000;

    // 24시간씩 청크 분할 (네이버 API 제한)
    let cursor = fromMs;
    while (cursor < toMs) {
      const chunkEnd = Math.min(cursor + DAY, toMs);
      const chunkFrom = new Date(cursor).toISOString();
      const chunkTo = new Date(chunkEnd).toISOString();

      for (const changeType of typesToCheck) {
        try {
          const params = new URLSearchParams({
            lastChangedFrom: chunkFrom,
            lastChangedTo: chunkTo,
            lastChangedType: changeType,
          });

          const data = await this.apiCall(
            'GET',
            `/v1/pay-order/seller/product-orders/last-changed-statuses?${params}`
          );

          if (data?.data?.lastChangeStatuses) {
            for (const s of data.data.lastChangeStatuses) {
              allStatuses.push(s);
            }
          }
        } catch (e) {
          console.log(`[${this.storeName}] ${changeType} ${chunkFrom.slice(0,10)} 조회 오류 (무시):`, e.message);
        }
        await this.sleep(300);
      }
      cursor = chunkEnd;
    }

    // 반품 관련 건 필터: claimType=RETURN & (claimStatus=RETURN_DONE or COLLECT_DONE)
    const returnStatuses = allStatuses.filter(s => {
      const claimType = (s.claimType || '').toUpperCase();
      const claimStatus = (s.claimStatus || '').toUpperCase();
      return claimType === 'RETURN' && (claimStatus === 'RETURN_DONE' || claimStatus === 'COLLECT_DONE');
    });

    // 중복 제거
    const seen = new Set();
    return returnStatuses
      .filter(s => {
        if (seen.has(s.productOrderId)) return false;
        seen.add(s.productOrderId);
        return true;
      })
      .map(s => ({ productOrderId: s.productOrderId, claimStatus: s.claimStatus }));
  }

  // === Connection test ===

  async testConnection() {
    try {
      await this.getToken();
      return { success: true, message: `${this.storeName} 연결 성공` };
    } catch (e) {
      return { success: false, message: e.message };
    }
  }
}

module.exports = { NaverCommerceClient };
