''' customer related mapping '''
customer_mapping_dict = {
    "customer_id": [
        "customer_id", "cust_id", "c_id", "id", "custid",
        "customerId", "CustomerID", "Customer_Id", "custID",
        "unique_customer_id", "customer_number", "cust_num",
        "client_id", "clientId", "user_id", "userId",
        "account_id", "acc_id", "member_id"
    ],

    "customer_name": [
        "customer_name", "name", "full_name", "cust_name",
        "customerName", "CustomerName", "cust_full_name",
        "customer_fullname", "fullName", "fullname",
        "client_name", "clientName", "username", "user_name",
        "account_name", "member_name", "person_name"
    ],

    "customer_type": [
        "customer_type", "cust_type", "type", "ctype",
        "customerType", "CustomerType", "client_type",
        "account_type", "user_type", "membership_type",
        "category", "segment_type"
    ],

    "gender": [
        "gender", "sex", "gndr", "gender_type",
        "Gender", "GenderType", "sex_type",
        "male_female", "mf"
    ],

    "date_of_birth": [
        "date_of_birth", "dob", "birthdate", "birth_date",
        "DateOfBirth", "DOB", "bday", "birthDay",
        "user_dob", "cust_dob", "client_dob",
        "birthday"
    ],

    "registration_date": [
        "registration_date", "reg_date", "signup_date", "joined_date",
        "RegistrationDate", "regDate", "sign_up_date", "signupdate",
        "created_date", "created_on", "account_created",
        "user_created_date", "customer_created_date"
    ],

    "customer_status": [
        "customer_status", "status", "cust_status",
        "customerStatus", "CustomerStatus", "account_status",
        "client_status", "user_status", "membership_status",
        "state", "record_status"
    ],

    "acquisition_channel": [
        "acquisition_channel", "acq_channel", "acquisition_source",
        "AcquisitionChannel", "AcqChannel", "source", "channel",
        "signup_channel", "registration_channel",
        "marketing_channel", "referral_source", "ref_source"
    ],

    "customer_segment": [
        "customer_segment", "segment", "cust_segment",
        "customerSegment", "CustomerSegment", "client_segment",
        "user_segment", "market_segment", "category_segment",
        "tier", "customer_group", "membership_segment"
    ],

    "created_at": [
        "created_at", "createdOn", "createdDate", "created",
        "creation_date", "creation_timestamp", "record_created",
        "inserted_at", "insert_date", "entry_date","created_timestamp","created_at_timestamp",                 
        "row_created_at", "added_on", "time_created","record_created_at"
    ]
}

''' adddress related mapping '''
mapping_dict_addresses = {
    "address_id": [
        "address_id", "addr_id", "a_id", "addressId",
        "AddressID", "addrID", "unique_address_id",
        "address_number", "addr_num", "addrNo",
        "location_id", "loc_id"
    ],

    "customer_id": [
        "customer_id", "cust_id", "c_id", "id",
        "customerId", "CustomerID", "custid",
        "client_id", "clientId", "user_id", "userId",
        "account_id", "member_id", "person_id"
    ],

    "address_type": [
        "address_type", "addr_type", "type",
        "addressType", "AddressType", "addr_kind",
        "location_type", "loc_type", "usage_type",
        "billing_shipping", "addr_category"
    ],

    "city": [
        "city", "town", "municipality", "locality",
        "CityName", "city_name", "town_name",
        "urban_area", "place_city", "city_town"
    ],

    "state_province": [
        "state_province", "state", "province", "region",
        "stateProvince", "StateProvince", "province_name",
        "state_name", "territory", "county", "area"
    ],

    "postal_code": [
        "postal_code", "zip", "zipcode", "zip_code",
        "post_code", "PostCode", "postalcode",
        "zipNo", "pincode", "pin_code", "mail_code",
        "area_code"
    ],

    "country": [
        "country", "nation", "country_name",
        "CountryName", "countryName", "nation_name",
        "state_country", "nation_country"
    ],

    "is_default": [
        "is_default", "default", "default_flag", "default_addr",
        "isDefault", "IsDefault", "primary_address",
        "main_address", "defaultAddress", "addr_default",
        "preferred_address"
    ],

     "created_at": [
        "created_at", "createdOn", "createdDate", "created",
        "creation_date", "creation_timestamp", "record_created",
        "inserted_at", "insert_date", "entry_date","created_timestamp","created_at_timestamp",                 
        "row_created_at", "added_on", "time_created","record_created_at"
    ]
}

'''product related mapping '''

mapping_dict_products = {
    "product_id": [
        "product_id", "prod_id", "p_id", "id", "productId",
        "ProductID", "prodid", "product_number", "prod_num",
        "item_id", "itemId", "sku_id", "unique_product_id"
    ],

    "product_name": [
        "product_name", "prod_name", "name", "pname",
        "ProductName", "productName", "item_name",
        "itemName", "full_product_name", "product_title",
        "item_title", "productLabel"
    ],

    "sku": [
        "sku", "SKU", "SkuCode", "sku_code", "skuId",
        "product_sku", "item_sku", "stock_code",
        "stock_unit", "stock_keeping_unit", "sku_number",
        "sku_no", "product_code"
    ],

    "category_id": [
        "category_id", "cat_id", "categoryId", "CategoryID",
        "catid", "product_category_id", "item_category_id",
        "category_number", "category_code", "category_ref"
    ],

    "brand": [
        "brand", "brand_name", "BrandName", "brandName",
        "product_brand", "item_brand", "manufacturer",
        "maker", "label", "company"
    ],

    "supplier_id": [
        "supplier_id", "supp_id", "s_id", "supplierId",
        "SupplierID", "vendor_id", "vendorId",
        "provider_id", "providerId", "supplier_number"
    ],

    "cost_price": [
        "cost_price", "cost", "purchase_price", "buying_price",
        "costPrice", "CostPrice", "wholesale_price",
        "base_price", "product_cost", "item_cost"
    ],

    "selling_price": [
        "selling_price", "sell_price", "retail_price",
        "retailPrice", "RetailPrice", "sale_price",
        "product_price", "item_price", "final_price",
        "market_price", "store_price", "price"
    ],

    "launch_date": [
        "launch_date", "release_date", "available_from",
        "LaunchDate", "product_launch_date", "introduced_on",
        "releaseDate", "上市日期"  # sometimes datasets have multilingual fields
    ],

    "product_status": [
        "product_status", "status", "prod_status",
        "ProductStatus", "availability", "stock_status",
        "item_status", "active_flag", "product_state",
        "availability_status"
    ],

    "is_digital": [
        "is_digital", "digital", "digital_flag",
        "IsDigital", "DigitalProduct", "digital_product",
        "virtual", "downloadable", "electronic",
        "ebook_flag", "online_only"
    ],

    "average_rating": [
        "average_rating", "avg_rating", "rating",
        "avgRating", "AverageRating", "product_rating",
        "mean_rating", "item_rating", "stars", "review_score"
    ],

    "total_reviews": [
        "total_reviews", "reviews", "review_count",
        "TotalReviews", "reviewCount", "num_reviews",
        "number_of_reviews", "total_review_count",
        "ratings_count", "feedback_count"
    ],

      "created_at": [
        "created_at", "createdOn", "createdDate", "created",
        "creation_date", "creation_timestamp", "record_created",
        "inserted_at", "insert_date", "entry_date","created_timestamp","created_at_timestamp",                 
        "row_created_at", "added_on", "time_created","record_created_at"
    ]
}

'''category related mapping '''

mapping_dict_categories = {
    "category_id": [
        "category_id", "cat_id", "c_id", "id",
        "categoryId", "CategoryID", "catid",
        "category_number", "cat_num", "catNo",
        "category_code", "category_ref", "category_key"
    ],

    "category_name": [
        "category_name", "cat_name", "name", "category",
        "CategoryName", "categoryName", "catname",
        "category_title", "cat_title", "group_name",
        "segment_name", "class_name", "dept_name"
    ],

    "parent_category_id": [
        "parent_category_id", "parent_cat_id", "parent_id",
        "parentCategoryId", "ParentCategoryID", "parent_catid",
        "super_category_id", "main_category_id",
        "upper_category_id", "root_category_id"
    ],

    "is_active": [
        "is_active", "active", "active_flag",
        "IsActive", "status", "active_status",
        "enabled", "available", "isEnabled",
        "category_active", "cat_active"
    ],

      "created_at": [
        "created_at", "createdOn", "createdDate", "created",
        "creation_date", "creation_timestamp", "record_created",
        "inserted_at", "insert_date", "entry_date",
        "row_created_at", "added_on", "time_created","record_created_at"
    ]
}
'''wishlist related mapping '''
mapping_dict_wishlist = {
    "wishlist_id": [
        "wishlist_id", "wish_id", "w_id", "wishListId",
        "WishlistID", "wishid", "wl_id", "wishlist_number",
        "wishlist_code", "wishlist_ref", "wishlist_key"
    ],

    "customer_id": [
        "customer_id", "cust_id", "c_id", "id",
        "customerId", "CustomerID", "custid",
        "client_id", "clientId", "user_id", "userId",
        "account_id", "member_id", "person_id"
    ],

    "product_id": [
        "product_id", "prod_id", "p_id", "id",
        "productId", "ProductID", "prodid",
        "item_id", "itemId", "sku_id", "unique_product_id"
    ],

    "added_date": [
        "added_date", "add_date", "date_added", "wishlist_date",
        "AddedDate", "addedOn", "time_added", "created_date",
        "inserted_at", "entry_date", "wishlist_added"
    ],

    "priority_level": [
        "priority_level", "priority", "prio_level",
        "priorityLevel", "PriorityLevel", "wishlist_priority",
        "importance", "importance_level", "rank", "wishlist_rank",
        "urgency"
    ],

    "purchased_date": [
        "purchased_date", "purchase_date", "date_purchased",
        "PurchasedDate", "buy_date", "wishlist_purchased_date",
        "purchasedOn", "purchase_timestamp", "purchase_time",
        "bought_date"
    ],

    "removed_date": [
        "removed_date", "remove_date", "date_removed",
        "RemovedDate", "wishlist_removed_date", "deleted_date",
        "removedOn", "removal_date", "removal_time",
        "unwish_date"
    ]
}

'''cart related mapping '''

mapping_dict_shopping_cart = {
    "cart_id": [
        "cart_id", "c_id", "cartId", "CartID",
        "shopping_cart_id", "shoppingCartId", "cartid",
        "basket_id", "basketId", "sc_id", "cart_number",
        "cart_code", "cart_ref", "cart_key"
    ],

    "customer_id": [
        "customer_id", "cust_id", "c_id", "customerId",
        "CustomerID", "custid", "client_id", "clientId",
        "user_id", "userId", "account_id", "member_id",
        "buyer_id", "shopper_id", "person_id"
    ],

    "session_id": [
        "session_id", "sess_id", "sid", "sessionId",
        "SessionID", "guest_session_id", "shopping_session_id",
        "browser_session", "cart_session_id"
    ],

    "product_id": [
        "product_id", "prod_id", "p_id", "productId",
        "ProductID", "prodid", "item_id", "itemId",
        "sku_id", "product_code", "unique_product_id"
    ],

    "quantity": [
        "quantity", "qty", "qnty", "no_of_items", "num_items",
        "item_count", "product_qty", "cart_qty",
        "units", "count", "amount"
    ],

    "unit_price": [
        "unit_price", "price", "price_per_unit",
        "unitPrice", "UnitPrice", "per_unit_cost",
        "unit_cost", "product_price", "cart_unit_price",
        "rate", "item_price", "price_each"
    ],

    "added_date": [
        "added_date", "add_date", "date_added", "cart_date",
        "AddedDate", "addedOn", "time_added", "created_date",
        "inserted_at", "entry_date", "cart_added"
    ],

    "cart_status": [
        "cart_status", "status", "cartState",
        "shopping_cart_status", "basket_status",
        "cart_status_code", "cart_condition",
        "activity_status", "checkout_status"
    ],

    "converted_order_id": [
        "converted_order_id", "order_id", "ord_id",
        "convertedOrderId", "linked_order_id",
        "checkout_order_id", "sale_id", "transaction_id",
        "purchased_order_id", "order_reference"
    ]
}

'''order related mapping '''
mapping_dict_orders = {
    "order_id": [
        "order_id", "ord_id", "o_id", "orderId", "OrderID",
        "orderid", "order_number", "ordernum", "order_no", "ord_no",
        "purchase_id", "transaction_id", "txn_id", "checkout_id",
        "invoice_id", "invoice_number", "order_ref", "reference_order_id"
    ],

    "customer_id": [
        "customer_id", "cust_id", "c_id", "customerId",
        "CustomerID", "custid", "client_id", "clientId",
        "user_id", "userId", "account_id", "member_id",
        "buyer_id", "shopper_id", "person_id"
    ],

    "session_id": [
        "session_id", "sess_id", "sid", "sessionId",
        "SessionID", "guest_session_id", "shopping_session_id",
        "browser_session", "order_session_id"
    ],

    "order_date": [
        "order_date", "ord_date", "date_ordered", "order_datetime",
        "order_timestamp", "purchase_date", "transaction_date",
        "checkout_date", "order_created", "order_time", "placed_date"
    ],

    "order_status": [
        "order_status", "ord_status", "status",
        "order_state", "order_condition", "order_flag",
        "purchase_status", "checkout_status", "delivery_status",
        "shipping_status", "payment_status"
    ],

    "subtotal": [
        "subtotal", "sub_total", "pre_tax_total",
        "gross_total", "items_total", "cart_total",
        "amount_before_tax", "before_discount_total",
        "base_total", "initial_total"
    ],

    "tax_amount": [
        "tax_amount", "tax", "taxes", "tax_total",
        "total_tax", "tax_fee", "vat", "gst",
        "sales_tax", "service_tax", "applied_tax"
    ],

    "shipping_cost": [
        "shipping_cost", "shipping_fee", "delivery_fee",
        "freight", "shipping_charges", "shippingAmount",
        "courier_fee", "logistics_cost", "transport_fee"
    ],

    "discount_amount": [
        "discount_amount", "discount", "discounts",
        "discount_value", "discount_total", "promo_discount",
        "voucher_amount", "coupon_discount", "rebate",
        "offer_amount", "deduction", "markdown"
    ],

    "total_amount": [
        "total_amount", "total", "grand_total", "order_total",
        "final_amount", "amount_paid", "net_amount",
        "checkout_total", "invoice_total", "bill_amount",
        "payment_total", "net_payable"
    ],

    "currency": [
        "currency", "curr", "currency_code",
        "currency_type", "currency_iso", "currency_symbol",
        "money_code", "money_type", "transaction_currency"
    ],

    "acquisition_channel": [
        "acquisition_channel", "acq_channel", "channel",
        "order_channel", "sales_channel", "source_channel",
        "marketing_channel", "campaign_channel", "origin_channel",
        "traffic_source"
    ],

    "device_type": [
        "device_type", "device", "deviceType",
        "platform", "user_device", "order_device",
        "os_type", "browser_device"
    ],

    "shipped_date": [
        "shipped_date", "ship_date", "date_shipped",
        "shipping_date", "dispatch_date", "shipment_date",
        "sent_date", "delivery_dispatch_date"
    ],

    "delivered_date": [
        "delivered_date", "delivery_date", "date_delivered",
        "received_date", "arrival_date", "fulfillment_date",
        "drop_date", "completed_date"
    ],

      "created_at": [
        "created_at", "createdOn", "createdDate", "created",
        "creation_date", "creation_timestamp", "record_created",
        "inserted_at", "insert_date", "entry_date","created_timestamp","created_at_timestamp",                 
        "row_created_at", "added_on", "time_created","record_created_at"
    ]
}

'''order item related mapping '''
mapping_dict_order_items = {
    "order_item_id": [
        "order_item_id", "oi_id", "orderitemid",
        "orderItemId", "OrderItemID", "oiid",
        "oi_number", "oi_no", "order_item_number",
        "order_item_code", "order_item_ref", "order_item_key",
        "line_item_id", "lineId", "line_id", "item_line_id"
    ],

    "order_id": [
        "order_id", "ord_id", "o_id", "orderId",
        "OrderID", "orderid", "order_number",
        "ord_no", "order_ref", "purchase_id",
        "transaction_id", "txn_id", "checkout_id"
    ],

    "product_id": [
        "product_id", "prod_id", "p_id", "productId",
        "ProductID", "prodid", "item_id", "itemId",
        "sku_id", "unique_product_id", "product_ref"
    ],

    "quantity": [
        "quantity", "qty", "order_qty", "ordered_qty",
        "Quantity", "no_of_items", "units", "item_count",
        "count", "number_of_units", "ordered_units"
    ],

    "unit_price": [
        "unit_price", "price_per_unit", "per_unit_price",
        "u_price", "unitPrice", "unit_cost", "item_price",
        "cost_per_unit", "price_each", "rate", "selling_price"
    ],

    "total_price": [
        "total_price", "price_total", "total",
        "item_total", "line_total", "subtotal",
        "extended_price", "gross_price", "net_price",
        "amount_total", "line_amount"
    ],

    "discount_amount": [
        "discount_amount", "discount", "discounts",
        "discount_value", "discount_total", "item_discount",
        "promo_discount", "voucher_amount", "coupon_discount",
        "rebate", "offer_amount", "deduction", "markdown"
    ],

    "product_cost": [
        "product_cost", "cost", "item_cost",
        "productCost", "prod_cost", "unit_cost",
        "purchase_cost", "buying_price", "base_cost",
        "manufacturing_cost", "cogs", "cost_of_goods"
    ],

    "created_at": [
        "created_at", "creation_date", "created_date",
        "createdOn", "inserted_at", "entry_date",
        "added_date", "record_created", "timestamp_created"
    ]
}

mapping_dict_payments = {
    "payment_id": [
        "payment_id", "pay_id", "pmt_id",
        "paymentId", "PaymentID", "payid",
        "payment_number", "payment_no", "payment_ref",
        "payment_code", "payment_key", "payment_identifier"
    ],

    "order_id": [
        "order_id", "ord_id", "o_id", "orderId",
        "OrderID", "orderid", "order_number",
        "ord_no", "order_ref", "purchase_id",
        "transaction_order_id", "txn_order_id"
    ],

    "payment_method": [
        "payment_method", "pay_method", "method",
        "paymentMethod", "PaymentMethod", "payment_type",
        "mode_of_payment", "payment_mode", "pmt_method",
        "method_of_payment", "payment_channel"
    ],

    "payment_provider": [
        "payment_provider", "pay_provider", "provider",
        "payment_gateway", "paymentProcessor", "processor",
        "payment_vendor", "gateway", "merchant",
        "payment_service", "provider_name"
    ],

    "payment_status": [
        "payment_status", "pay_status", "status",
        "PaymentStatus", "transaction_status", "txn_status",
        "payment_state", "payment_condition", "pmt_status",
        "settlement_status", "approval_status"
    ],

    "payment_date": [
        "payment_date", "pay_date", "date_paid",
        "payment_datetime", "payment_timestamp",
        "txn_date", "transaction_date", "processed_date",
        "payment_time", "settlement_date"
    ],

    "amount": [
        "amount", "amt", "payment_amount",
        "paid_amount", "total_paid", "paid_total",
        "gross_amount", "net_amount", "payment_value",
        "txn_amount", "order_amount"
    ],

    "transaction_id": [
        "transaction_id", "txn_id", "transactionId",
        "TransactionID", "trans_id", "txn_number",
        "transaction_ref", "transaction_reference",
        "payment_transaction_id", "provider_txn_id",
        "gateway_txn_id"
    ],

    "processing_fee": [
        "processing_fee", "fee", "transaction_fee",
        "gateway_fee", "service_fee", "commission",
        "payment_fee", "charge", "handling_fee",
        "processing_charge", "provider_fee"
    ],

    "refund_amount": [
        "refund_amount", "refunded_amount", "refund",
        "refund_total", "returned_amount", "reimbursed_amount",
        "credit_amount", "repayment_amount", "ref_amt",
        "refund_value", "amount_refunded"
    ],

    "refund_date": [
        "refund_date", "date_refunded", "refund_datetime",
        "refund_timestamp", "return_date", "reversal_date",
        "reimbursed_date", "credit_date"
    ],

     "created_at": [
        "created_at", "createdOn", "createdDate", "created",
        "creation_date", "creation_timestamp", "record_created",
        "inserted_at", "insert_date", "entry_date","created_timestamp","created_at_timestamp",                 
        "row_created_at", "added_on", "time_created","record_created_at"
    ]
}

'''inventory related mapping '''

mapping_dict_inventory = {
    "inventory_id": [
        "inventory_id", "inv_id", "i_id", "inventoryId",
        "InventoryID", "inventoryid", "inv_number", "inv_no",
        "inventory_code", "inventory_ref", "stock_id",
        "warehouse_id", "inventory_key"
    ],

    "product_id": [
        "product_id", "prod_id", "p_id", "productId",
        "ProductID", "prodid", "item_id", "itemId",
        "sku_id", "unique_product_id", "product_ref"
    ],

    "supplier_id": [
        "supplier_id", "sup_id", "s_id", "supplierId",
        "SupplierID", "vendor_id", "vendorId", "supplier_ref",
        "provider_id", "merchant_id", "seller_id"
    ],

    "stock_quantity": [
        "stock_quantity", "stock_qty", "quantity_in_stock",
        "on_hand", "available_stock", "qty_available",
        "inventory_qty", "units_in_stock", "stock_count",
        "current_stock", "qty"
    ],

    "reserved_quantity": [
        "reserved_quantity", "reserved_qty", "qty_reserved",
        "hold_qty", "stock_on_hold", "blocked_stock",
        "stock_reserved", "inventory_reserved", "reserved_units"
    ],

    "reorder_level": [
        "reorder_level", "reorder_threshold", "reorder_point",
        "restock_level", "stock_minimum", "min_stock",
        "replenishment_level", "alert_level", "low_stock_level"
    ],

    "last_restocked_date": [
        "last_restocked_date", "restocked_date", "restock_date",
        "lastRestockedDate", "last_restock", "inventory_restock_date",
        "last_replenished", "replenished_date", "last_added_stock_date"
    ],

    "last_sold_date": [
        "last_sold_date", "sold_date", "lastSaleDate",
        "last_sale", "last_transaction_date", "last_order_date",
        "last_checkout_date", "last_purchase_date", "last_sold_on"
    ],

    "storage_cost": [
        "storage_cost", "inventory_cost", "holding_cost",
        "warehouse_cost", "monthly_storage_fee", "stock_fee",
        "carrying_cost", "storage_fee", "cost_of_storage",
        "unit_storage_cost"
    ],

    "created_at": [
        "created_at", "createdOn", "createdDate", "created",
        "creation_date", "creation_timestamp", "record_created",
        "inserted_at", "insert_date", "entry_date","created_timestamp","created_at_timestamp",                 
        "row_created_at", "added_on", "time_created","record_created_at"
    ]
}

'''review related mapping '''

mapping_dict_reviews = {
    "review_id": [
        "review_id", "rev_id", "r_id", "reviewId",
        "ReviewID", "reviewid", "rev_number", "rev_no",
        "review_code", "review_ref", "feedback_id",
        "comment_id", "rating_id"
    ],

    "product_id": [
        "product_id", "prod_id", "p_id", "productId",
        "ProductID", "prodid", "item_id", "itemId",
        "sku_id", "unique_product_id", "product_ref"
    ],

    "customer_id": [
        "customer_id", "cust_id", "c_id", "customerId",
        "CustomerID", "custid", "buyer_id", "user_id",
        "reviewer_id", "client_id", "member_id"
    ],

    "rating": [
        "rating", "stars", "star_rating", "review_rating",
        "score", "review_score", "customer_rating",
        "user_rating", "given_rating", "feedback_rating"
    ],

    "review_title": [
        "review_title", "title", "headline", "summary",
        "review_headline", "short_title", "caption",
        "subject", "review_heading", "feedback_title"
    ],

    "review_text": [
        "review_text", "review", "comment", "review_body",
        "feedback", "review_content", "text", "description",
        "customer_feedback", "user_review", "remarks",
        "message", "review_comments"
    ],

    "review_date": [
        "review_date", "date", "submitted_date",
        "reviewed_on", "feedback_date", "comment_date",
        "timestamp", "reviewed_date", "date_submitted",
        "posted_date", "created_at"
    ],

    "is_verified_purchase": [
        "is_verified_purchase", "verified", "verified_purchase",
        "purchase_verified", "confirmed_purchase",
        "buyer_verified", "is_verified", "verified_flag",
        "authentic_review", "legit_purchase", "verified_buyer"
    ]
}

mapping_dict_marketing_campaigns = {
    "campaign_id": [
        "campaign_id", "cmp_id", "campaignId",
        "CampaignID", "cmpid", "id", "campaign_code",
        "campaign_ref", "marketing_id", "advert_id"
    ],

    "campaign_name": [
        "campaign_name", "name", "title", "campaignTitle",
        "campaign_label", "cmp_name", "marketing_name",
        "promo_name", "advert_name", "ad_campaign"
    ],

    "campaign_type": [
        "campaign_type", "type", "channel", "medium",
        "marketing_type", "promotion_type", "ad_type",
        "cmp_type", "campaign_channel", "advertising_type"
    ],

    "start_date": [
        "start_date", "start", "begin_date", "from_date",
        "campaign_start", "launch_date", "initiation_date",
        "date_start", "startday", "start_on"
    ],

    "end_date": [
        "end_date", "end", "close_date", "to_date",
        "campaign_end", "expiry_date", "finish_date",
        "date_end", "endday", "end_on"
    ],

    "budget": [
        "budget", "allocated_budget", "campaign_budget",
        "total_budget", "planned_budget", "cost_budget",
        "budget_amount", "max_budget", "budget_allocation"
    ],

    "spent_amount": [
        "spent_amount", "amount_spent", "spend", "expenditure",
        "actual_spent", "used_budget", "spent", "consumed_budget",
        "expense", "marketing_cost"
    ],

    "impressions": [
        "impressions", "views", "ad_views", "ad_impressions",
        "display_count", "reach", "exposures", "seen_count",
        "times_displayed", "campaign_impressions"
    ],

    "clicks": [
        "clicks", "ad_clicks", "num_clicks", "click_count",
        "click_throughs", "hit_count", "taps", "engagement_clicks",
        "link_clicks", "ctr_clicks"
    ],

    "conversions": [
        "conversions", "sales", "leads", "successful_conversions",
        "transactions", "orders", "signups", "registrations",
        "converted", "completed_actions", "goal_completions"
    ],

    "target_audience": [
        "target_audience", "audience", "segment", "customer_segment",
        "market_segment", "target_group", "buyer_persona",
        "audience_profile", "target_market", "demographics"
    ],

    "campaign_status": [
        "campaign_status", "status", "cmp_status", "state",
        "current_status", "marketing_status", "ad_status",
        "promotion_status", "campaign_state"
    ],

    "created_at": [
        "created_at", "createdOn", "createdDate", "created",
        "creation_date", "creation_timestamp", "record_created",
        "inserted_at", "insert_date", "entry_date","created_timestamp","created_at_timestamp",                 
        "row_created_at", "added_on", "time_created","record_created_at"
    ]
}


'''Master mapping dictionary'''

master_mapping_dict = {
    "customers": {
        "customer_id": [
            "customer_id", "cust_id", "c_id", "id", "custid",
            "customerId", "CustomerID", "Customer_Id", "custID",
            "unique_customer_id", "customer_number", "cust_num",
            "client_id", "clientId", "user_id", "account_id"
        ],
        "customer_name": [
            "customer_name", "name", "full_name", "cust_name",
            "customer_fullname", "customerName", "CustomerName",
            "client_name", "username", "user_name", "account_name"
        ],
        "customer_type": [
            "customer_type", "cust_type", "type", "user_type",
            "client_type", "acct_type", "segment_type"
        ],
        "gender": [
            "gender", "sex", "gender_type", "gndr", "mf",
            "customer_gender", "user_gender"
        ],
        "date_of_birth": [
            "date_of_birth", "dob", "birthdate", "birth_date",
            "birthday", "d_o_b", "dateBirth", "birth"
        ],
        "registration_date": [
            "registration_date", "reg_date", "signup_date",
            "joined_date", "created_date", "account_created",
            "user_registered"
        ],
        "customer_status": [
            "customer_status", "status", "acct_status",
            "user_status", "client_status", "cust_status"
        ],
        "acquisition_channel": [
            "acquisition_channel", "channel", "marketing_channel",
            "source", "utm_source", "acq_channel", "acquisition_source"
        ],
        "customer_segment": [
            "customer_segment", "segment", "cust_segment",
            "segmentation", "user_segment", "client_segment"
        ],
        "created_at": [
            "created_at", "created", "creation_date",
            "record_created", "inserted_at", "created_timestamp"
        ]
    },

    "addresses": {
        "address_id": [
            "address_id", "addr_id", "aid", "addressId",
            "unique_address_id", "addrid"
        ],
        "customer_id": [
            "customer_id", "cust_id", "cid", "user_id",
            "account_id", "client_id"
        ],
        "address_type": [
            "address_type", "addr_type", "type", "billing_shipping",
            "shipping_type", "billing_type"
        ],
        "city": [
            "city", "town", "municipality", "city_name",
            "location_city"
        ],
        "state_province": [
            "state_province", "state", "province", "region",
            "territory", "county", "state_name"
        ],
        "postal_code": [
            "postal_code", "zipcode", "zip", "postcode",
            "pin_code", "pincode", "zip_code"
        ],
        "country": [
            "country", "nation", "country_name", "cntry",
            "ctr", "location_country"
        ],
        "is_default": [
            "is_default", "default_flag", "default_address",
            "primary", "main_address", "is_primary"
        ],
        "created_at": [
            "created_at", "created", "creation_date",
            "record_created", "inserted_at"
        ]
    },

    "products": {
        "product_id": [
            "product_id", "prod_id", "pid", "prid",
            "productId", "item_id", "sku_id"
        ],
        "product_name": [
            "product_name", "prod_name", "item_name", "name",
            "productTitle", "product_title", "pname"
        ],
        "sku": [
            "sku", "stock_keeping_unit", "stock_id",
            "item_code", "product_code"
        ],
        "category_id": [
            "category_id", "cat_id", "cid", "categoryId"
        ],
        "brand": [
            "brand", "brand_name", "manufacturer", "maker"
        ],
        "supplier_id": [
            "supplier_id", "supp_id", "sid", "vendor_id",
            "provider_id"
        ],
        "cost_price": [
            "cost_price", "purchase_price", "buy_price",
            "cogs", "product_cost", "cost"
        ],
        "selling_price": [
            "selling_price", "retail_price", "sale_price",
            "price", "product_price", "unit_price"
        ],
        "launch_date": [
            "launch_date", "release_date", "available_since",
            "introduced_date"
        ],
        "product_status": [
            "product_status", "status", "availability",
            "item_status", "prod_status"
        ],
        "is_digital": [
            "is_digital", "digital_flag", "digital",
            "is_virtual", "is_downloadable"
        ],
        "average_rating": [
            "average_rating", "avg_rating", "rating",
            "stars", "mean_rating"
        ],
        "total_reviews": [
            "total_reviews", "reviews_count", "review_count",
            "num_reviews", "ratings_count"
        ],
        "created_at": [
            "created_at", "created", "creation_date",
            "record_created"
        ]
    },

    "categories": {
        "category_id": [
            "category_id", "cat_id", "cid", "categoryId"
        ],
        "category_name": [
            "category_name", "cat_name", "name", "categoryTitle"
        ],
        "parent_category_id": [
            "parent_category_id", "parent_cat_id", "parent_id",
            "parentCategory"
        ],
        "is_active": [
            "is_active", "active_flag", "active",
            "enabled", "status"
        ],
        "created_at": [
            "created_at", "created", "creation_date",
            "record_created"
        ]
    },

    "wishlist": {
        "wishlist_id": [
            "wishlist_id", "wish_id", "wlist_id", "w_id"
        ],
        "customer_id": [
            "customer_id", "cust_id", "cid", "user_id"
        ],
        "product_id": [
            "product_id", "prod_id", "pid", "item_id"
        ],
        "added_date": [
            "added_date", "date_added", "wishlist_date",
            "created_date", "inserted_at"
        ],
        "priority_level": [
            "priority_level", "priority", "importance",
            "priority_flag", "wishlist_priority"
        ],
        "purchased_date": [
            "purchased_date", "date_purchased", "bought_date",
            "purchased_on"
        ],
        "removed_date": [
            "removed_date", "date_removed", "deleted_date",
            "removed_on"
        ]
    },

    "shopping_cart": {
        "cart_id": [
            "cart_id", "shopping_cart_id", "cid", "cartId"
        ],
        "customer_id": [
            "customer_id", "cust_id", "uid", "user_id"
        ],
        "session_id": [
            "session_id", "sid", "sess_id", "sessionId"
        ],
        "product_id": [
            "product_id", "pid", "prod_id", "item_id"
        ],
        "quantity": [
            "quantity", "qty", "qnty", "count", "num_items"
        ],
        "unit_price": [
            "unit_price", "price", "item_price", "prod_price"
        ],
        "added_date": [
            "added_date", "date_added", "created_date",
            "inserted_at"
        ],
        "cart_status": [
            "cart_status", "status", "state", "cart_state"
        ],
        "converted_order_id": [
            "converted_order_id", "order_id", "linked_order_id",
            "conversion_id"
        ]
    },

    "orders": {
        "order_id": [
            "order_id", "oid", "ord_id", "orderId"
        ],
        "customer_id": [
            "customer_id", "cust_id", "uid", "user_id"
        ],
        "session_id": [
            "session_id", "sid", "sess_id"
        ],
        "order_date": [
            "order_date", "date", "placed_date", "created_date"
        ],
        "order_status": [
            "order_status", "status", "state", "order_state"
        ],
        "subtotal": [
            "subtotal", "sub_total", "before_tax", "amount_before_tax"
        ],
        "tax_amount": [
            "tax_amount", "tax", "vat", "gst"
        ],
        "shipping_cost": [
            "shipping_cost", "shipping_fee", "delivery_fee"
        ],
        "discount_amount": [
            "discount_amount", "discount", "coupon_discount"
        ],
        "total_amount": [
            "total_amount", "grand_total", "final_amount"
        ],
        "currency": [
            "currency", "curr", "money_type"
        ],
        "acquisition_channel": [
            "acquisition_channel", "channel", "utm_source"
        ],
        "device_type": [
            "device_type", "device", "platform"
        ],
        "shipped_date": [
            "shipped_date", "date_shipped", "dispatch_date"
        ],
        "delivered_date": [
            "delivered_date", "date_delivered", "delivery_date"
        ],
        "created_at": [
            "created_at", "created", "creation_date"
        ]
    },

    "order_items": {
        "order_item_id": [
            "order_item_id", "oiid", "item_id", "order_line_id"
        ],
        "order_id": [
            "order_id", "oid", "ord_id"
        ],
        "product_id": [
            "product_id", "pid", "prod_id", "item_id"
        ],
        "quantity": [
            "quantity", "qty", "qnty", "num_units"
        ],
        "unit_price": [
            "unit_price", "price", "product_price"
        ],
        "total_price": [
            "total_price", "line_total", "amount", "extended_price"
        ],
        "discount_amount": [
            "discount_amount", "discount", "line_discount"
        ],
        "product_cost": [
            "product_cost", "cost", "cost_price"
        ],
        "created_at": [
            "created_at", "created", "creation_date"
        ]
    },

    "payments": {
        "payment_id": [
            "payment_id", "pay_id", "pid", "paymentId"
        ],
        "order_id": [
            "order_id", "oid", "ord_id"
        ],
        "payment_method": [
            "payment_method", "method", "pay_method", "paymentType"
        ],
        "payment_provider": [
            "payment_provider", "provider", "gateway", "processor"
        ],
        "payment_status": [
            "payment_status", "status", "state"
        ],
        "payment_date": [
            "payment_date", "date", "processed_date"
        ],
        "amount": [
            "amount", "amt", "payment_amount"
        ],
        "transaction_id": [
            "transaction_id", "txn_id", "txid", "trans_id"
        ],
        "processing_fee": [
            "processing_fee", "fee", "transaction_fee"
        ],
        "refund_amount": [
            "refund_amount", "refund", "refunded_amt"
        ],
        "refund_date": [
            "refund_date", "date_refunded", "refund_on"
        ],
        "created_at": [
            "created_at", "created", "creation_date"
        ]
    },

    "inventory": {
        "inventory_id": [
            "inventory_id", "inv_id", "iid", "stock_id"
        ],
        "product_id": [
            "product_id", "pid", "prod_id"
        ],
        "supplier_id": [
            "supplier_id", "supp_id", "sid", "vendor_id"
        ],
        "stock_quantity": [
            "stock_quantity", "stock", "quantity", "qty"
        ],
        "reserved_quantity": [
            "reserved_quantity", "reserved", "held_qty"
        ],
        "reorder_level": [
            "reorder_level", "reorder_point", "rop", "threshold"
        ],
        "last_restocked_date": [
            "last_restocked_date", "restock_date", "replenished_date"
        ],
        "last_sold_date": [
            "last_sold_date", "sold_date", "last_sale_date"
        ],
        "storage_cost": [
            "storage_cost", "holding_cost", "warehouse_cost"
        ],
        "created_at": [
            "created_at", "created", "creation_date"
        ]
    },

    "reviews": {
        "review_id": [
            "review_id", "rev_id", "rid"
        ],
        "product_id": [
            "product_id", "pid", "prod_id"
        ],
        "customer_id": [
            "customer_id", "cust_id", "uid"
        ],
        "rating": [
            "rating", "stars", "score", "review_score"
        ],
        "review_title": [
            "review_title", "title", "headline"
        ],
        "review_text": [
            "review_text", "text", "comment", "feedback"
        ],
        "review_date": [
            "review_date", "date", "submitted_date"
        ],
        "is_verified_purchase": [
            "is_verified_purchase", "verified", "verified_flag"
        ]
    },

    "marketing_campaigns": {
        "campaign_id": [
            "campaign_id", "camp_id", "cid"
        ],
        "campaign_name": [
            "campaign_name", "name", "title"
        ],
        "campaign_type": [
            "campaign_type", "type", "channel"
        ],
        "start_date": [
            "start_date", "begin_date", "from_date"
        ],
        "end_date": [
            "end_date", "finish_date", "to_date"
        ],
        "budget": [
            "budget", "allocated_budget", "planned_spend"
        ],
        "spent_amount": [
            "spent_amount", "spend", "amount_spent"
        ],
        "impressions": [
            "impressions", "views", "impr"
        ],
        "clicks": [
            "clicks", "click_count", "num_clicks"
        ],
        "conversions": [
            "conversions", "sales", "num_conversions"
        ],
        "target_audience": [
            "target_audience", "audience", "target"
        ],
        "campaign_status": [
            "campaign_status", "status", "state"
        ],
        "created_at": [
            "created_at", "created", "creation_date"
        ]
    },

    "customer_sessions": {
        "session_id": [
            "session_id", "sid", "sess_id", "sessionId",
            "user_session", "session_code", "tracking_id", "visit_id"
        ],
        "customer_id": [
            "customer_id", "cust_id", "cid", "user_id", "uid",
            "buyer_id", "shopper_id", "account_id", "client_id"
        ],
        "session_start": [
            "session_start", "start_time", "login_time",
            "visit_start", "entry_time", "started_at"
        ],
        "session_end": [
            "session_end", "end_time", "logout_time",
            "exit_time", "visit_end", "closed_at"
        ],
        "device_type": [
            "device_type", "device", "platform", "os_type",
            "user_device", "browser_device"
        ],
        "referrer_source": [
            "referrer_source", "referrer", "source",
            "traffic_source", "utm_source", "origin"
        ],
        "pages_viewed": [
            "pages_viewed", "page_views", "pages", "views"
        ],
        "products_viewed": [
            "products_viewed", "product_views", "items_viewed",
            "catalog_views", "viewed_products"
        ],
        "conversion_flag": [
            "conversion_flag", "converted", "is_converted",
            "purchase_flag", "conversion"
        ],
        "cart_abandonment_flag": [
            "cart_abandonment_flag", "abandoned", "is_abandoned",
            "cart_left", "abandonment"
        ]
    }
}
