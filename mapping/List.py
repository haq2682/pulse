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
        "inserted_at", "insert_date", "entry_date",
        "row_created_at", "added_on", "time_created"
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
        "inserted_at", "insert_date", "entry_date",
        "row_created_at", "added_on", "time_created"
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
        "created_at", "createdOn", "createdDate",
        "creation_date", "creation_timestamp",
        "record_created", "inserted_at",
        "entry_date", "row_created_at",
        "added_on", "time_created"
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
        "created_at", "createdOn", "createdDate",
        "creation_date", "creation_timestamp",
        "record_created", "inserted_at",
        "entry_date", "row_created_at",
        "added_on", "time_created"
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
        "created_at", "creation_date", "created_date",
        "createdOn", "inserted_at", "entry_date",
        "added_date", "record_created", "timestamp_created"
    ]
}
