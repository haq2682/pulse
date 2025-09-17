import pandas as pd

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
def normalize_dataframe(df: pd.DataFrame, column_variants: dict):
    
    variant_to_standard = {}
    for std_col, variants in column_variants.items():
        for v in variants:
            variant_to_standard[v.lower()] = std_col
    print(variant_to_standard)

    mapped_cols = {}
    new_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in variant_to_standard:
            std_col = variant_to_standard[col_lower]
            new_columns.append(std_col)
            mapped_cols[col] = std_col
        else:
            new_columns.append(col) 

    df.columns = new_columns

    
    missing_cols = []
    for std_col in column_variants.keys():
        if std_col not in df.columns:
            df[std_col] = pd.NA
            missing_cols.append(std_col)

    
    schema_cols = list(column_variants.keys())
    extra_cols = [c for c in df.columns if c not in schema_cols]
    new_df =  df[schema_cols]
    df_extra = df[schema_cols + extra_cols]

    
    return new_df,df_extra, extra_cols, missing_cols, mapped_cols

df = pd.read_excel(r"D:\VS CODE\pulse\dataset\messy_customer_data.xlsx")  

new_df,extra_df, extra_cols, missing, mapped = normalize_dataframe(df, customer_mapping_dict)

print("\nNormalized DataFrame:")
print(new_df.head(5))
print("\nDataFrame with Extra Columns:")
print(extra_df.head(5))
print("\nMissing columns:")
print(missing)
print("\nExtra columns:")
print(extra_cols)
print("\nMapped columns:")
print(mapped)