import pyspark.sql.functions as F
from pyspark.sql import Window
from analysis_config import create_spark_session
from analysis_utils import (
    get_agg_tables,
    add_time_grain,
    check_null_dataframes
)
from analysis_export_utils import export_analytics_to_minio
def is_column_all_null_or_zero(df, col_name):
    if df is None:
        return True                    

    if col_name not in df.columns:
        return True                   

    col_type = dict(df.dtypes)[col_name]
    non_null_count = df.agg(
        F.count(F.col(col_name)).alias("non_null_count")
    ).collect()[0]["non_null_count"]
    if non_null_count == 0:
        return True                   

    
    if col_type in ("int", "bigint", "double", "float", "decimal", "smallint", "tinyint"):
        non_zero_non_null_count = df.agg(
            F.sum(
                F.when(
                    (F.col(col_name).isNotNull()) & (F.col(col_name) != 0), 1
                ).otherwise(0)
            ).alias("non_zero_non_null_count")
        ).collect()[0]["non_zero_non_null_count"]

        if non_zero_non_null_count == 0:
            return True                 

    return False

analysis = {}
product_analysis = {}
supplier_analysis = {}
dataframes = {}


def main():

    print("🚀 Starting Analysis Pipeline...")
    spark = create_spark_session("Ecommerce_Analysis_Main")



    print("\n📥 Loading Aggregated Tables from MinIO...")
        # Load transformed data from MinIO bucket
    dataframes = get_agg_tables(spark)
        
    if not dataframes:
        print("❌ No tables loaded. Exiting.")
        exit(1)

        # ---------------------------------------------------------

    if "agg_orders" in dataframes and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_placed_at"):
        dataframes["agg_orders"] = dataframes["agg_orders"].withColumn(
        "order_date",
        F.to_date("order_placed_at")
        )

    if "agg_customers" in dataframes and not is_column_all_null_or_zero(dataframes["agg_customers"], "account_created_at"):
        dataframes["agg_customers"] = dataframes["agg_customers"].withColumn(
        "account_created_date",
        F.to_date("account_created_at")
        )

    if "agg_orders" in dataframes and "agg_order_items" in dataframes:
        order_units = (
            dataframes["agg_order_items"]
            .groupBy("order_id")
            .agg(F.sum("quantity").alias("units_sold"))
        )

        dataframes["agg_orders"] = (
            dataframes["agg_orders"]
            .join(order_units, on="order_id", how="left")
            .fillna({"units_sold": 0})
        )
    else:
        print("agg_orders or agg_order_items table is missing.")



    def core_kpis_over_time(df,date_col, grain="day"):
        df_g = add_time_grain(df, date_col="order_placed_at", grain=grain)

        if grain == "day":
            group_cols = ["grain_date"]
            order_cols = ["grain_date"]
        elif grain == "week":
            group_cols = ["grain_year", "grain_week"]
            order_cols = ["grain_year", "grain_week"]
        else:  # "month"
            group_cols = ["grain_year", "grain_month"]
            order_cols = ["grain_year", "grain_month"]

        kpi = (
            df_g.groupBy(*group_cols)
                .agg(
                    F.countDistinct("order_id").alias("total_orders"),
                    F.sum("units_sold").alias("total_units_sold"),
                    F.sum("total_amount").alias("total_revenue"),
                    F.sum("order_profit").alias("gross_profit"),
                    F.sum("net_profit").alias("net_profit")
                ).fillna({
                    "total_units_sold": 0, 
                    "total_revenue": 0, 
                    "gross_profit": 0, 
                    "net_profit": 0
                })
                .withColumn(
                    "aov",
                    F.when(F.col("total_orders") > 0,
                        F.col("total_revenue") / F.col("total_orders"))
                    .otherwise(F.lit(0.0))
                )
                .withColumn(
                    "margin_pct",
                    F.when(F.col("total_revenue") > 0,
                        F.col("gross_profit") / F.col("total_revenue"))
                    .otherwise(F.lit(0.0))
                )
                .orderBy(*order_cols)
        )

        return kpi
    # Layer 1: Table Existence
    if "agg_orders" in dataframes:
        
        # Layer 2: Schema/Column Existence
        # We check for all columns required by the .agg() and .withColumn() operations
        required_columns = [
            "order_id", 
            "order_placed_at", 
            "units_sold", 
            "total_amount", 
            "order_profit", 
            "net_profit"
        ]
        
        if all(col in dataframes["agg_orders"].columns for col in required_columns):
            
            # Layer 3: Null/Zero Data Quality Check
            if (
                not is_column_all_null_or_zero(dataframes["agg_orders"], "order_placed_at") and
                not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id") and
                not is_column_all_null_or_zero(dataframes["agg_orders"], "total_amount") and
                not is_column_all_null_or_zero(dataframes["agg_orders"], "order_profit")
            ):
                # --- Analysis Logic Starts ---
                analysis["business_health_daily"] = core_kpis_over_time(
                    dataframes["agg_orders"], "order_placed_at", grain="day"
                )
                analysis["business_health_weekly"] = core_kpis_over_time(
                    dataframes["agg_orders"], "order_placed_at", grain="week"
                )
                analysis["business_health_monthly"] = core_kpis_over_time(
                    dataframes["agg_orders"], "order_placed_at", grain="month"
                )
                # --- Analysis Logic Ends ---
            else:
                print("Required metrics (dates, IDs, or amounts) are all NULL/zero; skipping business health analysis.")
        else:
            print("Missing required columns in agg_orders; skipping business health analysis.")
    else:
        print("missing agg_orders table; skipping business health analysis.")



    def analyze_category_margins(products_df):
    
        category_df = (
            products_df.groupBy("category")
                .agg(
                    F.avg("profit_margin").alias("avg_profit_margin"),
                    F.sum("total_profit").alias("total_category_profit"),
                    F.sum("total_revenue").alias("total_category_revenue"),
                    F.sum("total_units_sold").alias("units_sold")
                ).fillna({
                    "avg_profit_margin": 0,
                    "total_category_profit": 0,
                    "total_category_revenue": 0,
                    "units_sold": 0
                })
                .orderBy(F.col("avg_profit_margin").asc())
        )
        
        return category_df


    # Layer 1: Table Existence
    if "agg_products" in dataframes:
        
        # Layer 2: Schema/Column Existence
        if "category" in dataframes["agg_products"].columns and \
        "profit_margin" in dataframes["agg_products"].columns and \
        "total_profit" in dataframes["agg_products"].columns and \
        "total_revenue" in dataframes["agg_products"].columns and \
        "total_units_sold" in dataframes["agg_products"].columns:
            
            # Layer 3: Null/Zero Data Quality Check
            if (
                not is_column_all_null_or_zero(dataframes["agg_products"], "category") and
                not is_column_all_null_or_zero(dataframes["agg_products"], "profit_margin") and
                not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue")
            ):
                analysis["low_margin_categories"] = analyze_category_margins(dataframes["agg_products"])
            else:
                print("category, profit_margin, or total_revenue are all NULL/zero; skipping category margin analysis.")
        else:
            print("missing required columns in agg_products; skipping category margin analysis.")
    else:
        print("missing agg_products table; skipping category margin analysis.")



    def status_distribution_over_time(df,date_col, grain="day"):

        # Apply shared time-grain helper
        df_g = add_time_grain(df,  date_col="account_created_date", grain=grain)

        # Group/order columns based on grain
        if grain == "day":
            group_cols = ["grain_date"]
            order_cols = ["grain_date"]
        elif grain == "week":
            group_cols = ["grain_year", "grain_week"]
            order_cols = ["grain_year", "grain_week"]
        elif grain == "month":
            group_cols = ["grain_year", "grain_month"]
            order_cols = ["grain_year", "grain_month"]
        else:
            raise ValueError("grain must be 'day', 'week', or 'month'")

        return (
            df_g.groupBy(*(group_cols + ["account_status"]))
                .agg(F.countDistinct("customer_id").alias("customer_count"))
                .fillna({"customer_count": 0})
                .orderBy(*order_cols, "account_status")
        )

    # Layer 1: Table Existence
    if "agg_customers" in dataframes:
        
        # Layer 2: Schema/Column Existence
        if "customer_id" in dataframes["agg_customers"].columns and \
        "account_status" in dataframes["agg_customers"].columns and \
        "account_created_at" in dataframes["agg_customers"].columns:
            
            # Layer 3: Null/Zero Data Quality Check
            if (
                not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "account_status") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "account_created_at")
            ):
                analysis["customer_account_status_distribution_daily"] = status_distribution_over_time(
                    dataframes["agg_customers"], "account_created_at", grain="day"
                )
                analysis["customer_account_status_distribution_weekly"] = status_distribution_over_time(
                    dataframes["agg_customers"], "account_created_at", grain="week"
                )
                analysis["customer_account_status_distribution_monthly"] = status_distribution_over_time(
                    dataframes["agg_customers"], "account_created_at", grain="month"
                )
            else:
                print("customer_id, account_status, or account_created_at are all NULL/zero; skipping status distribution analysis.")
        else:
            print("missing required columns in agg_customers; skipping status distribution analysis.")
    else:
        print("missing agg_customers table; skipping status distribution analysis.")




    def new_customers(df, date_col="account_created_date", grain="day"):
        df_g = add_time_grain(df, date_col=date_col, grain=grain)

        if grain == "day":
            group_cols = ["grain_date"]
            order_cols = ["grain_date"]
        elif grain == "week":
            group_cols = ["grain_year", "grain_week"]
            order_cols = ["grain_year", "grain_week"]
        else:   # month
            group_cols = ["grain_year", "grain_month"]
            order_cols = ["grain_year", "grain_month"]

        new_df = (
            df_g.groupBy(*group_cols)
                .agg(F.countDistinct("customer_id").alias("new_customers"))
                .fillna({"new_customers": 0})
                .orderBy(*order_cols)
        )
        return new_df
    # Layer 1: Table Existence
    if "agg_customers" in dataframes:
        
        # Layer 2: Schema/Column Existence
        if "customer_id" in dataframes["agg_customers"].columns and \
        "account_created_date" in dataframes["agg_customers"].columns:
            if not is_column_all_null_or_zero(dataframes["agg_customers"], "account_created_date"):
                analysis["new_customers_daily"] = new_customers(dataframes["agg_customers"],"account_created_date", "day")
                analysis["new_customers_weekly"]  = new_customers(dataframes["agg_customers"], "account_created_date", "week")
                analysis["new_customers_monthly"]= new_customers(dataframes["agg_customers"], "account_created_date", "month")
            else:
                print("Account created at column is all NULL or zero; skipping new customers analysis.")
        else:
            print("missing required columns in agg_customers; skipping new customers analysis.")
    else:
        print("missing agg_customers table; skipping new customers analysis.")




    def cumulative_customers(df, date_col="account_created_at", grain="day"):
        new_df = new_customers(df, date_col, grain)

        # Define window by time order
        if grain == "day":
            window = Window.orderBy("grain_date") \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        elif grain == "week":
            window = Window.orderBy("grain_year", "grain_week") \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        else:   # month
            window = Window.orderBy("grain_year", "grain_month") \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

        cum_df = new_df.withColumn(
            "cumulative_customers",
            F.sum("new_customers").over(window)
        ).fillna({"cumulative_customers": 0})   
        
        return cum_df

    # Layer 1: Table Existence
    if "agg_customers" in dataframes:
        
        # Layer 2: Schema/Column Existence
        if "customer_id" in dataframes["agg_customers"].columns and \
        "account_created_date" in dataframes["agg_customers"].columns:
                    # Layer 3: Null/Zero Data Quality Check
            if not is_column_all_null_or_zero(dataframes["agg_customers"], "account_created_date"):
                analysis["cumulative_customers_daily"]   = cumulative_customers(dataframes["agg_customers"],"account_created_date", "day")
                analysis["cumulative_customers_weekly"]  = cumulative_customers(dataframes["agg_customers"], "account_created_date", "week")
                analysis["cumulative_customers_monthly"] = cumulative_customers(dataframes["agg_customers"], "account_created_date", "month")
            else:
                print("Account created at column is all NULL or zero; skipping cumulative customers analysis.")
        else:
            print("missing required columns in agg_customers; skipping cumulative customers analysis.")
    else:
        print("missing agg_customers table; skipping cumulative customers analysis.")





    def geo_acquisition_over_time(df, date_col="account_created_at", grain="day"):
        df_g = add_time_grain(df, date_col=date_col, grain=grain)

        if grain == "day":
            group_cols = ["grain_date", "country", "state_province", "city"]
            order_cols = ["grain_date", "country", "state_province", "city"]
        elif grain == "week":
            group_cols = ["grain_year", "grain_week", "country", "state_province", "city"]
            order_cols = ["grain_year", "grain_week", "country", "state_province", "city"]
        else:  # month
            group_cols = ["grain_year", "grain_month", "country", "state_province", "city"]
            order_cols = ["grain_year", "grain_month", "country", "state_province", "city"]

        return (
            df_g.groupBy(*group_cols)
                .agg(F.countDistinct("customer_id").alias("new_customers"))
                .fillna({"new_customers": 0})
                .orderBy(*order_cols)
        )
    # Layer 1: Table Existence
    if "agg_customers" in dataframes:
        
        # Layer 2: Schema/Column Existence
        if "customer_id" in dataframes["agg_customers"].columns and \
        "country" in dataframes["agg_customers"].columns and \
        "state_province" in dataframes["agg_customers"].columns and \
        "city" in dataframes["agg_customers"].columns and \
        "account_created_at" in dataframes["agg_customers"].columns:
            
            # Layer 3: Null/Zero Data Quality Check
            if (
                not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "country") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "account_created_at")
            ):
                # Static Geographic Acquisition
                analysis["geo_acquisition"] = (
                    dataframes["agg_customers"]
                    .groupBy("country", "state_province", "city")
                    .agg(F.countDistinct("customer_id").alias("new_customers"))
                )

                # Geographic Acquisition Over Time
                analysis["new_customers_geo_acquisition_daily"] = geo_acquisition_over_time(
                    dataframes["agg_customers"], "account_created_at", "day"
                )
                analysis["new_customers_geo_acquisition_monthly"] = geo_acquisition_over_time(
                    dataframes["agg_customers"], "account_created_at", "month"
                )
            else:
                print("customer_id, country, or account_created_at are all NULL/zero; skipping geo acquisition analysis.")
        else:
            print("missing required columns in agg_customers; skipping geo acquisition analysis.")
    else:
        print("missing agg_customers table; skipping geo acquisition analysis.")




    # Layer 1: Table Existence
    if "agg_customers" in dataframes:

        # --- Analysis 1: Age Group Distribution ---
        # Layer 2: Schema/Column Existence
        if "customer_id" in dataframes["agg_customers"].columns and \
        "customer_age_group" in dataframes["agg_customers"].columns:
            
            # Layer 3: Null/Zero Data Quality Check
            if (
                not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_age_group")
            ):
                analysis["customer_age_group_distribution"] = (
                    dataframes["agg_customers"]
                    .groupBy("customer_age_group")
                    .agg(F.countDistinct("customer_id").alias("customer_count"))
                    .fillna({"customer_count": 0})
                    .orderBy("customer_age_group")
                )
            else:
                print("customer_id or customer_age_group are all NULL/zero; skipping age group distribution analysis.")
        else:
            print("missing required columns in agg_customers; skipping age group distribution analysis.")


        if "customer_id" in dataframes["agg_customers"].columns and \
        "country" in dataframes["agg_customers"].columns and \
        "state_province" in dataframes["agg_customers"].columns and \
        "city" in dataframes["agg_customers"].columns:

            # Layer 3: Null/Zero Data Quality Check
            # City Level
            if (
                not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "country") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "state_province") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "city")
            ):
                analysis["customer_city_distribution"] = (
                    dataframes["agg_customers"]
                    .groupBy("country", "state_province", "city")
                    .agg(F.countDistinct("customer_id").alias("customer_count"))
                    .fillna({"customer_count": 0})
                    .orderBy("country", "state_province", "city")
                )
            else:
                print("Geographic metrics (city/state) are all NULL/zero; skipping city distribution analysis.")

            # State Level
            if (
                not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "country") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "state_province")
            ):
                analysis["customer_state_distribution"] = (
                    dataframes["agg_customers"]
                    .groupBy("country", "state_province")
                    .agg(F.countDistinct("customer_id").alias("customer_count"))
                    .fillna({"customer_count": 0})
                    .orderBy("country", "state_province")
                )
            else:
                print("Country or state_province are all NULL/zero; skipping state distribution analysis.")

            # Country Level
            if (
                not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "country")
            ):
                analysis["customer_country_distribution"] = (
                    dataframes["agg_customers"]
                    .groupBy("country")
                    .agg(F.countDistinct("customer_id").alias("customer_count"))
                    .fillna({"customer_count": 0})
                    .orderBy("country")
                )
            else:
                print("Country column is all NULL/zero; skipping country distribution analysis.")

        else:
            print("missing geographic columns in agg_customers; skipping geographic distribution analysis.")

    else:
        print("missing agg_customers table; skipping demographic and geographic distribution analysis.")




    if "agg_customers" in dataframes:

        if "customer_id" in dataframes["agg_customers"].columns and \
        "customer_age_group" in dataframes["agg_customers"].columns and \
        "order_total_spent" in dataframes["agg_customers"].columns and \
        "customer_lifetime_value" in dataframes["agg_customers"].columns and \
        "total_revenue" in dataframes["agg_customers"].columns:

            if (
                not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_age_group") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "order_total_spent") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value") and
                not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue")
                    ):
                analysis["customer_age_group_spending"] = (
                    dataframes["agg_customers"]
                    .groupBy("customer_age_group")
                    .agg(
                        F.countDistinct("customer_id").alias("customer_count"),
                        F.avg("order_total_spent").alias("avg_order_total_spent"),
                        F.avg("customer_lifetime_value").alias("avg_clv"),
                        F.sum("order_total_spent").alias("total_spent"),
                        F.sum("total_revenue").alias("total_revenue_age_group")
                    ).fillna({"avg_order_total_spent": 0,
                            "avg_clv": 0,
                            "total_spent": 0,
                            "total_revenue_age_group": 0})
                    .orderBy("customer_age_group")
                )
            else:
                print("Required metrics (Age Group, Spent, CLV, or Revenue) are all NULL/zero; skipping age group spending analysis.")
        else:
            print("Missing required columns in agg_customers; skipping age group spending analysis.")
    else:
        print("missing agg_customers table; skipping age group spending analysis.")



    if "agg_orders" in dataframes and "agg_customers" in dataframes and "agg_order_items" in dataframes and "agg_products" in dataframes:

        if "order_id" in dataframes["agg_orders"].columns and \
        "customer_id" in dataframes["agg_orders"].columns and \
        "customer_id" in dataframes["agg_customers"].columns and \
        "gender" in dataframes["agg_customers"].columns:

            if not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id") and \
            not is_column_all_null_or_zero(dataframes["agg_orders"], "customer_id") and \
            not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id"):
                
                cust_orders = (
                    dataframes["agg_orders"]
                    .select("order_id", "customer_id")
                    .join(
                        dataframes["agg_customers"].select("customer_id", "gender"),
                        on="customer_id",
                        how="inner"
                    )
                )

                if "order_id" in dataframes["agg_order_items"].columns and \
                "product_id" in dataframes["agg_order_items"].columns and \
                "quantity" in dataframes["agg_order_items"].columns and \
                "product_id" in dataframes["agg_products"].columns and \
                "product_name" in dataframes["agg_products"].columns and \
                "category" in dataframes["agg_products"].columns:

                    if not is_column_all_null_or_zero(dataframes["agg_order_items"], "order_id") and \
                    not is_column_all_null_or_zero(dataframes["agg_order_items"], "product_id") and \
                    not is_column_all_null_or_zero(dataframes["agg_products"], "product_id"):

                        cust_order_items = (
                            cust_orders
                            .join(dataframes["agg_order_items"].select("order_id", "product_id", "quantity"), on="order_id", how="inner")
                            .join(dataframes["agg_products"].select("product_id", "product_name", "category", "sub_category", "brand"),
                                on="product_id",
                                how="left")
                        )

                        if not is_column_all_null_or_zero(cust_order_items, "gender") and \
                        not is_column_all_null_or_zero(cust_order_items, "category") and \
                        not is_column_all_null_or_zero(cust_order_items, "quantity"):

                            analysis["gender_category_preference"] = (
                                cust_order_items
                                .groupBy("gender", "category")
                                .agg(
                                    F.sum("quantity").alias("total_units"),
                                    F.countDistinct("product_id").alias("distinct_products"),
                                    F.countDistinct("order_id").alias("orders_count")
                                )
                                .fillna({"total_units": 0, "distinct_products": 0, "orders_count": 0, "gender": "Unknown"})
                                .orderBy("gender", F.col("total_units").desc())
                            )

                            analysis["gender_product_preference"] = (
                                cust_order_items
                                .groupBy("gender", "product_id", "product_name", "category")
                                .agg(
                                    F.sum("quantity").alias("total_units"),
                                    F.countDistinct("order_id").alias("orders_count")
                                ).fillna({"total_units": 0, "orders_count": 0, "gender": "Unknown"})
                                .orderBy("gender", F.col("total_units").desc())
                            )
                        else:
                            print("Required columns in joined customer order items are NULL/zero; skipping preference analysis.")
                    else:
                        print("Order items or products data quality check failed; skipping customer order items join.")
                else:
                    print("Missing required columns in order_items or products; skipping customer order items join.")
            else:
                print("Order ID or Customer ID data quality check failed; skipping customer orders join.")
        else:
            print("Missing required columns in orders or customers; skipping customer orders join.")
    else:
        print("Missing one or more required tables; skipping gender preference analysis.")



    if "agg_customers" in dataframes:

        if "is_repeat_customer" in dataframes["agg_customers"].columns:

            if not is_column_all_null_or_zero(dataframes["agg_customers"], "is_repeat_customer"):
                dataframes["agg_customers"] = dataframes["agg_customers"].withColumn(
                    "customer_type",
                    F.when(F.col("is_repeat_customer") == 1, F.lit("returning"))
                    .otherwise(F.lit("new"))
                )

                if "customer_id" in dataframes["agg_customers"].columns and \
                "customer_type" in dataframes["agg_customers"].columns and \
                "country" in dataframes["agg_customers"].columns and \
                "state_province" in dataframes["agg_customers"].columns and \
                "city" in dataframes["agg_customers"].columns:

                    if not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and \
                    not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_type") and \
                    not is_column_all_null_or_zero(dataframes["agg_customers"], "country"):

                        analysis["new_vs_returning_customer_country"] = (
                            dataframes["agg_customers"]
                            .groupBy("country", "customer_type")
                            .agg(F.countDistinct("customer_id").alias("customer_count"))
                            .fillna({"customer_count": 0})
                            .orderBy("country", "customer_type")
                        )

                        if not is_column_all_null_or_zero(dataframes["agg_customers"], "state_province") and \
                        not is_column_all_null_or_zero(dataframes["agg_customers"], "city"):
                            
                            analysis["new_vs_returning_customer_city"] = (
                                dataframes["agg_customers"]
                                .groupBy("country", "state_province", "city", "customer_type")
                                .agg(F.countDistinct("customer_id").alias("customer_count"))
                                .orderBy("country", "state_province", "city", "customer_type")
                            )

                        if not is_column_all_null_or_zero(dataframes["agg_customers"], "state_province"):
                            
                            analysis["new_vs_returning_customer_state"] = (
                                dataframes["agg_customers"]
                                .groupBy("country", "state_province", "customer_type")
                                .agg(F.countDistinct("customer_id").alias("customer_count"))
                                .fillna({"customer_count": 0})
                                .orderBy("country", "state_province", "customer_type")
                            )
                    else:
                        print("customer_id, customer_type, or country are all NULL/zero; skipping geographic customer type analysis.")
                else:
                    print("Missing required columns in agg_customers; skipping geographic customer type analysis.")
            else:
                print("is_repeat_customer column is all NULL or zero; skipping customer type classification.")
        else:
            print("missing is_repeat_customer column in agg_customers; skipping customer type classification.")
    else:
        print("missing agg_customers table; skipping new vs returning customer analysis.")



    if "agg_customers" in dataframes:

        if "customer_id" in dataframes["agg_customers"].columns and \
        "total_sessions" in dataframes["agg_customers"].columns and \
        "total_pages_viewed" in dataframes["agg_customers"].columns and \
        "total_products_viewed" in dataframes["agg_customers"].columns:

            if not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and \
            not is_column_all_null_or_zero(dataframes["agg_customers"], "total_sessions") and \
            not is_column_all_null_or_zero(dataframes["agg_customers"], "total_pages_viewed") and \
            not is_column_all_null_or_zero(dataframes["agg_customers"], "total_products_viewed"):

                analysis["customer_engagement"] = dataframes["agg_customers"].select(
                    "customer_id",
                    "total_sessions",
                    "total_pages_viewed",
                    "total_products_viewed"
                )

                analysis["customer_engagement_summary"] = dataframes["agg_customers"].agg(
                    F.sum("total_sessions").alias("total_sessions_all_customers"),
                    F.avg("total_sessions").alias("avg_sessions_per_customer"),
                    F.sum("total_pages_viewed").alias("total_pages_viewed_all_customers"),
                    F.avg("total_pages_viewed").alias("avg_pages_viewed_per_customer"),
                    F.sum("total_products_viewed").alias("total_products_viewed_all_customers"),
                    F.avg("total_products_viewed").alias("avg_products_viewed_per_customer")
                ).fillna({
                    "total_sessions_all_customers": 0,
                    "avg_sessions_per_customer": 0,
                    "total_pages_viewed_all_customers": 0,
                    "avg_pages_viewed_per_customer": 0,
                    "total_products_viewed_all_customers": 0,
                    "avg_products_viewed_per_customer": 0
                })
            else:
                print("Required engagement columns are all NULL/zero; skipping engagement analysis.")
        else:
            print("Missing required columns in agg_customers; skipping engagement analysis.")
    else:
        print("missing agg_customers table; skipping engagement analysis.")



    if "agg_customers" in dataframes:

        if "session_conversion_rate" in dataframes["agg_customers"].columns and \
        "cart_abandonment_rate" in dataframes["agg_customers"].columns:

            if not is_column_all_null_or_zero(dataframes["agg_customers"], "session_conversion_rate") and \
            not is_column_all_null_or_zero(dataframes["agg_customers"], "cart_abandonment_rate"):

                analysis["session_to_order_analysis"] = dataframes["agg_customers"].agg(
                    F.avg("session_conversion_rate").alias("avg_session_conversion_rate"),
                    F.avg("cart_abandonment_rate").alias("avg_cart_abandonment_rate")
                ).fillna({
                    "avg_session_conversion_rate": 0,
                    "avg_cart_abandonment_rate": 0
                })
            else:
                print("session_conversion_rate or cart_abandonment_rate are all NULL/zero; skipping session to order analysis.")
        else:
            print("Missing required columns in agg_customers; skipping session to order analysis.")
    else:
        print("missing agg_customers table; skipping session to order analysis.")


    if "agg_customers" in dataframes:

        if "customer_id" in dataframes["agg_customers"].columns and \
        "total_revenue" in dataframes["agg_customers"].columns and \
        "customer_lifetime_value" in dataframes["agg_customers"].columns and \
        "customer_segment" in dataframes["agg_customers"].columns and \
        "rfm_segment" in dataframes["agg_customers"].columns:

            if not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue") and \
            not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value") and \
            not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_segment") and \
            not is_column_all_null_or_zero(dataframes["agg_customers"], "rfm_segment"):

                analysis["top_customers_by_revenue"] = (
                    dataframes["agg_customers"]
                    .fillna({"total_revenue": 0.0})
                    .select("customer_id", "total_revenue", "customer_lifetime_value", "customer_segment", "rfm_segment")
                    .orderBy(F.col("total_revenue").desc())
                )
            else:
                print("Required customer metrics (Revenue, CLV, or Segments) are all NULL/zero; skipping top customers analysis.")
        else:
            print("Missing required columns in agg_customers; skipping top customers analysis.")
    else:
        print("missing agg_customers table; skipping top customers analysis.")


    if "agg_orders" in dataframes and "agg_customers" in dataframes:

        if "order_profit" in dataframes["agg_orders"].columns and \
        "net_profit" in dataframes["agg_orders"].columns and \
        "customer_id" in dataframes["agg_orders"].columns and \
        "order_id" in dataframes["agg_orders"].columns and \
        "customer_id" in dataframes["agg_customers"].columns and \
        "customer_segment" in dataframes["agg_customers"].columns and \
        "rfm_segment" in dataframes["agg_customers"].columns and \
        "total_revenue" in dataframes["agg_customers"].columns and \
        "customer_lifetime_value" in dataframes["agg_customers"].columns:

            if not is_column_all_null_or_zero(dataframes["agg_orders"], "order_profit") and \
            not is_column_all_null_or_zero(dataframes["agg_orders"], "net_profit") and \
            not is_column_all_null_or_zero(dataframes["agg_orders"], "customer_id") and \
            not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and \
            not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue"):

                customer_profit = (
                    dataframes["agg_orders"]
                    .groupBy("customer_id")
                    .agg(
                        F.sum("order_profit").alias("total_order_profit"),
                        F.sum("net_profit").alias("total_net_profit"),
                        F.countDistinct("order_id").alias("orders_count")
                    )
                )

                customer_profit_enriched = (
                    customer_profit.alias("p")
                    .join(
                        dataframes["agg_customers"].select(
                            "customer_id", 
                            "customer_segment", 
                            "rfm_segment", 
                            "total_revenue", 
                            "customer_lifetime_value"
                        ).alias("c"),
                        on="customer_id",
                        how="left"
                    )
                )

                analysis["top_customers_by_profit"] = (
                    customer_profit_enriched
                    .orderBy(F.col("total_net_profit").desc_nulls_last())
                )
            else:
                print("Required columns in agg_orders or agg_customers are all NULL or zero; skipping top customers by profit analysis.")
        else:
            print("Missing required columns in agg_orders or agg_customers; skipping top customers by profit analysis.")
    else:
        print("Missing agg_orders or agg_customers table; skipping top customers by profit analysis.")


    if "agg_customers" in dataframes:

        if "customer_id" in dataframes["agg_customers"].columns and \
        "session_conversion_rate" in dataframes["agg_customers"].columns:

            if not is_column_all_null_or_zero(dataframes["agg_customers"], "session_conversion_rate"):
                conv_percentage = dataframes["agg_customers"].withColumn(
                    "session_conversion_percentage",
                    F.when(F.col("session_conversion_rate") < 0.1, "<10%")
                    .when(F.col("session_conversion_rate") < 0.25, "10–25%")
                    .when(F.col("session_conversion_rate") < 0.5, "25–50%")
                    .when(F.col("session_conversion_rate") < 0.75, "50–75%")
                    .otherwise("75%+")
                )
                analysis["session_conversion_distribution"] = (
                    conv_percentage
                    .groupBy("session_conversion_percentage")
                    .agg(F.countDistinct("customer_id").alias("customer_count"))
                    .fillna({"customer_count": 0})
                    .orderBy("session_conversion_percentage")
                )
            else:
                print("session_conversion_rate column is all NULL or zero; skipping session conversion percentage calculation.")
        else:
            print("Missing session_conversion_rate or customer_id in agg_customers; skipping conversion distribution.")

        if "customer_id" in dataframes["agg_customers"].columns and \
        "cart_abandonment_rate" in dataframes["agg_customers"].columns:

            if not is_column_all_null_or_zero(dataframes["agg_customers"], "cart_abandonment_rate"):
                abandon_percentage = dataframes["agg_customers"].withColumn(
                    "cart_abandonment_percentage",
                    F.when(F.col("cart_abandonment_rate") < 0.1, "<10%")
                    .when(F.col("cart_abandonment_rate") < 0.25, "10–25%")
                    .when(F.col("cart_abandonment_rate") < 0.5, "25–50%")
                    .when(F.col("cart_abandonment_rate") < 0.75, "50–75%")
                    .otherwise("75%+")
                )

                analysis["cart_abandonment_distribution"] = (
                    abandon_percentage
                    .groupBy("cart_abandonment_percentage")
                    .agg(F.countDistinct("customer_id").alias("customer_count"))
                    .fillna({"customer_count": 0})
                    .orderBy("cart_abandonment_percentage")
                )
            else:
                print("cart_abandonment_rate column is all NULL or zero; skipping cart abandonment percentage calculation.")
        else:
            print("Missing cart_abandonment_rate or customer_id in agg_customers; skipping abandonment distribution.")

    else:
        print("missing agg_customers table; skipping conversion and abandonment distribution analysis.")



    if "agg_customers" in dataframes:
        if (
            not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value")
            and not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue")
            and not is_column_all_null_or_zero(dataframes["agg_customers"], "avg_order_value")
            and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id")
        ):
            analysis["clv_summary"] = (
                dataframes["agg_customers"]
                .agg(
                    F.countDistinct("customer_id").alias("customers"),
                    F.avg("customer_lifetime_value").alias("avg_clv"),
                    F.expr(
                        "percentile_approx(customer_lifetime_value, array(0.25, 0.5, 0.75))"
                    ).alias("clv_percentiles"),
                    F.avg("total_revenue").alias("avg_total_revenue"),
                    F.avg("avg_order_value").alias("avg_order_value_overall"),
                    F.sum("total_revenue").alias("total_revenue_all_customers"),
                )
                # only fill scalar columns; leave clv_percentiles as-is
                .fillna({
                    "customers": 0,
                    "avg_clv": 0.0,
                    "avg_total_revenue": 0.0,
                    "avg_order_value_overall": 0.0,
                    "total_revenue_all_customers": 0.0,
                })
            )
        else:
            print(
                "One of the required columns in agg_customers is all NULL or zero; skipping CLV summary calculation."
            )
    else:
        print("missing agg_customers table; skipping CLV summary calculation.")



    def revenue_by_segment(df, group_cols):
        grouped = (
            df.groupBy(*group_cols)
            .agg(
                F.countDistinct("customer_id").alias("customer_count"),
                F.sum("total_revenue").alias("segment_revenue")
            )
        )

        total_rev = grouped.agg(F.sum("segment_revenue").alias("total_revenue_all")).first()[0] or 0.0

        result = (
            grouped
            .withColumn(
                "revenue_per_customer",
                F.when(F.col("customer_count") > 0,
                    F.col("segment_revenue") / F.col("customer_count"))
                .otherwise(F.lit(0.0))
            )
            .withColumn(
                "revenue_share",
                F.when(F.lit(total_rev) > 0,
                    F.col("segment_revenue") / F.lit(total_rev))
                .otherwise(F.lit(0.0))
            )
            .orderBy(F.col("segment_revenue").desc_nulls_last())
        )
        return result
    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue"):
            analysis["rev_by_country_city"] = revenue_by_segment(dataframes["agg_customers"], ["country", "city"])
            analysis["rev_by_customer_segment"] = revenue_by_segment(dataframes["agg_customers"], ["customer_segment"])
            analysis["rev_by_rfm_segment"] = revenue_by_segment(dataframes["agg_customers"], ["rfm_segment"])
            analysis["rev_by_segment_label"] = revenue_by_segment(dataframes["agg_customers"], ["customer_segment_label"])
            analysis["rev_by_referrer"] = revenue_by_segment(dataframes["agg_customers"], ["preferred_referrer_source"])
            analysis["rev_by_device"] = revenue_by_segment(dataframes["agg_customers"], ["preferred_device_type"])
        else:
            print("One of the required columns in agg_customers is all NULL or zero; skipping revenue by segment analysis.")
    else:
        print("agg_customers table is missing")



    if (
        "agg_daily_aggregations" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_daily_aggregations"], "order_date")
        and not is_column_all_null_or_zero(dataframes["agg_daily_aggregations"], "avg_order_value")
        and not is_column_all_null_or_zero(dataframes["agg_daily_aggregations"], "total_orders")
        and not is_column_all_null_or_zero(dataframes["agg_daily_aggregations"], "total_revenue")
    ):
        daily = dataframes["agg_daily_aggregations"].fillna(
            {
                "avg_order_value": 0.0,
                "total_orders": 0,
                "total_revenue": 0.0,
            }
        )

        analysis["aov_trend_daily"] = (
            daily.select(
                "order_date",
                "order_year",
                "order_month",
                "total_orders",
                "total_revenue",
                "avg_order_value",
            )
            .orderBy(F.col("order_date").asc())
        )
    else:
        print(
            "agg_daily_aggregations not available, or order_date, avg_order_value, total_orders, or total_revenue all NULL/zero; "
            "skipping daily AOV trend."
        )

    # 2) Weekly AOV trend
    if (
        "agg_weekly_aggregations" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_weekly_aggregations"], "year_week")
        and not is_column_all_null_or_zero(dataframes["agg_weekly_aggregations"], "avg_order_value")
        and not is_column_all_null_or_zero(dataframes["agg_weekly_aggregations"], "total_orders")
        and not is_column_all_null_or_zero(dataframes["agg_weekly_aggregations"], "total_revenue")
    ):
        weekly = dataframes["agg_weekly_aggregations"].fillna(
            {
                "avg_order_value": 0.0,
                "total_orders": 0,
                "total_revenue": 0.0,
            }
        )

        analysis["aov_trend_weekly"] = (
            weekly.select(
                "year_week",
                "order_year",
                "order_week",
                "total_orders",
                "total_revenue",
                "avg_order_value",
            )
            .orderBy(F.col("order_year").asc(), F.col("order_week").asc())
        )
    else:
        print(
            "agg_weekly_aggregations not available, or year_week all NULL/zero; "
            "skipping weekly AOV trend."
        )

    # 3) Monthly AOV trend
    if (
        "agg_monthly_aggregations" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_monthly_aggregations"], "year_month")
        and not is_column_all_null_or_zero(dataframes["agg_monthly_aggregations"], "avg_order_value")
        and not is_column_all_null_or_zero(dataframes["agg_monthly_aggregations"], "total_orders")
        and not is_column_all_null_or_zero(dataframes["agg_monthly_aggregations"], "total_revenue")

    ):
        monthly = dataframes["agg_monthly_aggregations"].fillna(
            {
                "avg_order_value": 0.0,
                "total_orders": 0,
                "total_revenue": 0.0,
            }
        )

        analysis["aov_trend_monthly"] = (
            monthly.select(
                "year_month",
                "order_year",
                "order_month",
                "total_orders",
                "total_revenue",
                "avg_order_value",
            )
            .orderBy(F.col("order_year").asc(), F.col("order_month").asc())
        )
    else:
        print(
            "agg_monthly_aggregations not available, or year_month, avg_order_value, total_orders, or total_revenue all NULL/zero; "
            "skipping monthly AOV trend."
        )


    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "total_discount_received") and not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue") and not is_column_all_null_or_zero(dataframes["agg_customers"], "avg_discount_per_order") and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value") and not is_column_all_null_or_zero(dataframes["agg_customers"], "total_orders"):
            disc_df = (
                dataframes["agg_customers"]
                .fillna({
                    "total_discount_received": 0.0,
                    "total_revenue": 0.0,
                    "avg_discount_per_order": 0.0,
                    "customer_lifetime_value": 0.0,
                    "total_orders": 0
                })
                .withColumn(
                    "discount_share_of_revenue",
                    F.when(F.col("total_revenue") > 0,
                        F.col("total_discount_received") / F.col("total_revenue"))
                    .otherwise(F.lit(0.0))
                )
            )
            analysis["discount_customers"] = (
                disc_df
                .withColumn(
                    "is_discount_hunter",
                    F.when(
                        (F.col("discount_share_of_revenue") >= 0.3) &
                        (F.col("total_orders") >= 3),
                        F.lit(1)
                    ).otherwise(F.lit(0))
                )
            )
            analysis["discount_customers_summary"] = (
                analysis["discount_customers"]
                .groupBy("is_discount_hunter")
                .agg(
                    F.countDistinct("customer_id").alias("customer_count"),
                    F.avg("discount_share_of_revenue").alias("avg_discount_share"),
                    F.avg("avg_discount_per_order").alias("avg_discount_per_order"),
                    F.avg("customer_lifetime_value").alias("avg_clv"),
                    F.avg("total_revenue").alias("avg_revenue")
                ).fillna({
                    "customer_count": 0,
                    "avg_discount_share": 0.0,
                    "avg_discount_per_order": 0.0,
                    "avg_clv": 0.0,
                    "avg_revenue": 0.0
                })
            )

            analysis["correlation_discount_vs_clv"] = disc_df.select(
            F.corr("discount_share_of_revenue", "customer_lifetime_value").alias("corr_discount_share_clv"),
            F.corr("avg_discount_per_order", "customer_lifetime_value").alias("corr_avg_discount_clv")
            )

        else:
            print("analysis skipped because one of the above columns is missing")
    else:
        print("missing agg_customers table; skipping discount customer analysis.")



    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "total_discount_received") and not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue") and not is_column_all_null_or_zero(dataframes["agg_customers"], "avg_discount_per_order") and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value") and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id"):
            discount_behavior = (
                dataframes["agg_customers"]
                .select(
                    "customer_id",
                    "total_revenue",
                    "total_discount_received",
                    (F.col("total_discount_received") / F.col("total_revenue")).alias("discount_to_revenue_ratio"),
                    "avg_discount_per_order",
                    "customer_lifetime_value"
                )
            )

            analysis["high_discount_customers"] = (
                discount_behavior
                .filter(F.col("discount_to_revenue_ratio") > 0.3)  # threshold example
                .orderBy(F.col("discount_to_revenue_ratio").desc())
            )
        else:
            print("One of the required columns in agg_customers is all NULL or zero; skipping discount behavior analysis.")
    else:
        print("missing agg_customers table; skipping discount behavior analysis.")




    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "total_carts_created") and not is_column_all_null_or_zero(dataframes["agg_customers"], "total_abandoned_carts") and not is_column_all_null_or_zero(dataframes["agg_customers"], "total_purchased_carts") and not is_column_all_null_or_zero(dataframes["agg_customers"], "cart_abandonment_rate") and not is_column_all_null_or_zero(dataframes["agg_customers"], "total_abandoned_value") and not is_column_all_null_or_zero(dataframes["agg_customers"], "avg_time_in_cart_days"):
            analysis["cart_behavior_summary"] = (
                dataframes["agg_customers"]
                .agg(
                    F.sum("total_carts_created").alias("total_carts_created"),
                    F.sum("total_abandoned_carts").alias("total_abandoned_carts"),
                    F.sum("total_purchased_carts").alias("total_purchased_carts"),
                    F.avg("cart_abandonment_rate").alias("avg_cart_abandonment_rate"),
                    F.sum("total_abandoned_value").alias("total_abandoned_value"),
                    F.avg("avg_time_in_cart_days").alias("avg_time_in_cart_days")
                )
            )

            analysis["high_value_abandoners"] = (
                dataframes["agg_customers"]
                .select(
                    "customer_id",
                    "total_abandoned_carts",
                    "total_abandoned_value",
                    "cart_abandonment_rate",
                    "total_revenue",
                    "customer_lifetime_value"
                )
                .filter(F.col("total_abandoned_value") > 0)
                .orderBy(F.col("total_abandoned_value").desc())
            )
        else:
            print("One of the required columns in agg_customers is all NULL or zero; skipping cart behavior analysis.")
    else:
        print("missing agg_customers table; skipping cart behavior analysis.")
        

    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "churn_risk") and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue") and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value") and not is_column_all_null_or_zero(dataframes["agg_customers"], "order_recency_days"):
            analysis["churn_risk_summary"] = (
                dataframes["agg_customers"]
                .groupBy("churn_risk")
                .agg(
                    F.countDistinct("customer_id").alias("num_customers"),
                    F.sum("total_revenue").alias("total_revenue"),
                    F.avg("customer_lifetime_value").alias("avg_clv"),
                    F.avg("order_recency_days").alias("avg_recency_days")
                )
                .orderBy("churn_risk")
            )
        else:
            print("One of the required columns in agg_customers is all NULL or zero; skipping churn risk analysis.")
    else:
        print("missing agg_customers table; skipping churn risk analysis.") 



    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "churn_risk") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_activity_score") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "order_recency_days") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "days_since_last_purchase") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "days_since_last_login") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_segment_label") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "rfm_segment") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "churn_risk") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "rfm_category"):

            analysis["high_clv_at_risk"] = (
                dataframes["agg_customers"]
                .select(
                    "customer_id",
                    "customer_lifetime_value",
                    "churn_risk",
                    "customer_activity_score",
                    "order_recency_days",
                    "days_since_last_purchase",
                    "days_since_last_login",
                    "customer_segment_label",
                    "rfm_segment",
                    "rfm_category"
                )
                .filter(
                    (F.col("churn_risk").rlike("^(?i)(medium|high)")) &
                    (F.col("customer_lifetime_value") > 0)
                )
                .orderBy(F.col("customer_lifetime_value").desc())
            )
        else:
            print("analysis skipped because one of the required columns in agg_customers is all NULL or zero.")
    else:
        print("missing agg_customers table; skipping high CLV at risk analysis.")



    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "rfm_segment") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "order_recency_days") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "total_orders") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "avg_order_value"):
            
            analysis["rfm_segment_summary"] = (
                dataframes["agg_customers"]
                .groupBy("rfm_segment")
                .agg(
                    F.countDistinct("customer_id").alias("num_customers"),
                    F.sum("total_revenue").alias("total_revenue"),
                    F.avg("customer_lifetime_value").alias("avg_clv"),
                    F.avg("order_recency_days").alias("avg_recency_days"),
                    F.avg("total_orders").alias("avg_total_orders"),
                    F.avg("avg_order_value").alias("avg_aov")
                )
                .orderBy(F.col("total_revenue").desc())
            )
        else:
            print("analysis skipped because one of the required columns in agg_customers is all NULL or zero.")
    else:
        print("missing agg_customers table; skipping RFM segment summary analysis.")


    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id"):
            
            analysis["high_intent_non_buyers"] = (
                dataframes["agg_customers"]
                .select(
                    "customer_id",
                    "total_products_viewed",
                    "wishlist_items_count",
                    "total_carts_created",
                    "total_purchased_carts",
                    "cart_abandonment_rate",
                    "session_conversion_rate"
                )
                .filter(
                    (F.col("total_revenue") == 0) &
                    (
                        (F.col("total_products_viewed") > 10) |
                        (F.col("wishlist_items_count") > 5) |
                        (F.col("total_carts_created") > 3)
                    )
                )
                .orderBy(F.col("total_products_viewed").desc())
            )
        else:
            print("analysis skipped because one of the required columns in agg_customers is all NULL or zero.")
    else:
        print("missing agg_customers table; skipping high intent non-buyers analysis.")



    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "account_created_at") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "first_order_date"):
            
            customers_with_first_order = (
                dataframes["agg_customers"]
                .select(
                    "customer_id",
                    F.to_date("account_created_at").alias("signup_date"),
                    F.to_date("first_order_date").alias("first_order_date")
                )
                .filter(F.col("first_order_date").isNotNull())
            )

            analysis["customers_cohorts"] = (
                customers_with_first_order
                .withColumn(
                    "signup_cohort_month",
                    F.date_trunc("month", F.col("signup_date")).cast("date")
                )
                .withColumn(
                    "first_order_month",
                    F.date_trunc("month", F.col("first_order_date")).cast("date")
                )
            )

            analysis["signup_cohort_summary"] = (
                analysis["customers_cohorts"]
                .groupBy("signup_cohort_month")
                .agg(
                    F.countDistinct("customer_id").alias("cohort_customers")
                )
            )
        else:
            print("analysis skipped because account_created_at or first_order_date is all NULL or zero.")
    else:
        print("missing agg_customers table; skipping signup cohort prep.")



    if "agg_orders" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_orders"], "order_placed_at") and \
        not is_column_all_null_or_zero(dataframes["agg_orders"], "customer_id") and \
        analysis.get("customers_cohorts") is not None:
            
            orders_with_month = (
                dataframes["agg_orders"]
                .select(
                    "order_id",
                    "customer_id",
                    F.date_trunc("month", "order_placed_at").cast("date").alias("order_month")
                )
                .filter(F.col("order_month").isNotNull())
            )

            orders_with_cohort = (
                orders_with_month
                .join(
                    analysis["customers_cohorts"].select("customer_id", "signup_cohort_month"),
                    on="customer_id",
                    how="inner"
                )
            )

            orders_with_relative_month = (
                orders_with_cohort
                .withColumn(
                    "months_since_signup",
                    (
                        (F.month("order_month") - F.month("signup_cohort_month"))
                        + 12 * (F.year("order_month") - F.year("signup_cohort_month"))
                    ).cast("int")
                )
                .filter(F.col("months_since_signup") >= 0)
            )

            analysis["customer_cohort_retention"] = (
                orders_with_relative_month
                .select("signup_cohort_month", "customer_id", "months_since_signup")
                .dropDuplicates(["signup_cohort_month", "customer_id", "months_since_signup"])
                .groupBy("signup_cohort_month", "months_since_signup")
                .agg(
                    F.countDistinct("customer_id").alias("active_customers")
                )
                .join(
                    analysis["signup_cohort_summary"],
                    on="signup_cohort_month",
                    how="left"
                )
                .withColumn(
                    "retention_rate",
                    F.when(
                        F.col("cohort_customers") > 0,
                        F.col("active_customers") / F.col("cohort_customers")
                    ).otherwise(F.lit(0.0))
                )
                .orderBy("signup_cohort_month", "months_since_signup")
            )
        else:
            print("analysis skipped because order_placed_at, customer_id, or cohort data is missing/null.")
    else:
        print("missing agg_orders table; skipping monthly cohort retention.")


    required_cols_360 = [
        "customer_id",
        "account_created_at",
        "account_status",
        "is_active",
        "is_repeat_customer",
        "total_orders",
        "total_items_purchased",
        "total_cancelled_orders",
        "total_reviews_written",
        "total_sessions",
        "total_pages_viewed",
        "total_products_viewed",
        "wishlist_items_count",
        "total_carts_created",
        "total_abandoned_carts",
        "total_purchased_carts",
        "order_frequency",
        "gender",
        "customer_age_group",
        "city",
        "state_province",
        "country",
        "order_recency_days",
        "customer_tenure_days",
        "days_since_last_login",
        "customer_activity_status",
        "customer_segment",
        "customer_segment_label",
        "rfm_segment",
        "rfm_category",
        "churn_risk",
        "customer_lifetime_value",
        "total_revenue",
        "avg_order_value",
        "avg_items_per_order",
        "total_discount_received",
        "avg_discount_per_order",
        "avg_session_duration",
        "session_conversion_rate",
        "cart_abandonment_rate",
        "preferred_device_type",
        "preferred_referrer_source",
        "preferred_payment_method",
        "wishlist_conversion_rate",
        "days_since_last_purchase",
        "cancellation_rate",
        "customer_activity_score",
        "total_abandoned_value",
        "avg_time_in_cart_days",
        "customer_abandonment_rate",
        "customer_purchase_rate",
    ]

    if "agg_customers" in dataframes:
        missing_for_360 = [c for c in required_cols_360 if c not in dataframes["agg_customers"].columns]
        if missing_for_360:
            print(f"Some 360 columns are missing in agg_customers: {missing_for_360}")
        
        available_cols_360 = [c for c in required_cols_360 if c in dataframes["agg_customers"].columns]
        
        analysis["customer_overall_health_summary"] = dataframes["agg_customers"].select(*available_cols_360)
    else:
        print("agg_customers dataframe not found; skipping customer 360 table.")


    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "rfm_segment") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "churn_risk") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value"):
            
            analysis["rfm_churn_crosstab"] = (
                dataframes["agg_customers"]
                .groupBy("rfm_segment", "churn_risk")
                .agg(
                    F.countDistinct("customer_id").alias("customer_count"),
                    F.sum("total_revenue").alias("segment_revenue"),
                    F.avg("customer_lifetime_value").alias("avg_clv")
                )
                .orderBy("rfm_segment", "churn_risk")
            )
        else:
            print("analysis skipped because one of (rfm_segment, churn_risk, customer_id, total_revenue, customer_lifetime_value) is all NULL or zero.")
    else:
        print("missing agg_customers table; skipping rfm x churn analysis.")


    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_segment_label") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "preferred_referrer_source") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue"):
            
            analysis["seg_referrer_crosstab"] = (
                dataframes["agg_customers"]
                .groupBy("customer_segment_label", "preferred_referrer_source")
                .agg(
                    F.countDistinct("customer_id").alias("customer_count"),
                    F.sum("total_revenue").alias("segment_revenue"),
                    F.avg("total_revenue").alias("avg_revenue_per_customer")
                )
                .orderBy("customer_segment_label", F.col("segment_revenue").desc())
            )
        else:
            print("analysis skipped because one of the required columns in agg_customers is all NULL or zero.")
    else:
        print("missing agg_customers table; skipping segment x referrer analysis.")


    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_segment_label") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "preferred_device_type") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue"):
            
            analysis["seg_device_crosstab"] = (
                dataframes["agg_customers"]
                .groupBy("customer_segment_label", "preferred_device_type")
                .agg(
                    F.countDistinct("customer_id").alias("customer_count"),
                    F.sum("total_revenue").alias("segment_revenue"),
                    F.avg("total_revenue").alias("avg_revenue_per_customer")
                )
                .orderBy("customer_segment_label", F.col("segment_revenue").desc())
            )
        else:
            print("analysis skipped because one of the required columns in agg_customers is all NULL or zero.")
    else:
        print("missing agg_customers table; skipping segment x device analysis.")


    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "preferred_payment_method") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "churn_risk") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue"):
            
            analysis["payment_method_vs_clv_churn"] = (
                dataframes["agg_customers"]
                .groupBy("preferred_payment_method", "churn_risk")
                .agg(
                    F.countDistinct("customer_id").alias("customer_count"),
                    F.avg("customer_lifetime_value").alias("avg_clv"),
                    F.avg("total_revenue").alias("avg_revenue_per_customer"),
                    F.sum("total_revenue").alias("total_revenue")
                )
                .orderBy("preferred_payment_method", "churn_risk")
            )

            analysis["payment_method_summary"] = (
                dataframes["agg_customers"]
                .groupBy("preferred_payment_method")
                .agg(
                    F.countDistinct("customer_id").alias("customer_count"),
                    F.avg("customer_lifetime_value").alias("avg_clv"),
                    F.avg("total_revenue").alias("avg_revenue_per_customer"),
                    F.sum("total_revenue").alias("total_revenue")
                )
                .orderBy(F.col("total_revenue").desc())
            )
        else:
            print("analysis skipped because one of the required columns in agg_customers is all NULL or zero.")
    else:
        print("missing agg_customers table; skipping payment vs CLV/churn analysis.")


    if "agg_customers" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_customers"], "preferred_referrer_source") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "churn_risk") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "total_revenue") and \
        not is_column_all_null_or_zero(dataframes["agg_customers"], "total_discount_received"):
            
            referrer_df = (
                dataframes["agg_customers"]
                .withColumn(
                    "discount_share_of_revenue",
                    F.when(F.col("total_revenue") > 0,
                        F.col("total_discount_received") / F.col("total_revenue"))
                    .otherwise(F.lit(0.0))
                )
            )

            analysis["referrer_source_summary"] = (
                referrer_df
                .groupBy("preferred_referrer_source")
                .agg(
                    F.countDistinct("customer_id").alias("customer_count"),
                    F.avg("customer_lifetime_value").alias("avg_clv"),
                    F.sum("total_revenue").alias("total_revenue"),
                    F.avg("total_revenue").alias("avg_revenue_per_customer"),
                    F.avg("discount_share_of_revenue").alias("avg_discount_share")
                ).fillna({
                    "customer_count": 0,
                    "avg_clv": 0.0,
                    "total_revenue": 0.0,
                    "avg_revenue_per_customer": 0.0,
                    "avg_discount_share": 0.0
                })
                .orderBy(F.col("total_revenue").desc_nulls_last())
            )

            analysis["referrer_churn_summary"] = (
                referrer_df
                .groupBy("preferred_referrer_source", "churn_risk")
                .agg(
                    F.countDistinct("customer_id").alias("customer_count"),
                    F.avg("customer_lifetime_value").alias("avg_clv"),
                    F.avg("discount_share_of_revenue").alias("avg_discount_share")
                ).fillna({
                    "customer_count": 0,
                    "avg_clv": 0.0,
                    "avg_discount_share": 0.0
                })
                .orderBy("preferred_referrer_source", "churn_risk")
            )
        else:
            print("analysis skipped because one of the required columns in agg_customers is all NULL or zero.")
    else:
        print("missing agg_customers table; skipping referrer source analysis.")


    if "agg_orders" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_orders"], "customer_id") and \
        not is_column_all_null_or_zero(dataframes["agg_orders"], "order_profit") and \
        not is_column_all_null_or_zero(dataframes["agg_orders"], "net_profit") and \
        not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id"):
            
            customer_profit = (
                dataframes["agg_orders"]
                .groupBy("customer_id")
                .agg(
                    F.countDistinct("order_id").alias("orders_count_profit"),
                    F.sum("order_profit").alias("total_order_profit"),
                    F.avg("order_profit").alias("avg_profit_per_order"),
                    F.sum("net_profit").alias("total_net_profit"),
                    F.avg("net_profit").alias("avg_net_profit_per_order")
                )
            )

            if "agg_customers" in dataframes:
                customers_with_profit = (
                    dataframes["agg_customers"]
                    .join(customer_profit, on="customer_id", how="left")
                )

                if "customer_segment_label" in customers_with_profit.columns and \
                "total_revenue" in customers_with_profit.columns and \
                "customer_lifetime_value" in customers_with_profit.columns:
                    
                    analysis["customer_profit_per_segment"] = (
                        customers_with_profit
                        .groupBy("customer_segment_label")
                        .agg(
                            F.countDistinct("customer_id").alias("customer_count"),
                            F.sum("total_revenue").alias("total_revenue"),
                            F.sum("total_order_profit").alias("total_order_profit"),
                            F.sum("total_net_profit").alias("total_net_profit"),
                            F.avg("customer_lifetime_value").alias("avg_clv"),
                            F.avg("total_order_profit").alias("avg_profit_per_customer")
                        )
                        .orderBy(F.col("total_order_profit").desc_nulls_last())
                    )
            else:
                print("agg_customers not found; created customer_profit only.")
        else:
            print("analysis skipped because one of the required columns in agg_orders is all NULL or zero.")
    else:
        print("missing agg_orders table; skipping customer profit analysis.")


    if "agg_rfm_segmentation" in dataframes and "agg_orders" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_rfm_segmentation"], "customer_id") and \
        not is_column_all_null_or_zero(dataframes["agg_rfm_segmentation"], "rfm_segment") and \
        not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id"):
            
            rfm = dataframes["agg_rfm_segmentation"].select(
                "customer_id",
                "rfm_segment",
            )

            orders = dataframes["agg_orders"].select(
                "order_id",
                "customer_id",
                "total_amount"
            ).fillna({"total_amount": 0.0})

            seg_orders = (
                orders.alias("o")
                .join(rfm.alias("r"), on="customer_id", how="left")
            )

            analysis["segment_aov_by_rfm"] = (
                seg_orders.groupBy("rfm_segment")
                .agg(
                    F.countDistinct("order_id").alias("orders"),
                    F.sum("total_amount").alias("total_revenue"),
                    F.avg("total_amount").alias("avg_order_value_segment"),
                    F.countDistinct("customer_id").alias("unique_customers"),
                )
                .orderBy(F.col("avg_order_value_segment").desc_nulls_last())
            )
        else:
            print("analysis skipped because customer_id or order_id is all NULL or zero.")
    else:
        print("missing agg_rfm_segmentation or agg_orders table; skipping segment-specific AOV.")



    if "agg_products" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_products"], "product_id") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "category") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_orders") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue"):

            product_analysis["best_selling_products"] = (
                dataframes["agg_products"]
                .select(
                    "product_id",
                    "product_name",
                    "category",
                    "sub_category",
                    "brand",
                    "total_units_sold",
                    "total_orders",
                    "total_revenue",
                )
                .fillna(
                    {
                        "total_units_sold": 0,
                        "total_orders": 0,
                        "total_revenue": 0.0,
                    }
                )
                .orderBy(
                    F.col("total_units_sold").desc_nulls_last(),
                    F.col("total_revenue").desc_nulls_last(),
                )
            )
        else:
            print("analysis skipped because one of the required columns in agg_products is all NULL or zero.")
    else:
        print("missing agg_products table; skipping best selling products analysis.")



    if "agg_orders" in dataframes and "agg_order_items" in dataframes and "agg_products" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id") and \
        not is_column_all_null_or_zero(dataframes["agg_orders"], "order_placed_at") and \
        not is_column_all_null_or_zero(dataframes["agg_order_items"], "order_id") and \
        not is_column_all_null_or_zero(dataframes["agg_order_items"], "product_id") and \
        not is_column_all_null_or_zero(dataframes["agg_order_items"], "quantity") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "product_id") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "category"):

            # Base joined fact
            order_product = (
                dataframes["agg_order_items"]
                .join(
                    dataframes["agg_orders"]
                    .select("order_id", "order_placed_at"),
                    on="order_id",
                    how="inner",
                )
                .join(
                    dataframes["agg_products"]
                    .select("product_id", "product_name", "category", "sub_category", "brand"),
                    on="product_id",
                    how="left",
                )
                .withColumn("order_date", F.to_date("order_placed_at"))
            )

            # Monthly grain: year + month
            order_product_monthly = add_time_grain(
                order_product, date_col="order_date", grain="month"
            )

            # Category level trends
            product_analysis["category_monthly_trends"] = (
                order_product_monthly
                .groupBy("category", "grain_year", "grain_month")
                .agg(
                    F.sum("quantity").alias("units_sold"),
                    F.countDistinct("order_id").alias("orders_count"),
                )
                .fillna({"units_sold": 0, "orders_count": 0})
                .orderBy("category", "grain_year", "grain_month")
            )

            # Product level trends
            product_analysis["product_monthly_trends"] = (
                order_product_monthly
                .groupBy(
                    "product_id",
                    "product_name",
                    "category",
                    "sub_category",
                    "brand",
                    "grain_year",
                    "grain_month",
                )
                .agg(
                    F.sum("quantity").alias("units_sold"),
                    F.countDistinct("order_id").alias("orders_count"),
                )
                .fillna({"units_sold": 0, "orders_count": 0})
                .orderBy("product_id", "grain_year", "grain_month")
            )

            product_analysis["product_calendar_month_seasonality"] = (
                order_product
                .withColumn("calendar_month", F.month("order_date"))
                .groupBy(
                    "product_id",
                    "product_name",
                    "category",
                    "sub_category",
                    "brand",
                    "calendar_month",
                )
                .agg(
                    F.sum("quantity").alias("units_sold"),
                    F.countDistinct("order_id").alias("orders_count"),
                )
                .fillna(
                    {
                        "units_sold": 0,
                        "orders_count": 0,
                    }
                )
                .orderBy("product_id", "calendar_month")
            )

            product_analysis["category_calendar_month_seasonality"] = (
                order_product
                .withColumn("calendar_month", F.month("order_date"))
                .groupBy("category", "calendar_month")
                .agg(
                    F.sum("quantity").alias("units_sold"),
                    F.countDistinct("order_id").alias("orders_count"),
                )
                .fillna(
                    {
                        "units_sold": 0,
                        "orders_count": 0,
                    }
                )
                .orderBy("category", "calendar_month")
            )
            

        else:
            print("analysis skipped because a required column in orders, items, or products is all NULL or zero.")
    else:
        print("missing required tables; skipping product monthly seasonal trends.")



    if "agg_products" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_products"], "product_id") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "category") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "profit_margin") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold"):

            product_analysis["highest_margin_products"] = (
                dataframes["agg_products"]
                .select(
                    "product_id",
                    "product_name",
                    "category",
                    "sub_category",
                    "brand",
                    "profit_margin",
                    "total_revenue",
                    "total_units_sold",
                    "total_orders",
                )
                .fillna(
                    {
                        "profit_margin": 0.0,
                        "total_revenue": 0.0,
                        "total_units_sold": 0,
                        "total_orders": 0,
                    }
                )
                .orderBy(
                    F.col("profit_margin").desc_nulls_last(),
                    F.col("total_revenue").desc_nulls_last(),
                )
            )
        else:
            print("analysis skipped because a required column in agg_products is all NULL or zero.")
    else:
        print("missing agg_products table; skipping highest margin products analysis.")


    if "agg_products" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_products"], "product_id") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "category") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "profit_margin") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_orders") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "view_to_purchase_rate") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "revenue_per_view"):

            product_analysis["low_margin_high_traffic_products"] = (
                dataframes["agg_products"]
                .select(
                    "product_id",
                    "product_name",
                    "category",
                    "sub_category",
                    "brand",
                    "profit_margin",
                    "total_revenue",
                    "total_units_sold",
                    "total_orders",
                    "view_to_purchase_rate",
                    "revenue_per_view",
                    "total_wishlist_adds",
                    "total_cart_adds",
                )
                .fillna(
                    {
                        "profit_margin": 0.0,
                        "total_revenue": 0.0,
                        "total_units_sold": 0,
                        "total_orders": 0,
                        "view_to_purchase_rate": 0.0,
                        "revenue_per_view": 0.0,
                        "total_wishlist_adds": 0,
                        "total_cart_adds": 0,
                    }
                )
            )

            # Derive a simple "traffic_score" to help rank
            product_analysis["low_margin_high_traffic_products"] = (
                product_analysis["low_margin_high_traffic_products"]
                .withColumn(
                    "traffic_score",
                    (
                        F.col("total_units_sold")
                        + F.col("total_orders")
                        + F.col("total_wishlist_adds")
                        + F.col("total_cart_adds")
                    )
                )
                .orderBy(
                    F.col("profit_margin").asc_nulls_last(),
                    F.col("traffic_score").desc_nulls_last(),
                    F.col("total_revenue").desc_nulls_last(),
                )
            )
        else:
            print("analysis skipped because a required column in agg_products is all NULL or zero.")
    else:
        print("missing agg_products table; skipping low-margin high-traffic products analysis.")


    if "agg_products" in dataframes:
        # Checking for table presence and column validity
        if not is_column_all_null_or_zero(dataframes["agg_products"], "product_id") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "category") and \
        ("current_stock_level" in dataframes["agg_products"].columns or 
            "current_stock" in dataframes["agg_products"].columns):

            products_df = dataframes["agg_products"]

            # Prefer current_stock_level if present, else fall back to current_stock
            stock_col = "current_stock_level" if "current_stock_level" in products_df.columns else "current_stock"

            product_analysis["out_of_stock_products"] = (
                products_df
                .select(
                    "product_id",
                    "product_name",
                    "category",
                    "sub_category",
                    "brand",
                    F.col(stock_col).alias("current_stock_level"),
                    "total_units_sold",
                    "total_orders",
                    "total_revenue",
                )
                .fillna(
                    {
                        "current_stock_level": 0,
                        "total_units_sold": 0,
                        "total_orders": 0,
                        "total_revenue": 0.0,
                    }
                )
                .filter(F.col("current_stock_level") <= 0)
                .orderBy(
                    F.col("total_revenue").desc_nulls_last(),
                    F.col("total_units_sold").desc_nulls_last(),
                )
            )
        else:
            print("analysis skipped because one of the required columns in agg_products is all NULL or zero.")
    else:
        print("missing agg_products table; skipping out-of-stock analysis.")


    if "agg_products" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_products"], "product_id") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "category") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "view_to_purchase_rate") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "cart_to_purchase_rate") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_orders") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_wishlist_adds") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_cart_adds") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "wishlist_to_purchase_rate"):
            product_analysis["low_conversion_products"] = (
                dataframes["agg_products"]
                .select(
                    "product_id",
                    "product_name",
                    "category",
                    "sub_category",
                    "brand",
                    "view_to_purchase_rate",
                    "cart_to_purchase_rate",
                    "wishlist_to_purchase_rate",
                    "total_units_sold",
                    "total_orders",
                    "total_wishlist_adds",
                    "total_cart_adds",
                    "total_revenue",
                )
                .fillna(
                    {
                        "view_to_purchase_rate": 0.0,
                        "cart_to_purchase_rate": 0.0,
                        "wishlist_to_purchase_rate": 0.0,
                        "total_units_sold": 0,
                        "total_orders": 0,
                        "total_wishlist_adds": 0,
                        "total_cart_adds": 0,
                        "total_revenue": 0.0,
                    }
                )
                .orderBy(
                    F.col("view_to_purchase_rate").asc_nulls_last(),
                    F.col("cart_to_purchase_rate").asc_nulls_last(),
                    F.col("wishlist_to_purchase_rate").asc_nulls_last(),
                    F.col("total_revenue").desc_nulls_last(),
                )
            )
        else:
            print("analysis skipped because one of the required conversion columns in agg_products is all NULL or zero.")
    else:
        print("missing agg_products table; skipping low conversion rate analysis.")


    if "agg_reviews" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_reviews"], "product_id") and \
        not is_column_all_null_or_zero(dataframes["agg_reviews"], "rating"):
            
            product_analysis["product_rating_summary"] = (
                dataframes["agg_reviews"]
                .groupBy("product_id")
                .agg(
                    F.count("*").alias("total_reviews"),
                    F.avg("rating").alias("avg_rating")
                )
                .fillna({"total_reviews": 0, "avg_rating": 0.0})
                .orderBy(F.col("avg_rating").desc_nulls_last())
            )
        else:
            print("analysis skipped because product_id or rating is all NULL or zero in agg_reviews.")
    else:
        print("missing agg_reviews table; skipping product rating summary.")



    if "agg_products" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_products"], "category") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "product_id") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_orders") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "view_to_purchase_rate") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "revenue_per_view") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue"):
            
            product_analysis["category_view_patterns"] = (
                dataframes["agg_products"]
                .groupBy("category")
                .agg(
                    F.countDistinct("product_id").alias("products_in_category"),
                    F.sum("total_units_sold").alias("total_units_sold"),
                    F.sum("total_orders").alias("total_orders"),
                    F.avg("view_to_purchase_rate").alias("avg_view_to_purchase_rate"),
                    F.avg("revenue_per_view").alias("avg_revenue_per_view"),
                    F.sum("total_revenue").alias("total_revenue")
                ).fillna({
                    "products_in_category": 0,
                    "total_units_sold": 0,
                    "total_orders": 0,
                    "avg_view_to_purchase_rate": 0.0,
                    "avg_revenue_per_view": 0.0,
                    "total_revenue": 0.0
                })
                .orderBy(F.col("total_revenue").desc_nulls_last())
            )
        else:
            print("Analysis skipped because one of the required columns in agg_products is all NULL or zero.")
    else:
        print("Missing agg_products table; skipping category view patterns analysis.")


    if "agg_products" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_products"], "view_to_purchase_rate") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "revenue_per_view") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_orders"):
            
            product_analysis["top_view_to_purchase_products"] = (
                dataframes["agg_products"]
                .select(
                    "product_id",
                    "product_name",
                    "category",
                    "view_to_purchase_rate",
                    "revenue_per_view",
                    "total_units_sold",
                    "total_orders"
                ).fillna({
                    "view_to_purchase_rate": 0.0,
                    "revenue_per_view": 0.0,
                    "total_units_sold": 0,
                    "total_orders": 0
                })
                .orderBy(F.col("view_to_purchase_rate").desc_nulls_last())
            )
        else:
            print("Analysis skipped because one of the required columns in agg_products is all NULL or zero.")
    else:
        print("Missing agg_products table; skipping top view to purchase products analysis.")



    if (
        "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "category")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue")
    ):

        # Base product metrics
        products_base_perf = (
            dataframes["agg_products"]
            .select(
                "product_id",
                "product_name",
                "category",
                "sub_category",
                "brand",
                "total_units_sold",
                "total_orders",
                "total_revenue",
                "view_to_purchase_rate",
                "product_performance_score"  # keep existing if present
            )
            .fillna({
                "total_units_sold": 0,
                "total_orders": 0,
                "total_revenue": 0.0,
                "view_to_purchase_rate": 0.0,
                "product_performance_score": 0.0
            })
        )

        # Join ratings if we have them
        if product_analysis["product_rating_summary"] is not None:
            products_perf = (
                products_base_perf
                .join(
                    product_analysis["product_rating_summary"],
                    on="product_id",
                    how="left"
                )
                .fillna({
                    "total_reviews": 0,
                    "avg_rating": 0.0
                })
            )
        else:
            products_perf = (
                products_base_perf
                .withColumn("total_reviews", F.lit(0))
                .withColumn("avg_rating", F.lit(0.0))
            )

        # 2) Compute max values for normalization
        max_vals = products_perf.agg(
            F.max("total_revenue").alias("max_revenue"),
            F.max("total_units_sold").alias("max_units"),
            F.max("avg_rating").alias("max_rating"),
            F.max("view_to_purchase_rate").alias("max_view_to_purchase")
        ).collect()[0]

        max_revenue = max_vals["max_revenue"] or 0.0
        max_units = max_vals["max_units"] or 0
        max_rating = max_vals["max_rating"] or 0.0
        max_view_to_purchase = max_vals["max_view_to_purchase"] or 0.0

        # Avoid division by zero by using F.lit(...) and checks
        products_perf_scored = (
            products_perf
            .withColumn(
                "revenue_score",
                F.when(F.lit(max_revenue) > 0,
                    F.col("total_revenue") / F.lit(max_revenue))
                .otherwise(F.lit(0.0))
            )
            .withColumn(
                "volume_score",
                F.when(F.lit(max_units) > 0,
                    F.col("total_units_sold") / F.lit(max_units))
                .otherwise(F.lit(0.0))
            )
            .withColumn(
                "rating_score",
                F.when(F.lit(max_rating) > 0,
                    F.col("avg_rating") / F.lit(max_rating))
                .otherwise(F.lit(0.0))
            )
            .withColumn(
                "view_to_purchase_score",
                F.when(F.lit(max_view_to_purchase) > 0,
                    F.col("view_to_purchase_rate") / F.lit(max_view_to_purchase))
                .otherwise(F.lit(0.0))
            )
            # 3) Weighted composite score (tweak weights as desired)
            .withColumn(
                "product_performance_score_computed",
                0.4 * F.col("revenue_score")
                + 0.3 * F.col("volume_score")
                + 0.2 * F.col("rating_score")
                + 0.1 * F.col("view_to_purchase_score")
            )
        )

        product_analysis["product_performance_score"] = (
            products_perf_scored
            .select(
                "product_id",
                "product_name",
                "category",
                "sub_category",
                "brand",
                "total_units_sold",
                "total_orders",
                "total_revenue",
                "avg_rating",
                "view_to_purchase_rate",
                "revenue_score",
                "volume_score",
                "rating_score",
                "view_to_purchase_score",
                "product_performance_score_computed"
            )
            .fillna({
                "revenue_score": 0.0,
                "volume_score": 0.0,
                "rating_score": 0.0,
                "view_to_purchase_score": 0.0,
                "product_performance_score_computed": 0.0
            })
            .orderBy(
                F.col("product_performance_score_computed").desc_nulls_last()
            )
        )

    else:
        print(
            "One of the required columns in agg_products is all NULL or zero; "
            "skipping product performance score analysis."
        )



    if "agg_products" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_products"], "category") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue"):
            
            # Aggregate revenue per category
            category_rev_df = (
                dataframes["agg_products"]
                .groupBy("category")
                .agg(
                    F.sum("total_revenue").alias("category_revenue"),
                    F.countDistinct("product_id").alias("products_in_category"),
                    F.sum("total_units_sold").alias("total_units_sold")
                )
                .fillna({
                    "category_revenue": 0.0,
                    "products_in_category": 0,
                    "total_units_sold": 0
                })
            )

            # Compute total revenue across all categories
            total_rev_all = (
                category_rev_df
                .agg(F.sum("category_revenue").alias("total_revenue_all"))
                .first()[0] or 0.0
            )

            # Add revenue share and revenue per product
            product_analysis["category_revenue_share"] = (
                category_rev_df
                .withColumn(
                    "revenue_per_product",
                    F.when(F.col("products_in_category") > 0,
                        F.col("category_revenue") / F.col("products_in_category"))
                    .otherwise(F.lit(0.0))
                )
                .withColumn(
                    "revenue_share",
                    F.when(F.lit(total_rev_all) > 0,
                        F.col("category_revenue") / F.lit(total_rev_all))
                    .otherwise(F.lit(0.0))
                )
                .orderBy(F.col("category_revenue").desc_nulls_last())
            )
        else:
            print("analysis skipped because category or total_revenue in agg_products is all NULL or zero.")
    else:
        print("missing agg_products table; skipping category revenue share analysis.")



    if "agg_products" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_products"], "category") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "product_id") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_orders") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "view_to_purchase_rate") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "revenue_per_view"):
            
            category_perf = (
                dataframes["agg_products"]
                .groupBy("category")
                .agg(
                    F.countDistinct("product_id").alias("products_in_category"),
                    F.sum("total_units_sold").alias("total_units_sold"),
                    F.sum("total_orders").alias("total_orders"),
                    F.sum("total_revenue").alias("total_revenue"),
                    F.avg("view_to_purchase_rate").alias("avg_view_to_purchase_rate"),
                    F.avg("revenue_per_view").alias("avg_revenue_per_view"),
                    F.avg("profit_margin").alias("avg_profit_margin")
                )
                .fillna({
                    "products_in_category": 0,
                    "total_units_sold": 0,
                    "total_orders": 0,
                    "total_revenue": 0.0,
                    "avg_view_to_purchase_rate": 0.0,
                    "avg_revenue_per_view": 0.0,
                    "avg_profit_margin": 0.0
                })
            )

            # Compute overall totals to derive revenue share
            total_rev_all = (
                category_perf
                .agg(F.sum("total_revenue").alias("total_revenue_all"))
                .first()[0] or 0.0
            )

            category_perf_with_share = (
                category_perf
                .withColumn(
                    "revenue_share",
                    F.when(F.lit(total_rev_all) > 0,
                        F.col("total_revenue") / F.lit(total_rev_all))
                    .otherwise(F.lit(0.0))
                )
            )

            # "Low-performing" = low revenue & low efficiency metrics
            product_analysis["low_performing_categories"] = (
                category_perf_with_share
                .orderBy(
                    F.col("total_revenue").asc_nulls_last(),
                    F.col("avg_view_to_purchase_rate").asc_nulls_last(),
                    F.col("avg_revenue_per_view").asc_nulls_last(),
                    F.col("avg_profit_margin").asc_nulls_last()
                )
            )
        else:
            print("analysis skipped because required columns in agg_products are missing or null.")
    else:
        print("missing agg_products table; skipping low-performing categories analysis.")


    # Category popularity score

    if (
        "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_products"], "category")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_orders")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue")
    ):
        category_pop_df = (
            dataframes["agg_products"]
            .groupBy("category")
            .agg(
                F.countDistinct("product_id").alias("products_in_category"),
                F.sum("total_units_sold").alias("total_units_sold"),
                F.sum("total_orders").alias("total_orders"),
                F.sum("total_revenue").alias("total_revenue"),
                F.sum("total_wishlist_adds").alias("total_wishlist_adds"),
                F.sum("total_cart_adds").alias("total_cart_adds")
            )
            .fillna({
                "products_in_category": 0,
                "total_units_sold": 0,
                "total_orders": 0,
                "total_revenue": 0.0,
                "total_wishlist_adds": 0,
                "total_cart_adds": 0
            })
        )

        # Get max values for normalization
        max_vals_cat = category_pop_df.agg(
            F.max("total_units_sold").alias("max_units"),
            F.max("total_orders").alias("max_orders"),
            F.max("total_revenue").alias("max_revenue"),
            F.max("total_wishlist_adds").alias("max_wishlist"),
            F.max("total_cart_adds").alias("max_cart")
        ).collect()[0]

        max_units = max_vals_cat["max_units"] or 0
        max_orders = max_vals_cat["max_orders"] or 0
        max_revenue = max_vals_cat["max_revenue"] or 0.0
        max_wishlist = max_vals_cat["max_wishlist"] or 0
        max_cart = max_vals_cat["max_cart"] or 0

        category_pop_scored = (
            category_pop_df
            .withColumn(
                "units_score",
                F.when(F.lit(max_units) > 0,
                    F.col("total_units_sold") / F.lit(max_units))
                .otherwise(F.lit(0.0))
            )
            .withColumn(
                "orders_score",
                F.when(F.lit(max_orders) > 0,
                    F.col("total_orders") / F.lit(max_orders))
                .otherwise(F.lit(0.0))
            )
            .withColumn(
                "revenue_score",
                F.when(F.lit(max_revenue) > 0,
                    F.col("total_revenue") / F.lit(max_revenue))
                .otherwise(F.lit(0.0))
            )
            .withColumn(
                "wishlist_score",
                F.when(F.lit(max_wishlist) > 0,
                    F.col("total_wishlist_adds") / F.lit(max_wishlist))
                .otherwise(F.lit(0.0))
            )
            .withColumn(
                "cart_score",
                F.when(F.lit(max_cart) > 0,
                    F.col("total_cart_adds") / F.lit(max_cart))
                .otherwise(F.lit(0.0))
            )
            # Composite popularity score (tweak weights as needed)
            .withColumn(
                "category_popularity_score",
                0.3 * F.col("revenue_score")
                + 0.25 * F.col("units_score")
                + 0.2 * F.col("orders_score")
                + 0.15 * F.col("wishlist_score")
                + 0.1 * F.col("cart_score")
            )
        )

        product_analysis["category_popularity_score"] = (
            category_pop_scored
            .select(
                "category",
                "products_in_category",
                "total_units_sold",
                "total_orders",
                "total_revenue",
                "total_wishlist_adds",
                "total_cart_adds",
                "units_score",
                "orders_score",
                "revenue_score",
                "wishlist_score",
                "cart_score",
                "category_popularity_score"
            )
            .fillna({
                "units_score": 0.0,
                "orders_score": 0.0,
                "revenue_score": 0.0,
                "wishlist_score": 0.0,
                "cart_score": 0.0,
                "category_popularity_score": 0.0
            })
            .orderBy(F.col("category_popularity_score").desc_nulls_last())
        )
    else:
        print(
            "One of the required columns in agg_products is all NULL or zero; "
            "skipping category popularity score analysis."
        )



    # Category profitability

    if (
        "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_products"], "category")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_profit")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "profit_margin")
    ):
        category_profit_df = (
            dataframes["agg_products"]
            .groupBy("category")
            .agg(
                F.countDistinct("product_id").alias("products_in_category"),
                F.sum("total_revenue").alias("category_revenue"),
                F.sum("total_profit").alias("category_profit"),
                F.avg("profit_margin").alias("avg_profit_margin"),
                F.sum("total_units_sold").alias("total_units_sold"),
                F.sum("total_orders").alias("total_orders")
            )
            .fillna(
                {
                    "products_in_category": 0,
                    "category_revenue": 0.0,
                    "category_profit": 0.0,
                    "avg_profit_margin": 0.0,
                    "total_units_sold": 0,
                    "total_orders": 0,
                }
            )
        )

        # Total profit to compute profit share
        total_profit_all = (
            category_profit_df
            .agg(F.sum("category_profit").alias("total_profit_all"))
            .first()[0] or 0.0
        )

        product_analysis["category_profitability"] = (
            category_profit_df
            .withColumn(
                "profit_per_product",
                F.when(F.col("products_in_category") > 0,
                    F.col("category_profit") / F.col("products_in_category"))
                .otherwise(F.lit(0.0))
            )
            .withColumn(
                "revenue_per_product",
                F.when(F.col("products_in_category") > 0,
                    F.col("category_revenue") / F.col("products_in_category"))
                .otherwise(F.lit(0.0))
            )
            .withColumn(
                "profit_share",
                F.when(F.lit(total_profit_all) > 0,
                    F.col("category_profit") / F.lit(total_profit_all))
                .otherwise(F.lit(0.0))
            )
            .orderBy(
                F.col("category_profit").desc_nulls_last()
            )
        )
    else:
        print(
            "One of the required columns in agg_products is all NULL or zero; "
            "skipping category profitability analysis."
        )




    if (
        "agg_orders" in dataframes
        and "agg_order_items" in dataframes
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id")
        and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_placed_at")
        and not is_column_all_null_or_zero(dataframes["agg_order_items"], "order_id")
        and not is_column_all_null_or_zero(dataframes["agg_order_items"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_order_items"], "quantity")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "category")
    ):
        # 1) Base joined fact with order date
        category_season_fact = (
            dataframes["agg_order_items"]
            .join(
                dataframes["agg_orders"].select("order_id", "order_placed_at"),
                on="order_id",
                how="inner",
            )
            .join(
                dataframes["agg_products"].select(
                    "product_id",
                    "category",
                    "sub_category",
                    "brand",
                    "sell_price",
                ),
                on="product_id",
                how="left",
            )
            .withColumn("order_date", F.to_date("order_placed_at"))
        )

        category_season_fact = category_season_fact.withColumn(
            "calendar_month", F.month("order_date")
        )

        category_season_fact = (
            category_season_fact
            .withColumn(
                "line_revenue",
                F.when(
                    F.col("sell_price").isNotNull(),
                    F.col("quantity") * F.col("sell_price"),
                ).otherwise(F.lit(0.0)),
            )
            .fillna(
                {
                    "quantity": 0,
                    "line_revenue": 0.0,
                }
            )
        )

        category_monthly_season = (
            category_season_fact
            .groupBy("category", "calendar_month")
            .agg(
                F.sum("quantity").alias("units_sold"),
                F.countDistinct("order_id").alias("orders_count"),
                F.sum("line_revenue").alias("total_revenue_month"),
            )
            .fillna(
                {
                    "units_sold": 0,
                    "orders_count": 0,
                    "total_revenue_month": 0.0,
                }
            )
        )

        product_analysis["category_monthly_seasonality"] = (
            category_monthly_season.orderBy("category", "calendar_month")
        )


        category_max_revenue = (
            category_monthly_season
            .groupBy("category")
            .agg(
                F.max("total_revenue_month").alias("max_revenue_month")
            )
        )

        category_peak_season = (
            category_monthly_season.alias("m")
            .join(
                category_max_revenue.alias("mx"),
                on="category",
                how="inner",
            )
            .filter(F.col("m.total_revenue_month") == F.col("mx.max_revenue_month"))
            .select(
                F.col("m.category").alias("category"),
                F.col("m.calendar_month").alias("peak_season_month"),
                F.col("m.units_sold"),
                F.col("m.orders_count"),
                F.col("m.total_revenue_month").alias("peak_season_revenue"),
            )
            .orderBy("category", "peak_season_month")
        )

        product_analysis["category_peak_season"] = category_peak_season

    else:
        print(
            "One of the required columns in agg_orders, agg_order_items, or agg_products "
            "is all NULL or zero; skipping category peak season analysis."
        )


    # Product lifecycle segments
    if (
        "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "category")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "days_since_launch")
    ):
        lifecycle_df = (
            dataframes["agg_products"]
            .select(
                "product_id",
                "product_name",
                "category",
                "sub_category",
                "brand",
                "launch_date",
                "days_since_launch",
                "total_units_sold",
                "total_orders",
                "total_revenue",
                "view_to_purchase_rate",
                "avg_rating",
                "current_stock_level",
                "current_stock",
                "days_of_supply",
                "inventory_turnover_rate",
            )
            .fillna(
                {
                    "days_since_launch": 0,
                    "total_units_sold": 0,
                    "total_orders": 0,
                    "total_revenue": 0.0,
                    "view_to_purchase_rate": 0.0,
                    "avg_rating": 0.0,
                    "current_stock_level": 0,
                    "current_stock": 0,
                    "days_of_supply": 0.0,
                    "inventory_turnover_rate": 0.0,
                }
            )
            # Segment by lifecycle
            .withColumn(
                "lifecycle_stage",
                F.when(F.col("days_since_launch") < 90, F.lit("New (<90 days)"))
                .when(F.col("days_since_launch") < 365, F.lit("Growth (90-365 days)"))
                .otherwise(F.lit("Mature (>=365 days)"))
            )
        )

        product_analysis["product_lifecycle_segments"] = lifecycle_df

        # Aggregate summary per lifecycle stage
        product_analysis["product_lifecycle_summary"] = (
            lifecycle_df
            .groupBy("lifecycle_stage")
            .agg(
                F.countDistinct("product_id").alias("products_count"),
                F.avg("view_to_purchase_rate").alias("avg_view_to_purchase_rate"),
                F.avg("avg_rating").alias("avg_rating"),
                F.avg("total_units_sold").alias("avg_units_sold"),
                F.avg("total_revenue").alias("avg_revenue"),
                F.avg("inventory_turnover_rate").alias("avg_inventory_turnover_rate"),
            )
            .fillna(
                {
                    "products_count": 0,
                    "avg_view_to_purchase_rate": 0.0,
                    "avg_rating": 0.0,
                    "avg_units_sold": 0.0,
                    "avg_revenue": 0.0,
                    "avg_inventory_turnover_rate": 0.0,
                }
            )
            .orderBy("lifecycle_stage")
        )
    else:
        print(
            "One of the required columns in agg_products is all NULL or zero; "
            "skipping product lifecycle segments analysis."
        )


    if "agg_products" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_products"], "product_id") and \
        ("current_stock_level" in dataframes["agg_products"].columns or 
            "current_stock" in dataframes["agg_products"].columns) and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold") and \
        not is_column_all_null_or_zero(dataframes["agg_products"], "days_of_supply"):
            
            prod_df = dataframes["agg_products"]

            # Prefer current_stock_level if present
            stock_col = (
                "current_stock_level"
                if "current_stock_level" in prod_df.columns
                else "current_stock"
            )

            stockout_risk_df = (
                prod_df
                .select(
                    "product_id",
                    "product_name",
                    "category",
                    "sub_category",
                    "brand",
                    F.col(stock_col).alias("current_stock_level"),
                    "days_of_supply",
                    "total_units_sold",
                    "total_orders",
                    "total_revenue",
                )
                .fillna(
                    {
                        "current_stock_level": 0,
                        "days_of_supply": 0.0,
                        "total_units_sold": 0,
                        "total_orders": 0,
                        "total_revenue": 0.0,
                    }
                )
                .withColumn(
                    "is_high_seller",
                    F.col("total_units_sold") > 0,
                )
                .withColumn(
                    "is_low_stock",
                    (F.col("current_stock_level") <= 0)
                    | (F.col("days_of_supply") <= 3),  # threshold: <= 3 days of supply
                )
                .withColumn(
                    "stockout_risk_flag",
                    F.when(F.col("is_high_seller") & F.col("is_low_stock"), F.lit(1)).otherwise(F.lit(0)),
                )
            )

            product_analysis["product_stockout_risk"] = (
                stockout_risk_df
                .orderBy(
                    F.col("stockout_risk_flag").desc_nulls_last(),
                    F.col("total_units_sold").desc_nulls_last(),
                    F.col("days_of_supply").asc_nulls_last(),
                )
            )
        else:
            print("Analysis skipped because required stock or sales columns in agg_products are missing or null.")
    else:
        print("Missing agg_products table; skipping stockout risk analysis.")



    # Product stockout replenishment priority
    if (
        "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "stockout_occurrences")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "stockout_days")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_orders")
        
    ):
        product_analysis["product_stockout_replenishment"] = (
            dataframes["agg_products"]
            .select(
                "product_id",
                "product_name",
                "category",
                "sub_category",
                "brand",
                "stockout_occurrences",
                "stockout_days",
                "total_units_sold",
                "total_orders",
                "total_revenue",
            )
            .fillna(
                {
                    "stockout_occurrences": 0,
                    "stockout_days": 0,
                    "total_units_sold": 0,
                    "total_orders": 0,
                    "total_revenue": 0.0,
                }
            )
            .withColumn(
                "replenishment_priority_score",
                F.col("stockout_occurrences") * 1.0
                + F.col("stockout_days") * 0.1
                + (F.col("total_revenue") / F.lit(1000.0)),  # scale revenue
            )
            .orderBy(
                F.col("replenishment_priority_score").desc_nulls_last(),
                F.col("total_revenue").desc_nulls_last(),
            )
        )
    else:
        print(
            "One of the required columns in agg_products is all NULL or zero; "
            "skipping stockout replenishment analysis."
        )


    if "agg_products" in dataframes:
        if (
            ("current_stock_level" in dataframes["agg_products"].columns or 
            "current_stock" in dataframes["agg_products"].columns) and 
            not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold") and 
            not is_column_all_null_or_zero(dataframes["agg_products"], "inventory_turnover_rate")
        ):
            prod_df2 = dataframes["agg_products"]

            stock_col2 = (
                "current_stock_level"
                if "current_stock_level" in prod_df2.columns
                else "current_stock"
            )

            dead_stock_df = (
                prod_df2
                .select(
                    "product_id",
                    "product_name",
                    "category",
                    "sub_category",
                    "brand",
                    F.col(stock_col2).alias("current_stock_level"),
                    "total_units_sold",
                    "total_orders",
                    "total_revenue",
                    "inventory_turnover_rate",
                    "days_of_supply",
                )
                .fillna(
                    {
                        "current_stock_level": 0,
                        "total_units_sold": 0,
                        "total_orders": 0,
                        "total_revenue": 0.0,
                        "inventory_turnover_rate": 0.0,
                        "days_of_supply": 0.0,
                    }
                )
                # Heuristic "dead stock" score: high stock + low sales + low turnover
                .withColumn(
                    "dead_stock_score",
                    F.col("current_stock_level") * 1.0
                    - F.col("total_units_sold") * 0.5
                    - F.col("inventory_turnover_rate") * 10.0,
                )
            )

            product_analysis["product_dead_stock"] = (
                dead_stock_df
                .orderBy(
                    F.col("dead_stock_score").desc_nulls_last(),
                    F.col("current_stock_level").desc_nulls_last(),
                    F.col("inventory_turnover_rate").asc_nulls_last(),
                )
            )
        else:
            print("Analysis skipped because stock, units sold, or turnover columns in agg_products are missing or null.")
    else:
        print("Missing agg_products table; skipping dead stock analysis.")


    # 1) Product inventory health (joined with revenue from agg_products)

    if (
        "agg_product_inventory_health" in dataframes
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "current_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "available_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "days_of_supply")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "inventory_turnover_ratio")
    ):
        inv_health = (
            dataframes["agg_product_inventory_health"]
            .select(
                "product_id",
                "current_stock",
                "available_stock",
                "reorder_point_breach_count",
                "stockout_frequency",
                "avg_stock_quantity",
                "days_of_supply",
                "inventory_turnover_ratio",
                "reorder_urgency",
                "stock_health_score",
            )
            .fillna(
                {
                    "current_stock": 0,
                    "available_stock": 0,
                    "reorder_point_breach_count": 0,
                    "stockout_frequency": 0.0,
                    "avg_stock_quantity": 0.0,
                    "days_of_supply": 0.0,
                    "inventory_turnover_ratio": 0.0,
                    "reorder_urgency": 0.0,
                    "stock_health_score": 0.0,
                }
            )
        )

        if (
            "agg_products" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
            and not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold")
            and not is_column_all_null_or_zero(dataframes["agg_products"], "total_orders")
            and not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue")
        ):
            prod_basic = (
                dataframes["agg_products"]
                .select(
                    "product_id",
                    "product_name",
                    "category",
                    "sub_category",
                    "brand",
                    "total_units_sold",
                    "total_orders",
                    "total_revenue",
                )
                .fillna(
                    {
                        "product_name": "Unknown",
                        "category": "Unknown",
                        "sub_category": "Unknown",
                        "brand": "Unknown",
                        "total_units_sold": 0,
                        "total_orders": 0,
                        "total_revenue": 0.0,
                    }
                )
            )

            product_inventory_health_df = (
                inv_health.join(prod_basic, on="product_id", how="left")
            )
        else:
            product_inventory_health_df = inv_health

        product_analysis["product_inventory_health"] = product_inventory_health_df

        critical_products = (
            product_inventory_health_df
            .withColumn(
                "is_low_days_of_supply",
                F.col("days_of_supply") <= 3,
            )
            .withColumn(
                "criticality_score",
                F.when(F.col("is_low_days_of_supply"), 1.0).otherwise(0.0)
                + (F.col("total_revenue") / F.lit(1000.0))
                + (F.col("reorder_urgency") * 0.5)
            )
            .orderBy(
                F.col("criticality_score").desc_nulls_last(),
                F.col("total_revenue").desc_nulls_last(),
                F.col("days_of_supply").asc_nulls_last(),
            )
        )

        product_analysis["product_inventory_critical"] = critical_products
        

    else:
        print(
            "One of the required columns in agg_product_inventory_health is all NULL or zero; "
            "skipping base product inventory health view."
        )



    # 1) Supplier × Product performance

    if (
        "agg_products" in dataframes
        and "agg_suppliers" in dataframes
        and "agg_supplier_inventory_health" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "supplier_id")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "profit_margin")
    ):

        # --- Base product + supplier_id info from products (p_ prefix) ---
        products_with_supplier = (
            dataframes["agg_products"]
            .select(
                F.col("product_id").alias("p_product_id"),
                F.col("product_name").alias("p_product_name"),
                F.col("category").alias("p_category"),
                F.col("sub_category").alias("p_sub_category"),
                F.col("brand").alias("p_brand"),
                "supplier_id",                               # join key, keep as-is
                F.col("total_units_sold").alias("p_total_units_sold"),
                F.col("total_orders").alias("p_total_orders"),
                F.col("total_revenue").alias("p_total_revenue"),
                F.col("total_profit").alias("p_total_profit"),
                F.col("profit_margin").alias("p_profit_margin"),
            )
            .fillna(
                {
                    "p_total_units_sold": 0,
                    "p_total_orders": 0,
                    "p_total_revenue": 0.0,
                    "p_total_profit": 0.0,
                    "p_profit_margin": 0.0,
                }
            )
        )

        # --- Supplier-level metrics (s_ prefix) ---
        suppliers_basic = (
            dataframes["agg_suppliers"]
            .select(
                "supplier_id",
                F.col("supplier_status").alias("s_supplier_status"),
                F.col("is_preferred").alias("s_is_preferred"),
                F.col("is_verified").alias("s_is_verified"),
                F.col("total_products_supplied").alias("s_total_products_supplied"),
                F.col("total_units_sold").alias("s_total_units_sold"),
                F.col("total_orders_fulfilled").alias("s_total_orders_fulfilled"),
                F.col("total_reviews").alias("s_total_reviews"),
                F.col("total_stockouts").alias("s_total_stockouts"),
                F.col("total_revenue_generated").alias("s_total_revenue_generated"),
                F.col("avg_profit_margin").alias("s_avg_profit_margin"),
                F.col("avg_product_rating").alias("s_avg_product_rating"),
                F.col("supplier_performance_score").alias("s_supplier_performance_score"),
                F.col("supplier_reliability_score").alias("s_supplier_reliability_score"),
                F.col("stock_efficiency_ratio").alias("s_stock_efficiency_ratio"),
                F.col("stockout_rate").alias("s_stockout_rate"),
                F.col("supplier_inventory_health_score").alias("s_inventory_health_score"),
            )
            .fillna(
                {
                    "s_supplier_status": "Unknown",
                    "s_is_preferred": False,
                    "s_is_verified": False,
                    "s_total_products_supplied": 0,
                    "s_total_units_sold": 0,
                    "s_total_orders_fulfilled": 0,
                    "s_total_reviews": 0,
                    "s_total_stockouts": 0,
                    "s_total_revenue_generated": 0.0,
                    "s_avg_profit_margin": 0.0,
                    "s_avg_product_rating": 0.0,
                    "s_supplier_performance_score": 0.0,
                    "s_supplier_reliability_score": 0.0,
                    "s_stock_efficiency_ratio": 0.0,
                    "s_stockout_rate": 0.0,
                    "s_inventory_health_score": 0.0,
                }
            )
        )

        # --- Supplier inventory health (si_ prefix) ---
        supplier_inv_health = (
            dataframes["agg_supplier_inventory_health"]
            .select(
                "supplier_id",
                F.col("total_products").alias("si_total_products"),
                F.col("total_current_stock").alias("si_total_current_stock"),
                F.col("total_available_stock").alias("si_total_available_stock"),
                F.col("total_reorder_breaches").alias("si_total_reorder_breaches"),
                F.col("total_stockouts").alias("si_total_stockouts"),
                F.col("total_storage_cost").alias("si_total_storage_cost"),
                F.col("avg_stock_per_product").alias("si_avg_stock_per_product"),
                F.col("stockout_rate").alias("si_stockout_rate"),
                F.col("supplier_inventory_health_score").alias("si_inventory_health_score"),
            )
            .fillna(
                {
                    "si_total_products": 0,
                    "si_total_current_stock": 0,
                    "si_total_available_stock": 0,
                    "si_total_reorder_breaches": 0,
                    "si_total_stockouts": 0,
                    "si_total_storage_cost": 0.0,
                    "si_avg_stock_per_product": 0.0,
                    "si_stockout_rate": 0.0,
                    "si_inventory_health_score": 0.0,
                }
            )
        )

        # --- Join all three on supplier_id ---
        joined = (
            products_with_supplier
            .join(suppliers_basic, on="supplier_id", how="left")
            .join(supplier_inv_health, on="supplier_id", how="left")
        )

        # --- Final, clean schema (no duplicate names) ---
        supplier_product_performance = joined.select(
            # product-level fields
            F.col("p_product_id").alias("product_id"),
            F.col("p_product_name").alias("product_name"),
            F.col("p_category").alias("category"),
            F.col("p_sub_category").alias("sub_category"),
            F.col("p_brand").alias("brand"),
            "supplier_id",
            F.col("p_total_units_sold").alias("total_units_sold"),
            F.col("p_total_orders").alias("total_orders"),
            F.col("p_total_revenue").alias("total_revenue"),
            F.col("p_total_profit").alias("total_profit"),
            F.col("p_profit_margin").alias("profit_margin"),

            # supplier-level fields
            F.col("s_supplier_status").alias("supplier_status"),
            F.col("s_is_preferred").alias("is_preferred"),
            F.col("s_is_verified").alias("is_verified"),
            F.col("s_total_products_supplied").alias("sup_total_products_supplied"),
            F.col("s_total_units_sold").alias("sup_total_units_sold"),
            F.col("s_total_orders_fulfilled").alias("sup_total_orders_fulfilled"),
            F.col("s_total_reviews").alias("sup_total_reviews"),
            F.col("s_total_stockouts").alias("sup_total_stockouts"),
            F.col("s_total_revenue_generated").alias("sup_total_revenue_generated"),
            F.col("s_avg_profit_margin").alias("sup_avg_profit_margin"),
            F.col("s_avg_product_rating").alias("sup_avg_product_rating"),
            F.col("s_supplier_performance_score").alias("supplier_performance_score"),
            F.col("s_supplier_reliability_score").alias("supplier_reliability_score"),
            F.col("s_stock_efficiency_ratio").alias("stock_efficiency_ratio"),
            F.col("s_stockout_rate").alias("sup_stockout_rate"),
            F.col("s_inventory_health_score").alias("sup_inventory_health_score"),

            # supplier inventory health aggregate
            F.col("si_total_products").alias("inv_total_products"),
            F.col("si_total_current_stock").alias("inv_total_current_stock"),
            F.col("si_total_available_stock").alias("inv_total_available_stock"),
            F.col("si_total_reorder_breaches").alias("inv_total_reorder_breaches"),
            F.col("si_total_stockouts").alias("inv_total_stockouts"),
            F.col("si_total_storage_cost").alias("inv_total_storage_cost"),
            F.col("si_avg_stock_per_product").alias("inv_avg_stock_per_product"),
            F.col("si_stockout_rate").alias("inv_stockout_rate"),
            F.col("si_inventory_health_score").alias("inv_inventory_health_score"),
        )

        product_analysis["supplier_product_performance"] = supplier_product_performance

    else:
        print(
            "One of the required columns in agg_products/agg_suppliers/agg_supplier_inventory_health "
            "is all NULL or zero; skipping supplier x product performance view."
        )



    if (
        "agg_product_inventory_health" in dataframes
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "stockout_frequency")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "reorder_point_breach_count")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "current_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "available_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "days_of_supply")
    ):
        # Base inventory health
        prod_inv = dataframes["agg_product_inventory_health"].fillna(
            {
                "stockout_frequency": 0,
                "reorder_point_breach_count": 0,
                "current_stock": 0,
                "available_stock": 0,
            }
        )

        # Optional enrich with product name & category from agg_products
        if (
            "agg_products" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        ):
            prod_meta = dataframes["agg_products"].select(
                "product_id",
                "product_name",
                "category"
                # NOTE: DO NOT bring agg_products.stock_status to avoid ambiguity
            )

            prod_inv = (
                prod_inv.alias("i")
                .join(prod_meta.alias("p"), on="product_id", how="left")
            )

        # Use stock_status from agg_product_inventory_health ONLY
        product_analysis["stockout_rate_by_product"] = (
            prod_inv.select(
                "product_id",
                "supplier_id",
                "stockout_frequency",
                "reorder_point_breach_count",
                "current_stock",
                "available_stock",
                "days_of_supply",
                "stock_status",      # this is i.stock_status
                "product_name",
                "category",
            )
            .orderBy(
                F.col("stockout_frequency").desc_nulls_last(),
                F.col("reorder_point_breach_count").desc_nulls_last(),
            )
        )

    # -------------------------------------------------------
    # Supplier-level stockout rate
    # -------------------------------------------------------
    has_suppliers = (
        "agg_suppliers" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_suppliers"], "supplier_id")
    )
    has_sup_inv = (
        "agg_supplier_inventory_health" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_supplier_inventory_health"], "supplier_id")
    )

    if has_suppliers or has_sup_inv:
        # Supplier base (from agg_suppliers if available)
        if has_suppliers:
            s = dataframes["agg_suppliers"].select(
                "supplier_id",
                "supplier_status",
                "total_products_supplied",
                "total_units_sold",
                "total_orders_fulfilled",
                "total_stockouts",
                "stockout_rate",                 # supplier-level stockout rate
                "supplier_reliability_score",
                "supplier_inventory_health_score",
            )
        else:
            # Fallback: only supplier_id from inventory health
            s = dataframes["agg_supplier_inventory_health"].select("supplier_id").distinct()

        # Inventory health aggregates (renamed to avoid clashes)
        if has_sup_inv:
            i = dataframes["agg_supplier_inventory_health"].select(
                "supplier_id",
                F.col("total_stockouts").alias("inv_total_stockouts"),
                F.col("stockout_rate").alias("inv_stockout_rate"),
                F.col("total_products").alias("inv_total_products"),
                F.col("total_current_stock").alias("inv_total_current_stock"),
                F.col("total_available_stock").alias("inv_total_available_stock"),
            )
            joined = s.alias("s").join(i.alias("i"), on="supplier_id", how="left")
        else:
            joined = s

        supplier_analysis["stockout_rate_by_supplier"] = (
            joined.select(
                "supplier_id",
                F.coalesce(F.col("supplier_status"), F.lit("unknown")).alias("supplier_status"),
                F.coalesce(F.col("total_stockouts"), F.lit(0)).alias("total_stockouts"),
                F.coalesce(F.col("stockout_rate"), F.lit(0.0)).alias("supplier_stockout_rate"),
                F.coalesce(F.col("inv_total_stockouts"), F.lit(0)).alias("inv_total_stockouts"),
                F.coalesce(F.col("inv_stockout_rate"), F.lit(0.0)).alias("inv_stockout_rate"),
                F.coalesce(F.col("inv_total_products"), F.lit(0)).alias("inv_total_products"),
                F.coalesce(F.col("inv_total_current_stock"), F.lit(0)).alias("inv_total_current_stock"),
                F.coalesce(F.col("inv_total_available_stock"), F.lit(0)).alias("inv_total_available_stock"),
                F.coalesce(F.col("supplier_reliability_score"), F.lit(0.0)).alias("supplier_reliability_score"),
                F.coalesce(F.col("supplier_inventory_health_score"), F.lit(0.0)).alias("supplier_inventory_health_score"),
            )
            .orderBy(
                F.col("supplier_stockout_rate").desc_nulls_last(),
                F.col("total_stockouts").desc_nulls_last(),
            )
        )
    else:
        print(
            "Neither agg_suppliers nor agg_supplier_inventory_health usable; "
            "skipping stockout rate by supplier."
        )


    # Supplier ranking: total_revenue_generated, avg_profit_margin, stockout_rate

    if (
        "agg_suppliers" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_suppliers"], "supplier_id")
        and not is_column_all_null_or_zero(dataframes["agg_suppliers"], "total_revenue_generated")
        and not is_column_all_null_or_zero(dataframes["agg_suppliers"], "avg_profit_margin")
        and not is_column_all_null_or_zero(dataframes["agg_suppliers"], "stockout_rate")
    ):
        product_analysis["supplier_ranking_core"] = (
            dataframes["agg_suppliers"]
            .select(
                "supplier_id",
                "supplier_status",
                "is_preferred",
                "is_verified",
                "total_products_supplied",
                "total_units_sold",
                "total_orders_fulfilled",
                "total_stockouts",
                "total_revenue_generated",
                "avg_profit_margin",
                "stockout_rate",
                "supplier_inventory_health_score",
                "supplier_performance_score",
                "supplier_reliability_score",
                "stock_efficiency_ratio",
            )
            .fillna(
                {
                    "total_products_supplied": 0,
                    "total_units_sold": 0,
                    "total_orders_fulfilled": 0,
                    "total_stockouts": 0,
                    "total_revenue_generated": 0.0,
                    "avg_profit_margin": 0.0,
                    "stockout_rate": 0.0,
                    "supplier_inventory_health_score": 0.0,
                    "supplier_performance_score": 0.0,
                    "supplier_reliability_score": 0.0,
                    "stock_efficiency_ratio": 0.0,
                }
            )
            .orderBy(
                F.col("total_revenue_generated").desc_nulls_last(),  # high revenue first
                F.col("avg_profit_margin").desc_nulls_last(),        # then high margin
                F.col("stockout_rate").asc_nulls_last(),             # then low stockout_rate
            )
        )
    else:
        print(
            "One of the required columns in agg_suppliers is all NULL or zero; "
            "skipping core supplier ranking analysis."
        )


    # 3) Supplier stockout impact: focus on top-revenue products

    if "supplier_product_performance" in product_analysis:
        spp2 = (
            product_analysis["supplier_product_performance"]
            .fillna(
                {
                    "total_revenue": 0.0,
                    "total_units_sold": 0,
                    "total_orders": 0,
                    "sup_total_stockouts": 0,
                    "sup_stockout_rate": 0.0,
                    "sup_inventory_health_score": 0.0,
                }
            )
        )

        TOP_REVENUE_THRESHOLD = dataframes["agg_suppliers"].agg(
            F.expr("percentile_approx(total_revenue_generated, 0.9)")
        ).collect()[0][0]

        supplier_stockout_impact = (
            spp2
            .withColumn(
                "is_top_revenue_product",
                F.col("total_revenue") >= F.lit(TOP_REVENUE_THRESHOLD),
            )
            .filter(F.col("is_top_revenue_product"))
            .select(
                "product_id",
                "product_name",
                "category",
                "sub_category",
                "brand",
                "supplier_id",
                "supplier_status",
                "is_preferred",
                "is_verified",
                "total_revenue",
                "total_units_sold",
                "total_orders",
                "profit_margin",
                # use disambiguated supplier-level columns
                "sup_total_stockouts",
                "sup_stockout_rate",
                "sup_inventory_health_score",
                "supplier_performance_score",
                "supplier_reliability_score",
            )
            .orderBy(
                F.col("sup_total_stockouts").desc_nulls_last(),
                F.col("sup_stockout_rate").desc_nulls_last(),
                F.col("total_revenue").desc_nulls_last(),
            )
        )

        product_analysis["supplier_stockout_impact_on_products"] = supplier_stockout_impact


    else:
        print(
            "supplier_product_performance not available; "
            "skipping supplier stockout impact on products analysis."
        )



    # Category-to-category affinity analysis

    if (
        "agg_category_affinity" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_category_affinity"], "product_a_category")
        and not is_column_all_null_or_zero(dataframes["agg_category_affinity"], "product_b_category")
        and not is_column_all_null_or_zero(dataframes["agg_category_affinity"], "pair_count")
        and not is_column_all_null_or_zero(dataframes["agg_category_affinity"], "total_co_occurrences")
        and not is_column_all_null_or_zero(dataframes["agg_category_affinity"], "avg_lift_between_categories")
        and not is_column_all_null_or_zero(dataframes["agg_category_affinity"], "avg_support")
    ):
        category_affinity_base = (
            dataframes["agg_category_affinity"]
            .select(
                "product_a_category",
                "product_b_category",
                "pair_count",
                "total_co_occurrences",
                "avg_lift_between_categories",
                "avg_support",
            )
            .fillna(
                {
                    "pair_count": 0,
                    "total_co_occurrences": 0,
                    "avg_lift_between_categories": 0.0,
                    "avg_support": 0.0,
                }
            )
            # You may want to drop self‑pairs if they exist
            .filter(F.col("product_a_category") != F.col("product_b_category"))
        )

        # Store full pair table ordered by lift
        product_analysis["category_affinity_pairs"] = (
            category_affinity_base
            .orderBy(
                F.col("avg_lift_between_categories").desc_nulls_last(),
                F.col("avg_support").desc_nulls_last(),
                F.col("total_co_occurrences").desc_nulls_last(),
            )
        )

        max_lift_per_a = (
            category_affinity_base.groupBy("product_a_category")
            .agg(
                F.max("avg_lift_between_categories").alias("max_lift_for_a")
            )
        )

        category_affinity_top_per_category = (
            category_affinity_base.alias("b")
            .join(
                max_lift_per_a.alias("m"),
                on="product_a_category",
                how="inner",
            )
            .filter(F.col("b.avg_lift_between_categories") == F.col("m.max_lift_for_a"))
            .select(
                F.col("b.product_a_category").alias("base_category"),
                F.col("b.product_b_category").alias("affinity_category"),
                F.col("b.pair_count"),
                F.col("b.total_co_occurrences"),
                F.col("b.avg_lift_between_categories").alias("avg_lift"),
                F.col("b.avg_support").alias("avg_support"),
            )
            .orderBy(
                F.col("avg_lift").desc_nulls_last(),
                F.col("avg_support").desc_nulls_last(),
            )
        )

        product_analysis["category_affinity_top_per_category"] = category_affinity_top_per_category

    else:
        print(
            "One of the required columns in agg_category_affinity is all NULL or zero; "
            "skipping category-to-category affinity analysis."
        )


    # 1) Product-to-product affinity: full pairs

    if (
        "agg_product_affinity" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_affinity"], "product_a_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_affinity"], "product_b_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_affinity"], "co_occurrence_count")
        and not is_column_all_null_or_zero(dataframes["agg_product_affinity"], "support")
        and not is_column_all_null_or_zero(dataframes["agg_product_affinity"], "avg_lift")
        and not is_column_all_null_or_zero(dataframes["agg_product_affinity"], "affinity_score")
    ):
        product_affinity_base = (
            dataframes["agg_product_affinity"]
            .select(
                "product_a_id",
                "product_a_name",
                "product_a_category",
                "product_b_id",
                "product_b_name",
                "product_b_category",
                "co_occurrence_count",
                "support",
                "confidence_a_to_b",
                "confidence_b_to_a",
                "lift_a_to_b",
                "lift_b_to_a",
                "avg_lift",
                "affinity_strength",
                "affinity_score",
            )
            .fillna(
                {
                    "co_occurrence_count": 0,
                    "support": 0.0,
                    "confidence_a_to_b": 0.0,
                    "confidence_b_to_a": 0.0,
                    "lift_a_to_b": 0.0,
                    "lift_b_to_a": 0.0,
                    "avg_lift": 0.0,
                    "affinity_strength": 0.0,
                    "affinity_score": 0.0,
                }
            )
            # Optional: exclude self‑pairs if present
            .filter(F.col("product_a_id") != F.col("product_b_id"))
        )

        product_analysis["product_affinity_pairs"] = (
            product_affinity_base
            .orderBy(
                F.col("affinity_score").desc_nulls_last(),
                F.col("avg_lift").desc_nulls_last(),
                F.col("co_occurrence_count").desc_nulls_last(),
            )
        )

        max_affinity_per_a = (
            product_affinity_base.groupBy("product_a_id")
            .agg(
                F.max("affinity_score").alias("max_affinity_for_a")
            )
        )

        product_affinity_top_per_product = (
            product_affinity_base.alias("p")
            .join(
                max_affinity_per_a.alias("m"),
                on="product_a_id",
                how="inner",
            )
            .filter(F.col("p.affinity_score") == F.col("m.max_affinity_for_a"))
            .select(
                F.col("p.product_a_id"),
                F.col("p.product_a_name"),
                F.col("p.product_a_category"),
                F.col("p.product_b_id").alias("recommended_product_id"),
                F.col("p.product_b_name").alias("recommended_product_name"),
                F.col("p.product_b_category").alias("recommended_product_category"),
                F.col("p.co_occurrence_count"),
                F.col("p.support"),
                F.col("p.confidence_a_to_b"),
                F.col("p.lift_a_to_b"),
                F.col("p.avg_lift"),
                F.col("p.affinity_strength"),
                F.col("p.affinity_score"),
            )
            .orderBy(
                F.col("affinity_score").desc_nulls_last(),
                F.col("avg_lift").desc_nulls_last(),
            )
        )

        product_analysis["product_affinity_top_per_product"] = product_affinity_top_per_product



    else:
        print(
            "One of the required columns in agg_product_affinity is all NULL or zero; "
            "skipping product-to-product affinity base analysis."
        )



    if (
        "agg_product_recommendations" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_recommendations"], "product_a_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_recommendations"], "recommended_products")
        and not is_column_all_null_or_zero(dataframes["agg_product_recommendations"], "avg_affinity_score")
    ):
        
        product_analysis["precomputed_product_recommendations"] = (
            dataframes["agg_product_recommendations"]
            .select(
                "product_a_id",
                "recommended_products",   
                "avg_affinity_score",
            )
            .fillna(
                {
                    "recommended_products": "[]",   
                    "avg_affinity_score": 0.0,
                }
            )
        )

        
        reco_df = product_analysis["precomputed_product_recommendations"]

        reco_with_flag = (
            reco_df
            .withColumn(
                "has_recommendations",
                F.when(
                    (F.col("recommended_products").isNotNull())
                    & (F.length(F.trim(F.col("recommended_products"))) > 2),  # more than "[]"
                    F.lit(1)
                ).otherwise(F.lit(0))
            )
        )

        product_analysis["precomputed_product_recommendations"] = reco_with_flag

        product_analysis["precomputed_reco_coverage"] = (
            reco_with_flag
            .agg(
                F.countDistinct("product_a_id").alias("total_products"),
                F.sum("has_recommendations").alias("products_with_recommendations"),
            )
            .withColumn(
                "coverage_rate",
                F.when(
                    F.col("total_products") > 0,
                    F.col("products_with_recommendations") / F.col("total_products"),
                ).otherwise(F.lit(0.0)),
            )
        )

    else:
        print(
            "One of the required columns in agg_product_recommendations is all NULL or zero; "
            "skipping precomputed product recommendations analysis."
        )




    # 1) Campaign Performance Summary
    if (
        "agg_marketing_campaigns" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "campaign_id")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "revenue_generated")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "clicks")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "spent_amount")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "impressions")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "budget")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "orders_from_campaign")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "campaign_efficiency_score")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "avg_order_value")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "conversion_rate")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "roas")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "roi")
    ):
        analysis["campaign_performance_summary"] = (
            dataframes["agg_marketing_campaigns"]
            .select(
                "campaign_id", "campaign_name", "campaign_type", "start_date", "end_date", "campaign_status",
                "impressions", "clicks", "orders_from_campaign", "revenue_generated", "avg_order_value",
                "conversion_rate", "roas", "roi", "campaign_efficiency_score", "spent_amount", "budget"
            )
            .fillna({
                "impressions": 0, "clicks": 0, "orders_from_campaign": 0, "revenue_generated": 0.0,
                "avg_order_value": 0.0, "conversion_rate": 0.0, "roas": 0.0, "roi": 0.0,
                "campaign_efficiency_score": 0.0, "spent_amount": 0.0, "budget": 0.0
            })
            .withColumn("revenue_per_click", F.when(F.col("clicks") > 0, F.col("revenue_generated") / F.col("clicks")).otherwise(0.0))
            .withColumn("roi_per_spend", F.when(F.col("spent_amount") > 0, F.col("revenue_generated") / F.col("spent_amount")).otherwise(None))
            .orderBy(F.col("revenue_generated").desc_nulls_last())
        )

    # 2) Find Campaign Attribution Column
    campaign_order_col = None
    if "agg_orders" in dataframes: 
        for c in ["campaign_id", "marketing_campaign_id", "attributed_campaign_id", "utm_campaign", "campaign"]:
            if c in dataframes["agg_orders"].columns and not is_column_all_null_or_zero(dataframes["agg_orders"], c):
                campaign_order_col = c
                break

    # Calculate Line Revenue
    line_revenue_col = None
    if "agg_order_items" in dataframes:
        for c in ["line_total", "line_revenue", "line_price", "extended_price"]:
            if c in dataframes["agg_order_items"]. columns and not is_column_all_null_or_zero(dataframes["agg_order_items"], c):
                line_revenue_col = c
                break
        
        if line_revenue_col is None and "product_cost" in dataframes["agg_order_items"].columns:
            if not is_column_all_null_or_zero(dataframes["agg_order_items"], "product_cost"):
                dataframes["agg_order_items"] = dataframes["agg_order_items"].withColumn(
                    "calculated_line_revenue", F.col("quantity") * F.col("product_cost")
                )
                line_revenue_col = "calculated_line_revenue"

    # 3) Campaign-Product Contribution
    if (
        campaign_order_col is not None
        and "agg_orders" in dataframes
        and "agg_order_items" in dataframes
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "campaign_id")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "clicks")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "spent_amount")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "impressions")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "budget")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "orders_from_campaign")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "campaign_efficiency_score")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "avg_order_value")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "conversion_rate")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "roas")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "roi")
    ):
        orders_with_campaign = (
            dataframes["agg_orders"]
            .select("order_id", "customer_id", campaign_order_col, "total_amount")
            .withColumnRenamed(campaign_order_col, "campaign_id_attributed")
            .filter(F.col("campaign_id_attributed").isNotNull())
        )
        
        order_items_cols = ["order_id", "product_id", "quantity"]
        if line_revenue_col: 
            order_items_cols.append(line_revenue_col)
        
        order_product_campaign = (
            dataframes["agg_order_items"]. select(*order_items_cols)
            .join(orders_with_campaign, on="order_id", how="inner")
            .join(
                dataframes["agg_products"]. select("product_id", "product_name", "category", "sub_category", "brand", "profit_margin"),
                on="product_id", how="left"
            )
            .withColumn("campaign_id", F.col("campaign_id_attributed"))
        )
        
        if line_revenue_col:
            campaign_product_agg = (
                order_product_campaign
                .groupBy("campaign_id", "product_id", "product_name", "category", "sub_category", "brand")
                .agg(
                    F.sum("quantity").alias("units_sold"),
                    F.sum(line_revenue_col).alias("product_revenue"),
                    F.countDistinct("order_id").alias("orders_count"),
                    F.avg("profit_margin").alias("avg_product_margin")
                )
            )
        else:
            product_campaign_units = (
                order_product_campaign
                .groupBy("campaign_id", "product_id", "product_name", "category", "sub_category", "brand")
                .agg(
                    F.sum("quantity").alias("units_sold"),
                    F. countDistinct("order_id").alias("orders_count"),
                    F.avg("profit_margin").alias("avg_product_margin")
                )
            )
            
            campaign_revenue = (
                orders_with_campaign
                .groupBy("campaign_id_attributed")
                .agg(F.sum("total_amount").alias("campaign_revenue"))
                .withColumnRenamed("campaign_id_attributed", "campaign_id")
            )
            
            campaign_product_agg = (
                product_campaign_units
                .join(campaign_revenue, on="campaign_id", how="left")
                .withColumn("product_revenue", F.lit(None).cast("double"))
            )
        
        if (
            "agg_marketing_campaigns" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "campaign_id")
            and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "revenue_generated")
        ):
            campaign_totals = (
                dataframes["agg_marketing_campaigns"]
                .select("campaign_id", F.col("revenue_generated").alias("campaign_revenue_marketing"))
            )
            campaign_product_agg = (
                campaign_product_agg
                . join(campaign_totals, on="campaign_id", how="left")
                .withColumn("campaign_revenue", F.coalesce(F.col("campaign_revenue_marketing"), F.col("campaign_revenue")))
                .drop("campaign_revenue_marketing")
            )
        else:
            if "campaign_revenue" not in campaign_product_agg.columns:
                campaign_product_agg = campaign_product_agg.withColumn("campaign_revenue", F.lit(None).cast("double"))
        
        campaign_product_agg = (
            campaign_product_agg
            . withColumn(
                "product_revenue_share",
                F.when(
                    (F.col("product_revenue").isNotNull()) & (F.col("campaign_revenue").isNotNull()) & (F.col("campaign_revenue") > 0),
                    F.col("product_revenue") / F.col("campaign_revenue")
                ).otherwise(None)
            )
            .fillna({"units_sold": 0, "orders_count": 0, "product_revenue": 0.0, "campaign_revenue": 0.0, "avg_product_margin": 0.0})
        )
        
        analysis["campaign_product_contribution"] = campaign_product_agg. orderBy(
            F.col("campaign_revenue").desc_nulls_last(),
            F.col("product_revenue").desc_nulls_last()
        )

    # 4) Campaign-Customer LTV
    if (
        campaign_order_col is not None
        and "agg_orders" in dataframes
        and "agg_customers" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_lifetime_value")
    ):
        orders_with_campaign = (
            dataframes["agg_orders"]
            .select("order_id", "customer_id", campaign_order_col, "total_amount")
            .withColumnRenamed(campaign_order_col, "campaign_id_attributed")
            .filter(F.col("campaign_id_attributed").isNotNull())
        )
        
        orders_customers = orders_with_campaign.join(
            dataframes["agg_customers"]. select("customer_id", "customer_lifetime_value"),
            on="customer_id", how="left"
        )
        
        analysis["campaign_ltv"] = (
            orders_customers
            .groupBy("campaign_id_attributed")
            .agg(
                F.countDistinct("customer_id").alias("distinct_customers"),
                F.sum("total_amount").alias("campaign_revenue_from_orders"),
                F.avg("customer_lifetime_value").alias("avg_customer_lifetime_value"),
                F.sum(F.when(F.col("customer_lifetime_value").isNotNull(), 1).otherwise(0)).alias("num_customers_with_clv")
            )
            .withColumnRenamed("campaign_id_attributed", "campaign_id")
            .fillna({"distinct_customers": 0, "campaign_revenue_from_orders": 0.0, "avg_customer_lifetime_value": 0.0, "num_customers_with_clv": 0})
            .orderBy(F.col("avg_customer_lifetime_value").desc_nulls_last())
        )
        
        try:
            high_clv_cutoff = dataframes["agg_customers"].approxQuantile("customer_lifetime_value", [0.9], 0.01)[0]
            
            if high_clv_cutoff and high_clv_cutoff > 0:
                high_clv_share = (
                    orders_customers
                    .withColumn("is_high_clv", F. when(F.col("customer_lifetime_value") >= high_clv_cutoff, 1).otherwise(0))
                    .groupBy("campaign_id_attributed")
                    .agg(
                        F.sum("is_high_clv").alias("high_clv_customers"),
                        F.countDistinct("customer_id").alias("distinct_customers")
                    )
                    .withColumnRenamed("campaign_id_attributed", "campaign_id")
                    .withColumn("high_clv_share", F.when(F.col("distinct_customers") > 0, F.col("high_clv_customers") / F.col("distinct_customers")).otherwise(0.0))
                    .fillna({"high_clv_customers":  0, "distinct_customers":  0, "high_clv_share":  0.0})
                )
                
                analysis["campaign_ltv"] = (
                    analysis["campaign_ltv"]
                    .join(high_clv_share. select("campaign_id", "high_clv_customers", "high_clv_share"), on="campaign_id", how="left")
                    .fillna({"high_clv_customers": 0, "high_clv_share": 0.0})
                )
        except:
            pass
        
        analysis["campaign_customer_ltv_summary"] = analysis["campaign_ltv"]

    # 5) Wasteful Campaigns
    if "campaign_performance_summary" in analysis: 
        cps = analysis["campaign_performance_summary"]
        
        if "clicks" in cps.columns and "revenue_generated" in cps. columns:
            if not is_column_all_null_or_zero(cps, "clicks") and not is_column_all_null_or_zero(cps, "revenue_generated"):
                analysis["campaign_wasteful_campaigns"] = (
                    cps
                    .withColumn("revenue_per_click_recalc", F.when(F.col("clicks") > 0, F.col("revenue_generated") / F.col("clicks")).otherwise(0.0))
                    .withColumn("is_low_efficiency", F.when(F.col("revenue_per_click_recalc") < 1.0, 1).otherwise(0))
                    .orderBy(F.col("revenue_per_click_recalc").asc_nulls_last(), F.col("clicks").desc_nulls_last())
                )

    # 6) Campaign Margin Profile
    if "campaign_product_contribution" in analysis:
        cpc = analysis["campaign_product_contribution"]
        
        if "avg_product_margin" in cpc.columns and not is_column_all_null_or_zero(cpc, "avg_product_margin"):
            analysis["campaign_margin_profile"] = (
                cpc
                . groupBy("campaign_id")
                .agg(
                    F.avg("avg_product_margin").alias("campaign_avg_product_margin"),
                    F.sum("product_revenue").alias("campaign_products_revenue"),
                    F.sum("units_sold").alias("campaign_units_sold")
                )
                .fillna({"campaign_avg_product_margin": 0.0, "campaign_products_revenue": 0.0, "campaign_units_sold": 0})
                .orderBy(F.col("campaign_products_revenue").desc_nulls_last())
            )



    if (
        "agg_product_inventory_health" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "current_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "available_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "minimum_stock_level")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "reorder_point_breach_count")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "avg_daily_sales")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "days_of_supply")
    ):
        inv_src = dataframes["agg_product_inventory_health"]
        inv = inv_src.fillna(
            {
                "current_stock": 0,
                "available_stock": 0,
                "minimum_stock_level": 0,
                "reorder_point_breach_count": 0,
                "avg_daily_sales": 0.0,
            }
        )

        # Layer 2: If days_of_supply missing, approximate from avg_daily_sales
        if "days_of_supply" not in inv.columns:
            inv = inv.withColumn(
                "days_of_supply",
                F.when(F.col("avg_daily_sales") > 0,
                    F.col("available_stock") / F.col("avg_daily_sales"))
                .otherwise(F.lit(None)),
            )

        # Compute stock_status_computed using business rules:
        inv = inv.withColumn(
            "stock_status_computed",
            F.when(
                (F.col("available_stock") <= 0)
                | (F.col("reorder_point_breach_count") > 0)
                | (F.col("days_of_supply").isNotNull() & (F.col("days_of_supply") < 7)),
                F.lit("critical"),
            )
            .when(
                (F.col("days_of_supply").isNotNull() & (F.col("days_of_supply") < 30))
                | (
                    F.col("minimum_stock_level").isNotNull()
                    & (F.col("available_stock") <= F.col("minimum_stock_level") * 1.5)
                ),
                F.lit("low"),
            )
            .otherwise(F.lit("healthy")),
        )

        product_analysis["inventory_stock_status"] = (
            inv.select(
                "product_id",
                *[c for c in ["supplier_id"] if c in inv.columns],
                *[c for c in ["available_stock", "current_stock", "minimum_stock_level"] if c in inv.columns],
                "days_of_supply",
                "stock_status_computed",
            )
            .orderBy(
                F.col("stock_status_computed").asc(),
                F.col("available_stock").asc_nulls_last(),
            )
        )

    elif (
        "agg_inventory" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_inventory"], "product_id")
    ):
        inv_src = dataframes["agg_inventory"]
        inv = inv_src.fillna(
            {
                "stock_quantity": 0,
                "available_stock": 0,
                "minimum_stock_level": 0,
            }
        )

        # Classification purely on levels and minimum_stock_level
        inv = inv.withColumn(
            "stock_status_computed",
            F.when(
                (F.col("available_stock") <= 0)
                | (F.col("reorder_point_breach") == 1),
                F.lit("critical"),
            )
            .when(
                F.col("available_stock") <= F.col("minimum_stock_level") * 1.5,
                F.lit("low"),
            )
            .otherwise(F.lit("healthy")),
        )

        product_analysis["inventory_stock_status"] = (
            inv.select(
                "product_id",
                "available_stock",
                "stock_quantity",
                "minimum_stock_level",
                "reorder_point_breach",
                "stock_status_computed",
            )
            .orderBy(
                F.col("stock_status_computed").asc(),
                F.col("available_stock").asc_nulls_last(),
            )
        )

    else:
        print("Required inventory tables not found or critical columns are null/zero.")




    # -------------------------------------------------------
    # Days of Supply Analysis
    # -------------------------------------------------------

    # Layer 1: Check primary health table
    if (
        "agg_product_inventory_health" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "current_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "available_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "avg_daily_sales")
    ):
        # Layer 2: Validate columns for health-based calculation
        inv = dataframes["agg_product_inventory_health"].fillna(
            {
                "current_stock": 0,
                "available_stock": 0,
                "avg_daily_sales": 0.0,
            }
        )

        # Respect existing days_of_supply if present, otherwise calculate
        if "days_of_supply" not in inv.columns:
            inv = inv.withColumn(
                "days_of_supply",
                F.when(
                    F.col("avg_daily_sales") > 0,
                    F.col("available_stock") / F.col("avg_daily_sales"),
                ).otherwise(F.lit(None)),
            )

        product_analysis["days_of_supply"] = (
            inv.select(
                "product_id",
                *[c for c in ["supplier_id"] if c in inv.columns],
                "available_stock",
                "avg_daily_sales",
                "days_of_supply",
            )
            .orderBy(F.col("days_of_supply").asc_nulls_last())
        )

    # Layer 1 Fallback: Check standard inventory table
    elif (
        "agg_inventory" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_inventory"], "product_id")
    ):
        # Layer 2: Validate columns for fallback calculation
        inv = dataframes["agg_inventory"].fillna(
            {
                "available_stock": 0,
                "stock_quantity": 0,
                "total_sold": 0,
                "avg_inventory": 0.0,
            }
        )

        # Derive avg_daily_sales based on available data grains
        if "avg_daily_sales" in inv.columns and not is_column_all_null_or_zero(inv, "avg_daily_sales"):
            pass 
        else:
            # Option A: Turnover Ratio
            if "stock_turnover_ratio" in inv.columns and not is_column_all_null_or_zero(inv, "stock_turnover_ratio"):
                inv = inv.withColumn(
                    "avg_daily_sales",
                    F.col("stock_turnover_ratio") * F.col("avg_inventory") / F.lit(365.0),
                )
            # Option B: Last Restock Date
            elif "last_restocked_date" in inv.columns and not is_column_all_null_or_zero(inv, "last_restocked_date"):
                inv = inv.withColumn(
                    "days_since_restock",
                    F.datediff(F.current_date(), F.col("last_restocked_date")),
                ).withColumn(
                    "avg_daily_sales",
                    F.when(
                        F.col("days_since_restock") > 0,
                        F.col("total_sold") / F.col("days_since_restock"),
                    ).otherwise(F.lit(0.0)),
                )
            else:
                inv = inv.withColumn("avg_daily_sales", F.lit(0.0))

        inv = inv.withColumn(
            "days_of_supply",
            F.when(
                F.col("avg_daily_sales") > 0,
                F.col("available_stock") / F.col("avg_daily_sales"),
            ).otherwise(F.lit(None)),
        )

        product_analysis["days_of_supply"] = (
            inv.select(
                "product_id",
                "available_stock",
                "stock_quantity",
                "avg_daily_sales",
                "days_of_supply",
            )
            .orderBy(F.col("days_of_supply").asc_nulls_last())
        )

    else:
        print(
            "agg_product_inventory_health or agg_inventory not available, or product_id column is all NULL/zero; "
            "skipping days of supply analysis."
        )




    # Layer 1: Check primary health table
    if (
        "agg_product_inventory_health" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "available_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "current_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "minimum_stock_level")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "reorder_point_breach_count")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "avg_daily_sales")
    ):
        # Layer 2: Validate columns and fill numeric defaults
        inv = dataframes["agg_product_inventory_health"].fillna(
            {
                "available_stock": 0,
                "current_stock": 0,
                "minimum_stock_level": 0,
                "reorder_point_breach_count": 0,
                "avg_daily_sales": 0.0,
            }
        )

        # Ensure days_of_supply exists for scoring
        if (
            "days_of_supply" not in inv.columns
            or is_column_all_null_or_zero(inv, "days_of_supply")
        ):
            inv = inv.withColumn(
                "days_of_supply",
                F.when(
                    F.col("avg_daily_sales") > 0,
                    F.col("available_stock") / F.col("avg_daily_sales"),
                ).otherwise(F.lit(None)),
            )

        # Reorder urgency scoring logic
        inv = inv.withColumn(
            "reorder_urgency_score",
            F.when(F.col("reorder_point_breach_count") > 0, 100).otherwise(0)
            + F.when(
                F.col("days_of_supply").isNotNull(),
                F.when(F.col("days_of_supply") < 7, 50)
                .when(F.col("days_of_supply") < 30, 20)
                .otherwise(0),
            ).otherwise(10)
            + F.when(
                (F.col("minimum_stock_level") > 0)
                & (F.col("available_stock") <= F.col("minimum_stock_level")),
                30,
            ).otherwise(0),
        )

        inv = inv.withColumn(
            "reorder_urgency_tier",
            F.when(F.col("reorder_urgency_score") >= 100, "urgent")
            .when(F.col("reorder_urgency_score") >= 30, "high")
            .when(F.col("reorder_urgency_score") >= 10, "medium")
            .otherwise("low"),
        )

        product_analysis["sku_reorder_urgency"] = (
            inv.select(
                "product_id",
                *[c for c in ["product_name", "category"] if c in inv.columns],
                "available_stock",
                "current_stock",
                "minimum_stock_level",
                "reorder_point_breach_count",
                "days_of_supply",
                "reorder_urgency_score",
                "reorder_urgency_tier",
            ).orderBy(F.col("reorder_urgency_score").desc_nulls_last())
        )

    # Layer 1 Fallback: Check standard inventory table
    elif (
        "agg_inventory" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_inventory"], "product_id")
    ):
        # Layer 2: Numeric fill for fallback
        inv = dataframes["agg_inventory"].fillna(
            {
                "available_stock": 0,
                "stock_quantity": 0,
                "minimum_stock_level": 0,
                "reorder_point_breach": 0,
            }
        )

        inv = inv.withColumn(
            "reorder_urgency_score",
            F.when(F.col("reorder_point_breach") == 1, 100).otherwise(0)
            + F.when(
                (F.col("minimum_stock_level") > 0)
                & (F.col("available_stock") <= F.col("minimum_stock_level")),
                40,
            ).otherwise(0)
            + F.when(
                (F.col("minimum_stock_level") > 0)
                & (F.col("available_stock") <= F.col("minimum_stock_level") * 1.5),
                20,
            ).otherwise(0),
        )

        inv = inv.withColumn(
            "reorder_urgency_tier",
            F.when(F.col("reorder_urgency_score") >= 100, "urgent")
            .when(F.col("reorder_urgency_score") >= 30, "high")
            .when(F.col("reorder_urgency_score") >= 10, "medium")
            .otherwise("low"),
        )

        product_analysis["sku_reorder_urgency"] = (
            inv.select(
                "product_id",
                "available_stock",
                "stock_quantity",
                "minimum_stock_level",
                "reorder_point_breach",
                "reorder_urgency_score",
                "reorder_urgency_tier",
            ).orderBy(F.col("reorder_urgency_score").desc_nulls_last())
        )

    else:
        print("Required inventory data missing; skipping reorder urgency analysis.")




    # -------------------------------------------------------
    # Reorder Point Breach Frequency: products frequently breaching reorder point
    # -------------------------------------------------------
    if (
        "agg_product_inventory_health" in dataframes
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "product_id")
    ):
        # 1) Base DF: DO NOT use None in fillna, only scalars
        inv = dataframes["agg_product_inventory_health"].fillna(
            {
                "reorder_point_breach_count": 0,
                "current_stock": 0,
                "available_stock": 0,
                "stock_health_score": 0,
                # don't touch days_of_supply / reorder_urgency here; leave them as-is
            }
        )

        # 2) Optional enrich with product metadata (NO stock_status to avoid ambiguity)
        if (
            "agg_products" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        ):
            prod = dataframes["agg_products"].select(
                "product_id",
                "product_name",
                "category",
                "total_units_sold",
                "total_revenue"
                # skip agg_products.stock_status on purpose
            )
            inv = inv.alias("i").join(prod.alias("p"), on="product_id", how="left")

        # 3) Threshold for "frequent" breaches
        BREACH_THRESHOLD = 3

        # 4) Final selection – simple, explicit
        #    (use stock_status from agg_product_inventory_health only)
        product_analysis["reorder_point_breach_frequency"] = (
            inv.filter(F.col("reorder_point_breach_count") >= BREACH_THRESHOLD)
            .select(
                "product_id",
                "supplier_id",
                "reorder_point_breach_count",
                "stock_health_score",
                "reorder_urgency",
                "current_stock",
                "available_stock",
                "days_of_supply",
                "stock_status",
                "product_name",
                "category",
                "total_units_sold",
                "total_revenue",
            )
            .orderBy(
                F.col("reorder_point_breach_count").desc_nulls_last(),
                F.col("stock_health_score").asc_nulls_last(),
            )
        )

    # -------------------------------------------------------
    # Fallback: use agg_inventory if agg_product_inventory_health not available
    # -------------------------------------------------------
    elif (
        "agg_inventory" in dataframes
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_inventory"], "product_id")
    ):
        inv = dataframes["agg_inventory"].fillna(
            {
                "reorder_point_breach": 0,
                "stock_quantity": 0,
                "available_stock": 0,
            }
        )

        # Aggregate breaches per product/supplier
        breaches_agg = (
            inv.groupBy("product_id", "supplier_id")
            .agg(
                F.sum("reorder_point_breach").alias("reorder_point_breach_count"),
                F.max("stock_quantity").alias("latest_stock_quantity"),
                F.max("available_stock").alias("latest_available_stock"),
            )
        )

        # Optional enrich with product metadata (again, skip stock_status to keep it simple)
        if (
            "agg_products" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        ):
            prod = dataframes["agg_products"].select(
                "product_id",
                "product_name",
                "category",
                "total_units_sold",
                "total_revenue",
            )
            breaches_agg = breaches_agg.alias("b").join(prod.alias("p"), on="product_id", how="left")

        BREACH_THRESHOLD = 3

        product_analysis["reorder_point_breach_frequency"] = (
            breaches_agg.filter(F.col("reorder_point_breach_count") >= BREACH_THRESHOLD)
            .select(
                "product_id",
                "supplier_id",
                "reorder_point_breach_count",
                "latest_stock_quantity",
                "latest_available_stock",
                "product_name",
                "category",
                "total_units_sold",
                "total_revenue",
            )
            .orderBy(F.col("reorder_point_breach_count").desc_nulls_last())
        )

    else:
        print(
            "agg_product_inventory_health or agg_inventory not available, or product_id column is all NULL/zero; "
            "skipping reorder point breach frequency analysis."
        )





    # Overstock Analysis: low stock_health_score & high storage cost
    if (
        "agg_product_inventory_health" in dataframes
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "stock_health_score")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "storage_cost")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "storage_cost_per_unit")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "current_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "available_stock")
    ):
        # 1) Base inventory DF – do NOT use None in fillna
        inv = dataframes["agg_product_inventory_health"].fillna(
            {
                "stock_health_score": 100,
                "storage_cost": 0.0,
                "storage_cost_per_unit": 0.0,
                "current_stock": 0,
                "available_stock": 0,
                # leave days_of_supply as-is (don’t fill with None)
            }
        )

        # 2) Optional enrich with product metadata
        if (
            "agg_products" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        ):
            prod = dataframes["agg_products"].select(
                "product_id",
                "product_name",
                "category",
                "total_units_sold",
                "total_revenue"
                # skip agg_products.stock_status to avoid ambiguity
            )
            inv = inv.alias("i").join(prod.alias("p"), on="product_id", how="left")

        # 3) Thresholds
        LOW_HEALTH_THRESHOLD = dataframes["agg_product_inventory_health"].select(F.avg("stock_health_score")).collect()[0][0]            # stock_health_score < 40
        HIGH_STORAGE_COST =   dataframes["agg_product_inventory_health"].select(F.avg("storage_cost")).collect()[0][0]       # absolute storage cost
        HIGH_STORAGE_COST_PER_UNIT = dataframes["agg_product_inventory_health"].select(F.avg("storage_cost_per_unit")).collect()[0][0]   # per-unit storage cost

        # 4) Flag overstock
        overstock = (
            inv.withColumn(
                "is_overstock",
                F.when(
                    (F.col("stock_health_score") < LOW_HEALTH_THRESHOLD)
                    & (
                        (F.col("storage_cost") >= HIGH_STORAGE_COST)
                        | (F.col("storage_cost_per_unit") >= HIGH_STORAGE_COST_PER_UNIT)
                    ),
                    1,
                ).otherwise(0),
            )
            .filter(F.col("is_overstock") == 1)
            .select(
                "product_id",
                "supplier_id",
                "stock_health_score",
                "storage_cost",
                "storage_cost_per_unit",
                "current_stock",
                "available_stock",
                "days_of_supply",
                "stock_status",   # from agg_product_inventory_health
                "product_name",
                "category",
                "total_units_sold",
                "total_revenue",
            )
            .orderBy(
                F.col("storage_cost").desc_nulls_last(),
                F.col("storage_cost_per_unit").desc_nulls_last(),
                F.col("stock_health_score").asc_nulls_last(),
            )
        )

        product_analysis["overstock_analysis"] = overstock

    else:
        print(
            "agg_product_inventory_health not available, or product_id column is all NULL/zero; "
            "skipping overstock analysis."
        )





    # Storage Cost Efficiency: total_storage_cost / total_available_stock by supplier
    if (
        "agg_supplier_inventory_health" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_supplier_inventory_health"], "supplier_id")
        and not is_column_all_null_or_zero(dataframes["agg_supplier_inventory_health"], "total_storage_cost")
        and not is_column_all_null_or_zero(dataframes["agg_supplier_inventory_health"], "total_available_stock")
        and not is_column_all_null_or_zero(dataframes["agg_supplier_inventory_health"], "total_current_stock")
        and not is_column_all_null_or_zero(dataframes["agg_supplier_inventory_health"], "avg_storage_cost_per_unit")
        and not is_column_all_null_or_zero(dataframes["agg_supplier_inventory_health"], "supplier_inventory_health_score")
    ):
        sup_inv = dataframes["agg_supplier_inventory_health"].fillna(
            {
                "total_storage_cost": 0.0,
                "total_available_stock": 0,
                "total_current_stock": 0,
                "avg_storage_cost_per_unit": 0.0,
                "supplier_inventory_health_score": 0.0,
            }
        )

        efficiency = (
            sup_inv.withColumn(
                "storage_cost_efficiency",
                F.when(
                    F.col("total_available_stock") > 0,
                    F.col("total_storage_cost") / F.col("total_available_stock")
                ).otherwise(F.lit(0.0)),
            )
            .select(
                "supplier_id",
                "total_storage_cost",
                "total_available_stock",
                "total_current_stock",
                "avg_storage_cost_per_unit",
                "supplier_inventory_health_score",
                "storage_cost_efficiency",
            )
            .orderBy(
                F.col("storage_cost_efficiency").desc_nulls_last(),
                F.col("total_storage_cost").desc_nulls_last(),
            )
        )

        supplier_analysis["storage_cost_efficiency_by_supplier"] = efficiency

    else:
        print(
            "agg_supplier_inventory_health not available, or supplier_id column is all NULL/zero; "
            "skipping storage cost efficiency by supplier analysis."
        )





    # Primary: product-level storage_cost in agg_product_inventory_health
    if (
        "agg_product_inventory_health" in dataframes
        and "agg_products" in dataframes
        and "agg_suppliers" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "storage_cost")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "storage_cost_per_unit")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "available_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "current_stock")
    ):
        inv = dataframes["agg_product_inventory_health"].fillna(
            {
                "storage_cost": 0.0,
                "storage_cost_per_unit": 0.0,
                "available_stock": 0,
                "current_stock": 0,
            }
        )

        # Optional enrich with product/category
        if (
            "agg_products" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        ):
            prod = dataframes["agg_products"].select(
                "product_id",
                "product_name",
                "category",
            )
            inv = inv.alias("i").join(prod.alias("p"), on="product_id", how="left")

        # 1) Overall inventory carrying cost (total storage_cost)
        analysis["inventory_carrying_cost_overall"] = inv.agg(
            F.sum("storage_cost").alias("total_inventory_carrying_cost")
        )

        # 2) By supplier
        supplier_analysis["inventory_carrying_cost_by_supplier"] = (
            inv.groupBy("supplier_id")
            .agg(
                F.sum("storage_cost").alias("total_storage_cost"),
                F.sum("available_stock").alias("total_available_stock"),
            )
            .orderBy(F.col("total_storage_cost").desc_nulls_last())
        )

        # 3) By product/category
        product_cols = [
            "product_id",
            "product_name",
            "category",
            "supplier_id",
            "storage_cost",
            "available_stock",
            "current_stock",
            "storage_cost_per_unit",
        ]
        product_analysis["inventory_carrying_cost_by_product"] = (
            inv.select(*[c for c in product_cols if c in inv.columns])
            .orderBy(F.col("storage_cost").desc_nulls_last())
        )

    # Fallback: supplier-level total_storage_cost in agg_supplier_inventory_health
    elif (
        "agg_supplier_inventory_health" in dataframes
        and "agg_suppliers" in dataframes
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_supplier_inventory_health"], "supplier_id")
    ):
        sup_inv = dataframes["agg_supplier_inventory_health"].fillna(
            {
                "total_storage_cost": 0.0,
            }
        )

        analysis["inventory_carrying_cost_overall"] = sup_inv.agg(
            F.sum("total_storage_cost").alias("total_inventory_carrying_cost")
        )

        supplier_analysis["inventory_carrying_cost_by_supplier"] = (
            sup_inv.select(
                "supplier_id",
                "total_storage_cost",
                "total_available_stock",
                "total_current_stock",
            )
            .orderBy(F.col("total_storage_cost").desc_nulls_last())
        )

    else:
        print(
            "agg_product_inventory_health or agg_supplier_inventory_health not available, "
            "or key id columns all NULL/zero; skipping inventory carrying cost analysis."
        )



    # Margin Erosion Risk: products with high storage cost relative to sales
    if (
        "agg_product_inventory_health" in dataframes
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "storage_cost")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "storage_cost_per_unit")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "current_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "available_stock")
    ):
        inv = dataframes["agg_product_inventory_health"].fillna(
            {
                "storage_cost": 0.0,
                "storage_cost_per_unit": 0.0,
                "available_stock": 0,
                "current_stock": 0,
                "stock_health_score": 100,
            }
        )

        # Optional enrich with product sales metrics
        if (
            "agg_products" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        ):
            prod = dataframes["agg_products"].select(
                "product_id",
                "product_name",
                "category",
                "total_units_sold",
                "total_revenue",
                "avg_order_value_product",
            )
            inv = inv.alias("i").join(prod.alias("p"), on="product_id", how="left")
        else:
            prod = None

        # Ratios indicating margin pressure
        inv = (
            inv
            # storage_cost / total_revenue (how much of revenue is being eaten by storage)
            .withColumn(
                "storage_cost_to_revenue",
                F.when(
                    F.col("total_revenue") > 0,
                    F.col("storage_cost") / F.col("total_revenue"),
                ).otherwise(F.lit(None)),
            )
            # storage_cost_per_unit / avg_order_value_product (per-unit storage vs price)
            .withColumn(
                "storage_cost_per_unit_to_price",
                F.when(
                    F.col("avg_order_value_product") > 0,
                    F.col("storage_cost_per_unit") / F.col("avg_order_value_product"),
                ).otherwise(F.lit(None)),
            )
        )

        # Thresholds for "at risk" (tune to your business):
        # - storage cost > 10% of revenue OR
        # - per-unit storage > 20% of unit selling price
        STORAGE_TO_REV_THRESHOLD = inv.agg(F.percentile_approx("storage_cost_to_revenue", 0.75)).collect()[0][0]  # e.g., 0.10
        STORAGE_PER_UNIT_TO_PRICE_THRESHOLD = inv.agg(F.percentile_approx("storage_cost_per_unit_to_price", 0.75)).collect()[0][0]  # e.g., 0.20

        risk_df = (
            inv.withColumn(
                "margin_erosion_risk",
                F.when(
                    (
                        (F.col("storage_cost_to_revenue") >= STORAGE_TO_REV_THRESHOLD)
                        | (F.col("storage_cost_per_unit_to_price") >= STORAGE_PER_UNIT_TO_PRICE_THRESHOLD)
                    )
                    # And stock is non-trivial (otherwise cost is negligible)
                    & (F.col("available_stock") > 0),
                    1,
                ).otherwise(0),
            )
            .filter(F.col("margin_erosion_risk") == 1)
            .select(
                "product_id",
                *[c for c in ["product_name", "category"] if c in inv.columns],
                "supplier_id",
                "storage_cost",
                "storage_cost_per_unit",
                "available_stock",
                "current_stock",
                "stock_health_score",
                *[c for c in ["total_units_sold", "total_revenue", "avg_order_value_product"] if c in inv.columns],
                "storage_cost_to_revenue",
                "storage_cost_per_unit_to_price",
            )
            .orderBy(
                F.col("storage_cost_to_revenue").desc_nulls_last(),
                F.col("storage_cost_per_unit_to_price").desc_nulls_last(),
                F.col("storage_cost").desc_nulls_last(),
            )
        )

        product_analysis["margin_erosion_risk"] = risk_df

    else:
        print(
            "agg_product_inventory_health not available, or product_id column is all NULL/zero; "
            "skipping margin erosion risk analysis."
        )



    if (
        "agg_product_inventory_health" in dataframes
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "available_stock")
        and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "reserved_quantity")
    ):
        inv = dataframes["agg_product_inventory_health"].fillna(
            {
                "available_stock": 0,
                "reserved_quantity": 0,
            }
        )

        product_analysis["reserved_vs_available"] = (
            inv.select(
                "product_id",
                *[c for c in ["product_name", "category", "supplier_id"] if c in inv.columns],
                "available_stock",
                "reserved_quantity",
            )
            .withColumn(
                "total_stock",
                F.col("available_stock") + F.col("reserved_quantity"),
            )
            .withColumn(
                "reserved_share",
                F.when(
                    F.col("total_stock") > 0,
                    F.col("reserved_quantity") / F.col("total_stock"),
                ).otherwise(F.lit(0.0)),
            )
            .orderBy(F.col("reserved_share").desc_nulls_last())
        )

    elif (
        "agg_inventory" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_inventory"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_inventory"], "available_stock")
        and not is_column_all_null_or_zero(dataframes["agg_inventory"], "reserved_quantity")
    ):
        inv = dataframes["agg_inventory"].fillna(
            {
                "available_stock": 0,
                "reserved_quantity": 0,
                "stock_quantity": 0,
            }
        )

        product_analysis["reserved_vs_available"] = (
            inv.select(
                "product_id",
                *[c for c in ["supplier_id"] if c in inv.columns],
                "available_stock",
                "reserved_quantity",
                "stock_quantity",
            )
            .withColumn(
                "total_stock",
                F.when(
                    F.col("stock_quantity") > 0,
                    F.col("stock_quantity"),
                ).otherwise(F.col("available_stock") + F.col("reserved_quantity")),
            )
            .withColumn(
                "reserved_share",
                F.when(
                    F.col("total_stock") > 0,
                    F.col("reserved_quantity") / F.col("total_stock"),
                ).otherwise(F.lit(0.0)),
            )
            .orderBy(F.col("reserved_share").desc_nulls_last())
        )

    else:
        print(
            "agg_product_inventory_health or agg_inventory not available, or product_id "
            "column is all NULL/zero; skipping reserved vs available stock analysis."
        )


    # Excess inventory: items not selling
    # Preferred source: agg_product_inventory_health
    if "agg_product_inventory_health" in dataframes and "products" in dataframes:
        if "product_id" in dataframes["agg_products"].columns and "product_id" in dataframes["agg_product_inventory_health"].columns:    
            if (
                "agg_product_inventory_health" in dataframes
                and "agg_products" in dataframes
                and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "product_id")
                and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "available_stock")
                and not is_column_all_null_or_zero(dataframes["agg_product_inventory_health"], "current_stock")
                and not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold")
                and not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue")
                and not is_column_all_null_or_zero(dataframes["agg_products"], "avg_order_value_product")
            ):
                inv = dataframes["agg_product_inventory_health"].fillna(
                    {
                        "available_stock": 0,
                        "current_stock": 0,
                    }
                )

                # Try to enrich with product-level sales if agg_products exists
                if (
                    "agg_products" in dataframes
                    and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
                ):
                    prod = dataframes["agg_products"].select(
                        "product_id",
                        "product_name",
                        "category",
                        "total_units_sold",
                        "total_revenue",
                        "avg_order_value_product",
                        "days_since_launch",
                    )

                    inv = (
                        inv.alias("i")
                        .join(prod.alias("p"), on="product_id", how="left")
                    )
                else:
                    prod = None

                # Fallback for avg_daily_sales if missing: use product total_units_sold over a 180-day window
                if "avg_daily_sales" in inv.columns:
                    inv = inv.withColumn(
                        "avg_daily_sales_effective",
                        F.when(F.col("avg_daily_sales").isNotNull(), F.col("avg_daily_sales"))
                        .otherwise(
                            F.when(
                                F.col("total_units_sold").isNotNull() & (F.col("total_units_sold") > 0),
                                F.col("total_units_sold") / F.lit(180.0),
                            ).otherwise(F.lit(0.0))
                        ),
                    )
                else:
                    inv = inv.withColumn(
                        "avg_daily_sales_effective",
                        F.when(
                            F.col("total_units_sold").isNotNull() & (F.col("total_units_sold") > 0),
                            F.col("total_units_sold") / F.lit(180.0),
                        ).otherwise(F.lit(0.0)),
                    )

                # Ensure days_of_supply exists: prefer existing, else derive from available_stock / avg_daily_sales_effective
                if "days_of_supply" in inv.columns and not is_column_all_null_or_zero(inv, "days_of_supply"):
                    inv = inv.withColumn(
                        "days_of_supply_effective",
                        F.col("days_of_supply"),
                    )
                else:
                    inv = inv.withColumn(
                        "days_of_supply_effective",
                        F.when(
                            F.col("avg_daily_sales_effective") > 0,
                            F.col("available_stock") / F.col("avg_daily_sales_effective"),
                        ).otherwise(F.lit(None)),
                    )

                # Thresholds (you can tune these):
            
                EXCESS_DOS = inv.agg(F.percentile_approx("days_of_supply_effective", 0.75)).collect()[0][0]  # 75th percentile
                LOW_SALES_RATE = inv.agg(F.percentile_approx("avg_daily_sales_effective", 0.25)).collect()[0][0]  # 25th percentile

                excess = (
                    inv.withColumn(
                        "is_excess_inventory",
                        F.when(
                            (F.col("available_stock") > 0)
                            & (F.col("days_of_supply_effective").isNotNull())
                            & (F.col("days_of_supply_effective") >= F.lit(EXCESS_DOS))
                            & (F.col("avg_daily_sales_effective") <= F.lit(LOW_SALES_RATE)),
                            1,
                        ).otherwise(0),
                    )
                    .filter(F.col("is_excess_inventory") == 1)
                    .select(
                        "product_id",
                        *[c for c in ["product_name", "category"] if c in inv.columns],
                        "supplier_id",
                        "available_stock",
                        "current_stock",
                        "avg_daily_sales_effective",
                        "days_of_supply_effective",
                        *[c for c in ["total_units_sold", "total_revenue", "days_since_launch"] if c in inv.columns],
                        "stock_health_score",
                        "reorder_urgency",
                    )
                    .orderBy(
                        F.col("days_of_supply_effective").desc_nulls_last(),
                        F.col("available_stock").desc_nulls_last(),
                    )
                )

                product_analysis["excess_inventory_not_selling"] = excess

            # Fallback: use agg_inventory + agg_products if product_inventory_health not usable
            elif (
                "agg_inventory" in dataframes
                and not is_column_all_null_or_zero(dataframes["agg_inventory"], "product_id")
                and not is_column_all_null_or_zero(dataframes["agg_inventory"], "available_stock")
                and not is_column_all_null_or_zero(dataframes["agg_inventory"], "stock_quantity")
                and "agg_products" in dataframes
                and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
                and not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold")
                and not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue")
                and not is_column_all_null_or_zero(dataframes["agg_products"], "avg_order_value_product")
            ):
                inv = dataframes["agg_inventory"].fillna(
                    {
                        "available_stock": 0,
                        "stock_quantity": 0,
                        "total_sold": 0,
                    }
                )

                base = inv
                if (
                    "agg_products" in dataframes
                    and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
                ):
                    prod = dataframes["agg_products"].select(
                        "product_id",
                        "product_name",
                        "category",
                        "total_units_sold",
                        "total_revenue",
                        "days_since_launch",
                    )
                    base = base.alias("i").join(prod.alias("p"), on="product_id", how="left")

                # Approx avg_daily_sales from total_sold or total_units_sold over 180 days
                base = base.withColumn(
                    "total_units_sold_effective",
                    F.coalesce(F.col("total_sold"), F.col("total_units_sold")),
                ).withColumn(
                    "avg_daily_sales_effective",
                    F.when(
                        F.col("total_units_sold_effective") > 0,
                        F.col("total_units_sold_effective") / F.lit(180.0),
                    ).otherwise(F.lit(0.0)),
                )

                base = base.withColumn(
                    "days_of_supply_effective",
                    F.when(
                        F.col("avg_daily_sales_effective") > 0,
                        F.col("available_stock") / F.col("avg_daily_sales_effective"),
                    ).otherwise(F.lit(None)),
                )

                EXCESS_DOS = base.agg(F.percentile_approx("days_of_supply_effective", 0.75)).collect()[0][0]  # 75th percentile
                LOW_SALES_RATE = base.agg(F.percentile_approx("avg_daily_sales_effective", 0.25)).collect()[0][0]  # 25th percentile

                excess = (
                    base.withColumn(
                        "is_excess_inventory",
                        F.when(
                            (F.col("available_stock") > 0)
                            & (F.col("days_of_supply_effective").isNotNull())
                            & (F.col("days_of_supply_effective") >= F.lit(EXCESS_DOS))
                            & (F.col("avg_daily_sales_effective") <= F.lit(LOW_SALES_RATE)),
                            1,
                        ).otherwise(0),
                    )
                    .filter(F.col("is_excess_inventory") == 1)
                    .select(
                        "product_id",
                        *[c for c in ["product_name", "category"] if c in base.columns],
                        "supplier_id",
                        "available_stock",
                        "stock_quantity",
                        "avg_daily_sales_effective",
                        "days_of_supply_effective",
                        *[c for c in ["total_units_sold_effective", "total_revenue", "days_since_launch"] if c in base.columns],
                    )
                    .orderBy(
                        F.col("days_of_supply_effective").desc_nulls_last(),
                        F.col("available_stock").desc_nulls_last(),
                    )
                )

                product_analysis["excess_inventory_not_selling"] = excess

            else:
                print(
                    "agg_product_inventory_health or agg_inventory not available, or product_id column is all NULL/zero; "
                    "skipping excess inventory (items not selling) analysis."
                )
        else:
            print("missing product_id column in agg_products or agg_product_inventory_health")
    else:
        print("missing agg_products table  or inventory data; skipping excess inventory (items not selling) analysis.")



    # High-Value Funnel:  Sessions → Views → Cart → Order
    if (
        "agg_customer_sessions" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "session_id")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "customer_id")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "session_start")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "device_type")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "referrer_source")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "products_viewed")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "total_products_viewed")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "items_added_to_cart")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "orders_from_session")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "converted")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "abandoned")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "cart_value")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "session_engagement_score")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "session_type")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "conversion_flag")
        and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "cart_abandonment_flag")

    ):
        
        funnel = dataframes["agg_customer_sessions"]. select(
            "session_id",
            "customer_id",
            "session_start",
            "device_type",
            "referrer_source",
            "products_viewed",
            "total_products_viewed",
            "items_added_to_cart",
            "orders_from_session",
            "converted",
            "abandoned",
            "cart_value",
            "session_engagement_score",
            "session_type",
            "conversion_flag",
            "cart_abandonment_flag"
        )
        
        # Create funnel step flags
        funnel = (
            funnel
            .withColumn("reached_view", F. when(F.col("total_products_viewed") > 0, 1).otherwise(0))
            .withColumn("reached_cart", F.when(F. col("items_added_to_cart") > 0, 1).otherwise(0))
            .withColumn("reached_order", F.when(F.col("orders_from_session") > 0, 1).otherwise(0))
        )
        
        # Calculate session monetary value
        funnel = funnel.withColumn(
            "session_monetary_value",
            F.coalesce(F.col("cart_value"), F.lit(0.0))
        )
        
        # Mark high-value sessions
        HIGH_VALUE_REVENUE_THRESHOLD = funnel.select(F.avg("session_monetary_value")).collect()[0][0]  # e.g., average session value
        
        funnel = funnel.withColumn(
            "is_high_value_session",
            F.when(F.col("session_monetary_value") >= HIGH_VALUE_REVENUE_THRESHOLD, 1).otherwise(0)
        )
        
        # Calculate conversion metrics
        funnel = funnel. withColumn(
            "view_to_cart_rate",
            F.when(
                F.col("total_products_viewed") > 0,
                F.col("items_added_to_cart") / F.col("total_products_viewed")
            ).otherwise(0.0)
        )
        
        funnel = funnel.withColumn(
            "cart_to_order_rate",
            F.when(
                F.col("items_added_to_cart") > 0,
                F.col("orders_from_session") / F.col("items_added_to_cart")
            ).otherwise(0.0)
        )
        
        funnel = funnel.withColumn(
            "view_to_order_rate",
            F.when(
                F.col("total_products_viewed") > 0,
                F.col("orders_from_session") / F.col("total_products_viewed")
            ).otherwise(0.0)
        )
        
        analysis["high_value_funnel"] = funnel. orderBy(F.col("session_monetary_value").desc_nulls_last())
        
        # Aggregate funnel metrics
        analysis["funnel_summary"] = funnel.agg(
            F.count("*").alias("total_sessions"),
            F.sum("reached_view").alias("sessions_with_views"),
            F.sum("reached_cart").alias("sessions_with_cart"),
            F.sum("reached_order").alias("sessions_with_orders"),
            F.sum("is_high_value_session").alias("high_value_sessions"),
            F.avg("total_products_viewed").alias("avg_products_viewed"),
            F.avg("items_added_to_cart").alias("avg_items_added_to_cart"),
            F.avg("orders_from_session").alias("avg_orders_per_session"),
            F.avg("session_monetary_value").alias("avg_session_value"),
            F.sum("session_monetary_value").alias("total_session_value")
        ).withColumn(
            "view_to_cart_conversion",
            F.when(
                F.col("sessions_with_views") > 0,
                F.col("sessions_with_cart") / F.col("sessions_with_views")
            ).otherwise(0.0)
        ).withColumn(
            "cart_to_order_conversion",
            F.when(
                F.col("sessions_with_cart") > 0,
                F.col("sessions_with_orders") / F.col("sessions_with_cart")
            ).otherwise(0.0)
        ).withColumn(
            "overall_conversion_rate",
            F.when(
                F.col("total_sessions") > 0,
                F.col("sessions_with_orders") / F.col("total_sessions")
            ).otherwise(0.0)
        )
        
        # High-value session analysis
        if not is_column_all_null_or_zero(funnel, "is_high_value_session"):
            analysis["high_value_vs_regular"] = funnel.groupBy("is_high_value_session").agg(
                F.count("*").alias("session_count"),
                F.avg("total_products_viewed").alias("avg_products_viewed"),
                F.avg("items_added_to_cart").alias("avg_items_in_cart"),
                F.avg("orders_from_session").alias("avg_orders"),
                F.avg("session_monetary_value").alias("avg_session_value"),
                F.sum("session_monetary_value").alias("total_value"),
                F.avg("view_to_cart_rate").alias("avg_view_to_cart_rate"),
                F.avg("cart_to_order_rate").alias("avg_cart_to_order_rate"),
                F.avg("view_to_order_rate").alias("avg_view_to_order_rate")
            ).orderBy(F.col("is_high_value_session").desc())
        
        # Funnel by device type
        if not is_column_all_null_or_zero(funnel, "device_type"):
            analysis["funnel_by_device"] = funnel.groupBy("device_type").agg(
                F. count("*").alias("total_sessions"),
                F.sum("reached_view").alias("sessions_with_views"),
                F.sum("reached_cart").alias("sessions_with_cart"),
                F.sum("reached_order").alias("sessions_with_orders"),
                F.avg("session_monetary_value").alias("avg_session_value"),
                F.sum("is_high_value_session").alias("high_value_sessions")
            ).withColumn(
                "conversion_rate",
                F.when(
                    F.col("total_sessions") > 0,
                    F.col("sessions_with_orders") / F.col("total_sessions")
                ).otherwise(0.0)
            ).orderBy(F.col("avg_session_value").desc_nulls_last())
        
        # Funnel by referrer source
        if not is_column_all_null_or_zero(funnel, "referrer_source"):
            analysis["funnel_by_referrer"] = funnel.groupBy("referrer_source").agg(
                F.count("*").alias("total_sessions"),
                F.sum("reached_view").alias("sessions_with_views"),
                F.sum("reached_cart").alias("sessions_with_cart"),
                F.sum("reached_order").alias("sessions_with_orders"),
                F.avg("session_monetary_value").alias("avg_session_value"),
                F.sum("is_high_value_session").alias("high_value_sessions")
            ).withColumn(
                "conversion_rate",
                F.when(
                    F.col("total_sessions") > 0,
                    F. col("sessions_with_orders") / F.col("total_sessions")
                ).otherwise(0.0)
            ).orderBy(F.col("avg_session_value").desc_nulls_last())
        
        # Abandoned vs Converted sessions comparison
        analysis["abandoned_vs_converted"] = funnel.groupBy("converted").agg(
            F.count("*").alias("session_count"),
            F.avg("total_products_viewed").alias("avg_products_viewed"),
            F.avg("items_added_to_cart").alias("avg_items_in_cart"),
            F.avg("session_monetary_value").alias("avg_cart_value"),
            F.sum("session_monetary_value").alias("total_cart_value")
        ).orderBy(F.col("converted").desc())

    else:
        print("agg_customer_sessions missing or session_id is all NULL/zero; skipping high-value funnel analysis.")





    # Checkout drop-off reasons (incomplete payment, missing info, etc.)
    # 1) Prefer agg_cart_abandonment_analysis from agg-only schema

    if (
        "agg_cart_abandonment_analysis" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_id")
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_status")
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_status_derived")
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_abandonment_reason")
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "session_converted")
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "abandonment_risk_score")
    ):

        carts = dataframes["agg_cart_abandonment_analysis"].fillna(
            {
                "cart_status": "unknown",
                "cart_status_derived": "unknown",
                "cart_abandonment_reason": "unknown",
                "session_converted": 0,
                "abandonment_risk_score": 0.0,
            }
        )

        # We treat "abandoned" or equivalent derived status as drop-off
        # Adjust if you use different labels in cart_status_derived
        dropped_carts = carts.filter(
            F.lower(F.coalesce(F.col("cart_status_derived"), F.col("cart_status"))).isin(
                "abandoned", "lost", "expired"
            )
        )

        # 1) Overall abandonment reasons
        product_analysis["checkout_dropoff_reasons"] = (
            dropped_carts.groupBy("cart_abandonment_reason")
            .agg(
                F.count("*").alias("dropoff_count"),
                F.avg("abandonment_risk_score").alias("avg_abandonment_risk_score"),
                F.avg("session_converted").alias("conversion_after_abandonment_rate"),
            )
            .orderBy(F.col("dropoff_count").desc_nulls_last())
        )

        # 2) Bucket reasons into higher-level categories: payment vs info vs other
        dropped_bucketed = dropped_carts.withColumn(
            "dropoff_bucket",
            F.when(
                F.lower(F.col("cart_abandonment_reason")).like("%payment%"),
                F.lit("payment_issues"),
            )
            .when(
                F.lower(F.col("cart_abandonment_reason")).like("%missing%")
                | F.lower(F.col("cart_abandonment_reason")).like("%info%")
                | F.lower(F.col("cart_abandonment_reason")).like("%address%"),
                F.lit("missing_or_incomplete_info"),
            )
            .when(
                F.lower(F.col("cart_abandonment_reason")).like("%technical%")
                | F.lower(F.col("cart_abandonment_reason")).like("%error%")
                | F.lower(F.col("cart_abandonment_reason")).like("%timeout%"),
                F.lit("technical_errors"),
            )
            .otherwise(F.lit("other_or_unknown")),
        )

        product_analysis["checkout_dropoff_buckets"] = (
            dropped_bucketed.groupBy("dropoff_bucket")
            .agg(
                F.count("*").alias("dropoff_count"),
                F.avg("abandonment_risk_score").alias("avg_abandonment_risk_score"),
            )
            .orderBy(F.col("dropoff_count").desc_nulls_last())
        )

        # 3) Optional enrichment with session/device if agg_customer_sessions exists
        if (
            "agg_customer_sessions" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "session_id")
            and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "device_type")
            and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "referrer_source")
            and not is_column_all_null_or_zero(dataframes["agg_customer_sessions"], "session_engagement_score")
        ):
            sessions = dataframes["agg_customer_sessions"].select(
                "session_id",
                "device_type",
                "referrer_source",
                "session_engagement_score",
            )

            carts_with_sessions = (
                dropped_bucketed.join(sessions, on="session_id", how="left")
            )

            # Drop-off by device and bucket
            product_analysis["checkout_dropoff_by_device_and_reason"] = (
                carts_with_sessions.groupBy("device_type", "dropoff_bucket")
                .agg(
                    F.count("*").alias("dropoff_count"),
                    F.avg("abandonment_risk_score").alias("avg_abandonment_risk_score"),
                    F.avg("session_engagement_score").alias("avg_session_engagement_score"),
                )
                .orderBy(
                    F.col("dropoff_count").desc_nulls_last(),
                    F.col("device_type").asc_nulls_last(),
                )
            )

    else:
        # 2) Fallback to global-schema checkout events if you later have one (e.g., agg_checkout_events)
        if (
            "agg_checkout_events" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_checkout_events"], "checkout_id")
            and not is_column_all_null_or_zero(dataframes["agg_checkout_events"], "checkout_status")
            and not is_column_all_null_or_zero(dataframes["agg_checkout_events"], "dropoff_reason")
            
        ):
            chk = dataframes["agg_checkout_events"].fillna(
                {
                    "checkout_status": "unknown",
                    "dropoff_reason": "unknown",
                }
            )

            # Non-completed = dropped
            dropped_chk = chk.filter(
                F.lower(F.col("checkout_status")).isin("abandoned", "failed", "incomplete")
            )

            product_analysis["checkout_dropoff_reasons"] = (
                dropped_chk.groupBy("dropoff_reason")
                .agg(F.count("*").alias("dropoff_count"))
                .orderBy(F.col("dropoff_count").desc_nulls_last())
            )

            dropped_bucketed = dropped_chk.withColumn(
                "dropoff_bucket",
                F.when(
                    F.lower(F.col("dropoff_reason")).like("%payment%"),
                    F.lit("payment_issues"),
                )
                .when(
                    F.lower(F.col("dropoff_reason")).like("%missing%")
                    | F.lower(F.col("dropoff_reason")).like("%info%")
                    | F.lower(F.col("dropoff_reason")).like("%address%"),
                    F.lit("missing_or_incomplete_info"),
                )
                .when(
                    F.lower(F.col("dropoff_reason")).like("%technical%")
                    | F.lower(F.col("dropoff_reason")).like("%error%")
                    | F.lower(F.col("dropoff_reason")).like("%timeout%"),
                    F.lit("technical_errors"),
                )
                .otherwise(F.lit("other_or_unknown")),
            )

            product_analysis["checkout_dropoff_buckets"] = (
                dropped_bucketed.groupBy("dropoff_bucket")
                .agg(F.count("*").alias("dropoff_count"))
                .orderBy(F.col("dropoff_count").desc_nulls_last())
            )

        else:
            print(
                "Neither agg_cart_abandonment_analysis (agg-only) nor a checkout events table "
                "with reasons is available; skipping checkout drop-off reasons analysis."
            )





    has_suppliers = (
        "agg_suppliers" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_suppliers"], "supplier_id")
    )

    has_sup_inv = (
        "agg_supplier_inventory_health" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_supplier_inventory_health"], "supplier_id")
    )

    if not has_suppliers and not has_sup_inv:
        print(
            "agg_suppliers and agg_supplier_inventory_health not available, or supplier_id all NULL/zero; "
            "skipping supplier & procurement insights."
        )
    else:
        # ------------------------------------------------------------------
        # 1) Build a non-ambiguous base supplier frame from agg_suppliers
        #    Use ONLY these names; do NOT bring in overlapping metrics from inv table.
        # ------------------------------------------------------------------
        if has_suppliers:
            s = dataframes["agg_suppliers"].select(
                "supplier_id",
                "supplier_status",
                "total_products_supplied",
                "total_units_sold",
                "total_orders_fulfilled",
                # keep this as the authoritative stockouts at supplier level
                "total_stockouts",
                "total_revenue_generated",
                "avg_profit_margin",
                "total_stock_value",
                "avg_stock_quantity",
                "avg_restock_lead_time",
                "supplier_performance_score",
                "stock_efficiency_ratio",
                "supplier_reliability_score",
                "stockout_rate",                     # supplier-level stockout rate
                "supplier_inventory_health_score",   # supplier-level health score
                "contract_start_date",
                "contract_end_date",
                "days_until_contract_expiry",
                "contract_status_flag",
                "avg_order_value",
            )
        else:
            # minimal shell when agg_suppliers missing
            s = dataframes["agg_supplier_inventory_health"].select("supplier_id").distinct()

        # ------------------------------------------------------------------
        # 2) Build inventory-health frame with RENAMED columns to avoid clashes
        #    We DO NOT bring in columns that already exist in 's' with same name.
        # ------------------------------------------------------------------
        if has_sup_inv:
            i = dataframes["agg_supplier_inventory_health"].select(
                "supplier_id",
                F.col("total_products").alias("inv_total_products"),
                F.col("total_current_stock").alias("inv_total_current_stock"),
                F.col("total_available_stock").alias("inv_total_available_stock"),
                # this "total_stockouts" would clash, so we rename it:
                F.col("total_stockouts").alias("inv_total_stockouts"),
                F.col("total_reorder_breaches").alias("inv_total_reorder_breaches"),
                F.col("total_storage_cost").alias("inv_total_storage_cost"),
                F.col("avg_stock_per_product").alias("inv_avg_stock_per_product"),
                F.col("stockout_rate").alias("inv_stockout_rate"),  # separate from supplier stockout_rate
                F.col("breach_rate").alias("inv_breach_rate"),
                F.col("avg_storage_cost_per_unit").alias("inv_avg_storage_cost_per_unit"),
                F.col("days_since_last_restock").alias("inv_days_since_last_restock"),
                F.col("supplier_inventory_health_score").alias("inv_health_score"),
            )

            joined = (
                s.alias("s")
                .join(i.alias("i"), on="supplier_id", how="left")
            )
        else:
            joined = s

        # ------------------------------------------------------------------
        # 3) Supplier reliability score (no ambiguities)
        #    Prefer: agg_suppliers.supplier_reliability_score,
        #    else use inv_health_score from inventory health.
        # ------------------------------------------------------------------
        joined = joined.withColumn(
            "supplier_reliability_score_effective",
            F.coalesce(
                F.col("supplier_reliability_score"),
                F.col("inv_health_score"),
            ),
        )

        supplier_analysis["supplier_reliability"] = (
            joined.select(
                "supplier_id",
                "supplier_status",
                "supplier_reliability_score_effective",
                "supplier_performance_score",
                "stock_efficiency_ratio",
            )
            .orderBy(F.col("supplier_reliability_score_effective").desc_nulls_last())
        )

        # ------------------------------------------------------------------
        # 4) Supplier stockout rate (no ambiguous total_stockouts)
        #    Use supplier-level metrics; keep inventory-level as separate columns.
        # ------------------------------------------------------------------
        supplier_analysis["supplier_stockouts"] = (
            joined.select(
                "supplier_id",
                F.coalesce(F.col("total_stockouts"), F.lit(0)).alias("total_stockouts"),
                F.coalesce(F.col("stockout_rate"), F.lit(0.0)).alias("supplier_stockout_rate"),
                # inventory-health side for context
                F.coalesce(F.col("inv_total_stockouts"), F.lit(0)).alias("inv_total_stockouts"),
                F.coalesce(F.col("inv_stockout_rate"), F.lit(0.0)).alias("inv_stockout_rate"),
                F.coalesce(F.col("inv_total_products"), F.lit(0)).alias("inv_total_products"),
                F.coalesce(F.col("inv_total_current_stock"), F.lit(0)).alias("inv_total_current_stock"),
                F.coalesce(F.col("inv_total_available_stock"), F.lit(0)).alias("inv_total_available_stock"),
            )
            .orderBy(
                F.col("supplier_stockout_rate").desc_nulls_last(),
                F.col("total_stockouts").desc_nulls_last(),
            )
        )

        # ------------------------------------------------------------------
        # 5) Supplier fulfillment performance
        # ------------------------------------------------------------------
        supplier_analysis["supplier_fulfillment_performance"] = (
            joined.select(
                "supplier_id",
                "supplier_status",
                F.coalesce(F.col("total_orders_fulfilled"), F.lit(0)).alias("total_orders_fulfilled"),
                F.coalesce(F.col("total_units_sold"), F.lit(0)).alias("total_units_sold"),
                F.coalesce(F.col("avg_restock_lead_time"), F.lit(0.0)).alias("avg_restock_lead_time"),
                F.coalesce(F.col("supplier_performance_score"), F.lit(0.0)).alias("supplier_performance_score"),
                F.coalesce(F.col("stock_efficiency_ratio"), F.lit(0.0)).alias("stock_efficiency_ratio"),
                F.col("supplier_reliability_score_effective"),
            )
            .orderBy(
                F.col("supplier_performance_score").desc_nulls_last(),
                F.col("supplier_reliability_score_effective").desc_nulls_last(),
            )
        )

        # ------------------------------------------------------------------
        # 6) Supplier revenue contribution
        # ------------------------------------------------------------------
        if "total_revenue_generated" in joined.columns:
            total_rev_all = joined.agg(F.sum("total_revenue_generated")).collect()[0][0] or 0.0

            supplier_revenue = joined.withColumn(
                "revenue_contribution_share",
                F.when(
                    F.lit(total_rev_all) > 0,
                    F.col("total_revenue_generated") / F.lit(total_rev_all),
                ).otherwise(F.lit(0.0)),
            )

            supplier_analysis["supplier_revenue_contribution"] = (
                supplier_revenue.select(
                    "supplier_id",
                    "total_revenue_generated",
                    F.coalesce(F.col("total_orders_fulfilled"), F.lit(0)).alias("total_orders_fulfilled"),
                    "avg_order_value",
                    "revenue_contribution_share",
                )
                .orderBy(F.col("total_revenue_generated").desc_nulls_last())
            )

        # ------------------------------------------------------------------
        # 7) Supplier’s avg profit margin
        # ------------------------------------------------------------------
        if "avg_profit_margin" in joined.columns:
            supplier_analysis["supplier_profit_margin"] = (
                joined.select(
                    "supplier_id",
                    "avg_profit_margin",
                    F.coalesce(F.col("total_revenue_generated"), F.lit(0.0)).alias("total_revenue_generated"),
                    F.coalesce(F.col("total_orders_fulfilled"), F.lit(0)).alias("total_orders_fulfilled"),
                )
                .orderBy(
                    F.col("avg_profit_margin").desc_nulls_last(),
                    F.col("total_revenue_generated").desc_nulls_last(),
                )
            )

        # ------------------------------------------------------------------
        # 8) Days since last restock (supplier-level, from inventory health)
        # ------------------------------------------------------------------
        if "inv_days_since_last_restock" in joined.columns:
            supplier_analysis["supplier_days_since_last_restock"] = (
                joined.select(
                    "supplier_id",
                    F.col("inv_days_since_last_restock").alias("days_since_last_restock"),
                    F.coalesce(F.col("inv_total_products"), F.lit(0)).alias("inv_total_products"),
                    F.coalesce(F.col("inv_total_current_stock"), F.lit(0)).alias("inv_total_current_stock"),
                    F.coalesce(F.col("inv_total_available_stock"), F.lit(0)).alias("inv_total_available_stock"),
                )
                .orderBy(F.col("days_since_last_restock").desc_nulls_last())
            )

        # ------------------------------------------------------------------
        # 9) Supplier contract expiry prediction
        # ------------------------------------------------------------------
        if "contract_end_date" in joined.columns or "days_until_contract_expiry" in joined.columns:
            supplier_analysis["supplier_contract_expiry"] = (
                joined.select(
                    "supplier_id",
                    "supplier_status",
                    "contract_start_date",
                    "contract_end_date",
                    "days_until_contract_expiry",
                    "contract_status_flag",
                    "supplier_reliability_score_effective",
                    F.coalesce(F.col("supplier_performance_score"), F.lit(0.0)).alias("supplier_performance_score"),
                )
                .orderBy(
                    F.col("days_until_contract_expiry").asc_nulls_last(),
                    F.col("supplier_reliability_score_effective").desc_nulls_last(),
                )
            )




    # Layer 1: Table Check
    if "agg_wishlist" in dataframes:
        # Layer 2: Column Check
        if (
            not is_column_all_null_or_zero(dataframes["agg_wishlist"], "product_id")
            and not is_column_all_null_or_zero(dataframes["agg_wishlist"], "purchased_date")
            and not is_column_all_null_or_zero(dataframes["agg_wishlist"], "customer_id")
        ):
            analysis["wishlist_overall_summary"] = (
                dataframes["agg_wishlist"]
                .agg(
                    F.count("*").alias("total_wishlist_items"),
                    F.countDistinct("customer_id").alias("customers_using_wishlist"),
                    F.countDistinct("product_id").alias("products_in_wishlist"),
                    F.sum(F.when(F.col("purchased_date").isNotNull(), 1).otherwise(0)).alias("wishlist_purchased_items")
                )
                .withColumn(
                    "wishlist_conversion_rate",
                    F.when(
                        F.col("total_wishlist_items") > 0,
                        F.col("wishlist_purchased_items") / F.col("total_wishlist_items")
                    ).otherwise(0.0)
                )
                .fillna({
                    "total_wishlist_items": 0,
                    "customers_using_wishlist": 0,
                    "products_in_wishlist": 0,
                    "wishlist_purchased_items": 0,
                    "wishlist_conversion_rate": 0.0
                })
            )

            analysis["wishlist_by_product"] = (
                dataframes["agg_wishlist"]  
                .groupBy("product_id")
                .agg(
                    F.count("*").alias("wishlist_adds"),
                    F.sum(F.when(F.col("purchased_date").isNotNull(), 1).otherwise(0)).alias("wishlist_purchases")
                )
                .withColumn(
                    "wishlist_conversion_rate",
                    F.col("wishlist_purchases") / F.col("wishlist_adds")
                )
            )

            analysis["wishlist_by_customer"] = (
                dataframes["agg_wishlist"] 
                .groupBy("customer_id")
                .agg(
                    F.count("*").alias("wishlist_adds"),
                    F.sum(F.when(F.col("purchased_date").isNotNull(), 1).otherwise(0)).alias("wishlist_purchases")
                )
                .withColumn(
                    "wishlist_conversion_rate",
                    F.when(F.col("wishlist_adds") > 0,
                        F.col("wishlist_purchases") / F.col("wishlist_adds"))
                    .otherwise(F.lit(0.0))
                )
            )    

            wl = dataframes["agg_wishlist"].select(
                "wishlist_id",
                "customer_id",
                "product_id",
                "wishlist_to_purchase_time"
            ).filter(F.col("wishlist_to_purchase_time").isNotNull())

            # 1) Overall stats
            analysis["wishlist_time_to_purchase_stats"] = wl.agg(
                F.count("*").alias("records"),
                F.avg("wishlist_to_purchase_time").alias("avg_time"),
                F.expr("percentile_approx(wishlist_to_purchase_time, 0.5)").alias("median_time"),
                F.expr("percentile_approx(wishlist_to_purchase_time, 0.9)").alias("p90_time"),
                F.max("wishlist_to_purchase_time").alias("max_time"),
            )

            # 2) Buckets (adjust edges to your unit: days/hours)
            wl_buckets = wl.withColumn(
                "time_bucket",
                F.when(F.col("wishlist_to_purchase_time") <= 1, "≤ 1")
                .when((F.col("wishlist_to_purchase_time") > 1) & (F.col("wishlist_to_purchase_time") <= 7), "1–7")
                .when((F.col("wishlist_to_purchase_time") > 7) & (F.col("wishlist_to_purchase_time") <= 30), "7–30")
                .otherwise("> 30")
            )

            # 3) Counts per bucket
            bucket_counts = (
                wl_buckets.groupBy("time_bucket")
                .agg(F.count("*").alias("count"))
            )

            # 4) Total count (for share calculation)
            total_count = bucket_counts.agg(F.sum("count").alias("total_count"))

            # 5) Cross join to get total_count on every row, then compute share
            analysis["wishlist_time_to_purchase_distribution"] = (
                bucket_counts.crossJoin(total_count)
                .withColumn("share", F.col("count") / F.col("total_count"))
                .select("time_bucket", "count", "share")
                .orderBy("time_bucket")
            )

            if  not is_column_all_null_or_zero(dataframes["agg_wishlist"], "removed_date") and \
                not is_column_all_null_or_zero(dataframes["agg_wishlist"], "purchased_date") and \
                not is_column_all_null_or_zero(dataframes["agg_wishlist"], "added_date"):
                    
                wl2 = dataframes["agg_wishlist"].select(
                    "wishlist_id",
                    "customer_id",
                    "product_id",
                    "added_date",
                    "purchased_date",
                    "removed_date"
                )

                # Abandoned = no purchased_date and no removed_date
                abandoned = wl2.filter(
                    F.col("purchased_date").isNull() & F.col("removed_date").isNull()
                )

                analysis["abandoned_wishlist_items"] = abandoned

                analysis["abandoned_wishlist_by_customer"] = (
                    abandoned.groupBy("customer_id")
                    .agg(F.count("*").alias("abandoned_wishlist_count"))
                    .orderBy(F.col("abandoned_wishlist_count").desc())
                )

                # By product
                analysis["abandoned_wishlist_by_product"] = (
                    abandoned.groupBy("product_id")
                    .agg(F.count("*").alias("abandoned_wishlist_count"))
                    .orderBy(F.col("abandoned_wishlist_count").desc())
                )

                wl3 = dataframes["agg_wishlist"].select(
                    "wishlist_id",
                    "customer_id",
                    "product_id",
                    "added_date"
                ).filter(F.col("added_date").isNotNull())

                # Adds by year-month
                wl_monthly = wl3.withColumn("year_month", F.date_format("added_date", "yyyy-MM"))

                analysis["wishlist_adds_by_month"] = (
                    wl_monthly.groupBy("year_month")
                    .agg(
                        F.count("*").alias("wishlist_adds")
                    )
                    .orderBy("year_month")
                )

            else:
                print("removed_date, purchased_date, or added_date column missing/all NULL/zero; skipping abandoned wishlist analysis.")

        else:
            print("One of the required columns in agg_wishlist is all NULL or zero; skipping wishlist overall analysis.")
    else:
        print("agg_wishlist table not found in dataframes; skipping wishlist analysis.")




    # -------------------------------------------------------
    # Shopping Cart Statistics & Distribution
    # -------------------------------------------------------

    # Layer 1: Table Existence Check
    if "agg_shopping_cart" in dataframes:
        

        if (
            not is_column_all_null_or_zero(dataframes["agg_shopping_cart"], "cart_id") 
            and not is_column_all_null_or_zero(dataframes["agg_shopping_cart"], "cart_status")
        ):
            analysis["cart_overall_stats"] = (
                dataframes["agg_shopping_cart"]
                .agg(
                    F.countDistinct("cart_id").alias("total_carts"),
                    F.count("*").alias("total_cart_lines")
                )
                .fillna({
                    "total_carts": 0,
                    "total_cart_lines": 0
                })
            )
        else:
            print("cart_id or cart_status column is all NULL or zero; skipping overall cart statistics analysis.")

    
        if not is_column_all_null_or_zero(dataframes["agg_shopping_cart"], "cart_status"):
            analysis["cart_status_distribution"] = (
                dataframes["agg_shopping_cart"]
                .groupBy("cart_status")
                .agg(
                    F.countDistinct("cart_id").alias("carts_count"),
                    F.count("*").alias("cart_lines_count")
                )
                .fillna({
                    "carts_count": 0,
                    "cart_lines_count": 0
                })
                .orderBy("cart_status")
            )
        else:
            print("cart_status column is all NULL or zero; skipping cart status distribution analysis.")

    else:
        print("agg_shopping_cart table not found in dataframes; skipping cart analysis.")



    if "agg_cart_abandonment_analysis" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_id") and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_status"):
            analysis["cart_abandon_summary"] = dataframes["agg_cart_abandonment_analysis"].agg(
                F.countDistinct("cart_id").alias("total_carts_tracked"),
                F.countDistinct(F.when(F.col("cart_status") == "Abandoned", F.col("cart_id"))).alias("abandoned_carts"),
                F.countDistinct(F.when(F.col("cart_status") == "Converted", F.col("cart_id"))).alias("converted_carts")
            ).withColumn(
                "abandonment_rate",
                F.col("abandoned_carts") / F.col("total_carts_tracked")
            ).withColumn(
                "purchase_rate",
                F.col("converted_carts") / F.col("total_carts_tracked")
            ).fillna({
                "total_carts_tracked": 0,
                "abandoned_carts": 0,
                "converted_carts": 0,
                "abandonment_rate": 0.0,
                "purchase_rate": 0.0
            })
        else:
            print("cart_id or cart_status column is all NULL or zero; skipping cart abandonment overall analysis.")
    else:
        print("agg_cart_abandonment_analysis table not found in dataframes; skipping cart abandonment analysis.")



    if "agg_cart_abandonment_analysis" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_status"):
            analysis["cart_value_stats"] = (
                dataframes["agg_cart_abandonment_analysis"]
                .groupBy("cart_status")
                .agg(
                    F.countDistinct("cart_id").alias("carts_count"),
                    F.avg("cart_total_value").alias("avg_cart_value"),
                F.avg("cart_items_count").alias("avg_cart_items"),
                F.avg("time_in_cart_days").alias("avg_time_in_cart_days"),
                F.avg("recovery_potential_score").alias("avg_recovery_potential_score")
            ).fillna({
                "carts_count": 0,
                "avg_cart_value": 0.0,
                "avg_cart_items": 0.0,
                "avg_time_in_cart_days": 0.0,
                "avg_recovery_potential_score": 0.0
            })
            .orderBy("cart_status")
        )
        else:
            print("cart_status column is all NULL or zero; skipping cart value statistics analysis.")
    else:
        print("agg_cart_abandonment_analysis table not found in dataframes; skipping cart value statistics analysis.")



    if "agg_cart_abandonment_analysis" in dataframes:
        if not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_status") and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_total_value"):
            avg_cart_value = dataframes["agg_cart_abandonment_analysis"].agg(F.avg("cart_total_value")).collect()[0][0]
            analysis["high_value_abandoned_carts"] = (
                dataframes["agg_cart_abandonment_analysis"]
                .filter(
                    (F.col("cart_status").rlike("^(?i)(abandoned)")) &
                    (F.col("cart_total_value") >= avg_cart_value))
                ).select(
                    "cart_id",
                    "customer_id",
                    "cart_total_value",
                    "cart_items_count",
                    "time_in_cart_days",
                    "recovery_potential_score",
                    "abandonment_risk_score"
                )
            
            
        else:
            print("cart_status or cart_total_value column is all NULL or zero; skipping high-value abandoned carts analysis.")
    else:
        print("agg_cart_abandonment_analysis table not found in dataframes; skipping high-value abandoned carts analysis.")





    # Average Time-to-Purchase: time_in_cart_days/hours for completed carts
    if (
        "agg_cart_abandonment_analysis" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_id")
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "session_converted")
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "time_in_cart_days")
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "time_in_cart_hours")
    ):
        carts = dataframes["agg_cart_abandonment_analysis"].fillna(
            {
                "session_converted": 0,
                "time_in_cart_days": 0,
                "time_in_cart_hours": 0,
                "cart_items_count": 0,
            }
        )

        # Define "completed / purchased" carts:
        # 1) session_converted = 1
        # 2) OR cart_status_derived looks like a purchased/completed status
        completed_carts = carts.filter(
            (F.col("session_converted") == 1)
            | (
                F.lower(F.col("cart_status_derived")).isin(
                    "purchased", "completed", "converted"
                )
            )
        )

        # 1) Overall average time-to-purchase (days & hours)
        analysis["time_to_purchase_overall"] = (
            completed_carts.agg(
                F.countDistinct("cart_id").alias("completed_carts"),
                F.avg("time_in_cart_days").alias("avg_time_in_cart_days"),
                F.avg("time_in_cart_hours").alias("avg_time_in_cart_hours"),
                F.expr("percentile(time_in_cart_days, array(0.25, 0.5, 0.75))").alias(
                    "time_in_cart_days_percentiles"
                ),
            )
        )

        # 2) Time-to-purchase by cart value tier (e.g., low/medium/high) if populated
        group_cols = []
        if "cart_value_tier" in carts.columns:
            group_cols.append("cart_value_tier")
        if "cart_size_category" in carts.columns:
            group_cols.append("cart_size_category")

        if group_cols:
            analysis["time_to_purchase_by_tier"] = (
                completed_carts.groupBy(*group_cols)
                .agg(
                    F.countDistinct("cart_id").alias("completed_carts"),
                    F.avg("time_in_cart_days").alias("avg_time_in_cart_days"),
                    F.avg("time_in_cart_hours").alias("avg_time_in_cart_hours"),
                    F.avg("cart_items_count").alias("avg_cart_items_count"),
                )
                .orderBy(F.col("avg_time_in_cart_days").desc_nulls_last())
            )

        # 3) Optional: time-to-purchase distribution buckets (e.g., same day, 1–3 days, >3 days)
        buckets = (
            completed_carts.withColumn(
                "time_to_purchase_bucket",
                F.when(F.col("time_in_cart_days") <= 0, "same_day_or_less")
                .when(F.col("time_in_cart_days") <= 1, "1_day")
                .when(F.col("time_in_cart_days") <= 3, "2-3_days")
                .otherwise(">3_days"),
            )
            .groupBy("time_to_purchase_bucket")
            .agg(
                F.countDistinct("cart_id").alias("completed_carts"),
                F.avg("time_in_cart_days").alias("avg_time_in_cart_days"),
            )
            .orderBy("time_to_purchase_bucket")
        )

        analysis["time_to_purchase_buckets"] = buckets

    else:
        print(
            "agg_cart_abandonment_analysis not available, or cart_id all NULL/zero; "
            "skipping average time-to-purchase analysis."
        )




    if (
        "agg_marketing_campaigns" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "campaign_id")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "impressions")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "clicks")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "conversions")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "spent_amount")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "revenue_generated")
        and not is_column_all_null_or_zero(dataframes["agg_marketing_campaigns"], "budget")
    ):
        cm = dataframes["agg_marketing_campaigns"].fillna(
            {
                "impressions": 0,
                "clicks": 0,
                "conversions": 0,
                "total_impressions": 0,
                "total_clicks": 0,
                "total_conversions": 0,
                "revenue_generated": 0.0,
                "spent_amount": 0.0,
                "budget": 0.0,
            }
        )

        # Prefer existing derived fields when present
        cm_perf = (
            cm
            # Base funnel metrics
            .withColumn(
                "effective_impressions",
                F.when(F.col("total_impressions") > 0, F.col("total_impressions")).otherwise(F.col("impressions"))
            )
            .withColumn(
                "effective_clicks",
                F.when(F.col("total_clicks") > 0, F.col("total_clicks")).otherwise(F.col("clicks"))
            )
            .withColumn(
                "effective_conversions",
                F.when(F.col("total_conversions") > 0, F.col("total_conversions")).otherwise(F.col("conversions"))
            )
            # CTR: use existing ctr if not null, else compute
            .withColumn(
                "ctr_effective",
                F.when(
                    F.col("ctr").isNotNull(),
                    F.col("ctr")
                ).otherwise(
                    F.when(
                        F.col("effective_impressions") > 0,
                        F.col("effective_clicks") / F.col("effective_impressions")
                    ).otherwise(F.lit(0.0))
                )
            )
            # Conversion rate: use existing conversion_rate if present
            .withColumn(
                "conversion_rate_effective",
                F.when(
                    F.col("conversion_rate").isNotNull(),
                    F.col("conversion_rate")
                ).otherwise(
                    F.when(
                        F.col("effective_clicks") > 0,
                        F.col("effective_conversions") / F.col("effective_clicks")
                    ).otherwise(F.lit(0.0))
                )
            )
            # ROAS: use existing roas if present, else compute revenue_generated / spent_amount
            .withColumn(
                "roas_effective",
                F.when(
                    F.col("roas").isNotNull(),
                    F.col("roas")
                ).otherwise(
                    F.when(
                        F.col("spent_amount") > 0,
                        F.col("revenue_generated") / F.col("spent_amount")
                    ).otherwise(F.lit(None))
                )
            )
            # ROI: use existing roi if present, else (revenue - spend) / spend
            .withColumn(
                "roi_effective",
                F.when(
                    F.col("roi").isNotNull(),
                    F.col("roi")
                ).otherwise(
                    F.when(
                        F.col("spent_amount") > 0,
                        (F.col("revenue_generated") - F.col("spent_amount")) / F.col("spent_amount")
                    ).otherwise(F.lit(None))
                )
            )
        )

        # Store concise performance view
        analysis["campaign_performance"] = (
            cm_perf.select(
                "campaign_id",
                "campaign_name",
                "campaign_type",
                "campaign_status",
                "performance_tier",
                "budget",
                "spent_amount",
                "effective_impressions",
                "effective_clicks",
                "effective_conversions",
                "revenue_generated",
                "avg_order_value",
                "ctr_effective",
                "conversion_rate_effective",
                "roas_effective",
                "roi_effective",
            )
            .orderBy(
                F.col("revenue_generated").desc_nulls_last(),
                F.col("roas_effective").desc_nulls_last(),
            )
        )

    else:
        print(
            "agg_marketing_campaigns not available, or campaign_id column is all NULL/zero; "
            "skipping campaign performance analysis."
        )



    # Device-Based Conversion Rates: Mobile vs Desktop from device_used
    if (
        "agg_cart_abandonment_analysis" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_id")
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "device_used")
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "session_converted")
        and not is_column_all_null_or_zero(dataframes["agg_cart_abandonment_analysis"], "cart_status_derived")
    ):
        carts = dataframes["agg_cart_abandonment_analysis"].fillna(
            {
                "device_used": "unknown",
                "session_converted": 0,
                "cart_status_derived": "unknown",
            }
        )

        # Normalize a simple converted flag:
        # - primary: session_converted
        # - backup: status-derived indicates purchased/completed
        carts = carts.withColumn(
            "converted_flag",
            F.when(F.col("session_converted") == 1, 1)
            .when(
                F.lower(F.col("cart_status_derived")).isin("purchased", "completed", "converted"),
                1,
            )
            .otherwise(0),
        )

        # Abandoned flag from status
        carts = carts.withColumn(
            "abandoned_flag",
            F.when(
                F.lower(F.col("cart_status_derived")).isin("abandoned", "lost", "expired"),
                1,
            ).otherwise(0),
        )

        # Aggregate by device_used
        device_stats = (
            carts.groupBy("device_used")
            .agg(
                F.countDistinct("cart_id").alias("carts"),
                F.sum("converted_flag").alias("converted_carts"),
                F.sum("abandoned_flag").alias("abandoned_carts"),
            )
            .withColumn(
                "conversion_rate",
                F.when(F.col("carts") > 0, F.col("converted_carts") / F.col("carts")).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "abandonment_rate",
                F.when(F.col("carts") > 0, F.col("abandoned_carts") / F.col("carts")).otherwise(F.lit(0.0)),
            )
            .orderBy(F.col("conversion_rate").desc_nulls_last())
        )

        analysis["device_conversion_rates"] = device_stats

    else:
        print(
            "agg_cart_abandonment_analysis not available, or cart_id/device_used/session_converted/cart_status_derived columns all NULL/zero; "
            "skipping device-based conversion rates analysis."
        )



    has_payments = "agg_payments" in dataframes and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_id") and not is_column_all_null_or_zero(dataframes["agg_payments"], "order_id") and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_method")

    has_customers = "agg_customers" in dataframes and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and not is_column_all_null_or_zero(dataframes["agg_customers"], "country") and not is_column_all_null_or_zero(dataframes["agg_customers"], "state_province")
    has_orders = "agg_orders" in dataframes and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id") and not is_column_all_null_or_zero(dataframes["agg_orders"], "customer_id")

    if not (has_payments and has_customers and has_orders):
        print(
            "agg_payments, agg_orders, or agg_customers missing/invalid; "
            "cannot compute payment counts by country & method."
        )
    else:
        payments = dataframes["agg_payments"].select(
            "payment_id",
            "order_id",
            "payment_method",
            "payment_status",
        )

        orders = dataframes["agg_orders"].select(
            "order_id",
            "customer_id",
        )

        customers = dataframes["agg_customers"].select(
            "customer_id",
            "country",
            "state_province",
        )

        pay_geo = (
            payments.alias("p")
            .join(orders.alias("o"), on="order_id", how="left")
            .join(customers.alias("c"), on="customer_id", how="left")
            .fillna(
                {
                    "payment_method": "unknown",
                    "country": "unknown",
                    "state_province": "unknown",
                }
            )
        )

        # Optional: keep only non-failed payments
        pay_geo = pay_geo.filter(
            ~F.lower(F.col("payment_status")).isin("failed", "declined", "cancelled","incomplete","unknown")
        )

        # 1) By country & payment_method
        analysis["payment_counts_by_country_method"] = (
            pay_geo.groupBy("country", "payment_method")
            .agg(
                F.countDistinct("payment_id").alias("payment_count"),
                F.countDistinct("order_id").alias("order_count"),
            )
            .orderBy("country", F.col("payment_count").desc())
        )

        # 2) By state & payment_method (optional)
        analysis["payment_counts_by_state_method"] = (
            pay_geo.groupBy("country", "state_province", "payment_method")
            .agg(
                F.countDistinct("payment_id").alias("payment_count"),
                F.countDistinct("order_id").alias("order_count"),
            )
            .orderBy("country", "state_province", F.col("payment_count").desc())
        )



    has_payments = "agg_payments" in dataframes and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_id") and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_method") and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_status")


    # -------------------------------------------------------------------
    # 1) Overall success rate by payment_method
    # -------------------------------------------------------------------
    if has_payments:
        payments = dataframes["agg_payments"].select(
            "payment_id",
            "order_id",
            "payment_method",
            "payment_status",
        ).fillna(
            {
                "payment_method": "unknown",
                "payment_status": "unknown",
            }
        )

        # Flag completed payments
        
        payments = payments.withColumn(
            "is_completed",
            F.when(
                F.lower(F.col("payment_status")).isin("completed", "successful","success","done","transfered","was_successful"),
                1,
            ).otherwise(0),
        )

        analysis["payment_method_success_rates"] = (
            payments.groupBy("payment_method")
            .agg(
                F.countDistinct("payment_id").alias("total_payments"),
                F.sum("is_completed").alias("completed_payments"),
                F.countDistinct("order_id").alias("distinct_orders"),
            )
            .withColumn(
                "success_rate",
                F.when(
                    F.col("total_payments") > 0,
                    F.col("completed_payments") / F.col("total_payments"),
                ).otherwise(F.lit(0.0)),
            )
            .orderBy(F.col("success_rate").desc_nulls_last())
        )
    else:
        print(
            "agg_payments not available, or payment_id/payment_method/payment_status columns all NULL/zero; "
            "skipping payment method success rates."
        )

    # -------------------------------------------------------------------
    # 2) OPTIONAL: Success rate by payment_method + country
    # -------------------------------------------------------------------
    has_customers = "agg_customers" in dataframes and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id") and not is_column_all_null_or_zero(dataframes["agg_customers"], "country")
    has_orders = "agg_orders" in dataframes and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id") and not is_column_all_null_or_zero(dataframes["agg_orders"], "customer_id")

    if has_payments and has_customers and has_orders:
        orders = dataframes["agg_orders"].select("order_id", "customer_id")
        customers = dataframes["agg_customers"].select("customer_id", "country")

        pay_geo = (
            payments.alias("p")          # reuse 'payments' with is_completed flag
            .join(orders.alias("o"), on="order_id", how="left")
            .join(customers.alias("c"), on="customer_id", how="left")
            .fillna({"country": "unknown"})
        )

        analysis["payment_method_success_rates_by_country"] = (
            pay_geo.groupBy("country", "payment_method")
            .agg(
                F.countDistinct("payment_id").alias("total_payments"),
                F.sum("is_completed").alias("completed_payments"),
                F.countDistinct("order_id").alias("distinct_orders"),
            )
            .withColumn(
                "success_rate",
                F.when(
                    F.col("total_payments") > 0,
                    F.col("completed_payments") / F.col("total_payments"),
                ).otherwise(F.lit(0.0)),
            )
            .orderBy("country", F.col("success_rate").desc_nulls_last())
        )




    has_payments = "agg_payments" in dataframes and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_id") and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_method") and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_status")
    has_orders = "agg_orders" in dataframes and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id") and not is_column_all_null_or_zero(dataframes["agg_orders"], "total_amount")

    if not (has_payments and has_orders):
        print(
            "agg_payments or agg_orders missing/invalid; "
            "cannot compute payment method AOV correlation."
        )
    else:
        payments = dataframes["agg_payments"].select(
            "payment_id",
            "order_id",
            "payment_method",
            "payment_status",
        ).fillna(
            {
                "payment_method": "unknown",
                "payment_status": "unknown",
            }
        )

        orders = dataframes["agg_orders"].select(
            "order_id",
            "total_amount",
        ).fillna({"total_amount": 0.0})

        # Join payments to orders to get order value per payment
        pay_orders = (
            payments.alias("p")
            .join(orders.alias("o"), on="order_id", how="left")
        )

        # Optional: focus only on completed payments
        pay_orders = pay_orders.filter(F.lower(F.col("payment_status")) == "completed")

        analysis["payment_method_aov"] = (
            pay_orders.groupBy("payment_method")
            .agg(
                F.countDistinct("payment_id").alias("payment_count"),
                F.countDistinct("order_id").alias("order_count"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("avg_order_value_method"),
            )
            .orderBy(F.col("avg_order_value_method").desc_nulls_last())
        )




    has_payments = "agg_payments" in dataframes and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_id") and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_method") and not is_column_all_null_or_zero(dataframes["agg_payments"], "refund_amount") and not is_column_all_null_or_zero(dataframes["agg_payments"], "processing_fee")

    if not has_payments:
        print(
            "agg_payments not available, or payment_id all NULL/zero; "
            "skipping refund rate by payment method."
        )
    else:
        payments = dataframes["agg_payments"].select(
            "payment_id",
            "order_id",
            "payment_method",
            "payment_status",
            "processing_fee",
            "refund_amount",
        ).fillna(
            {
                "payment_method": "unknown",
                "payment_status": "unknown",
                "processing_fee": 0.0,
                "refund_amount": 0.0,
            }
        )

        # Flag payments that had any refund
        payments = payments.withColumn(
            "has_refund",
            F.when(F.col("refund_amount") > 0, 1).otherwise(0),
        )

        # Aggregate by payment_method
        analysis["refund_rate_by_payment_method"] = (
            payments.groupBy("payment_method")
            .agg(
                F.countDistinct("payment_id").alias("total_payments"),
                F.sum("refund_amount").alias("total_refund_amount"),
                F.sum("has_refund").alias("payments_with_refund"),
            )
            .withColumn(
                # 1) Share of payments that had a refund (count-based rate)
                "refund_rate_payments",
                F.when(
                    F.col("total_payments") > 0,
                    F.col("payments_with_refund") / F.col("total_payments"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                # 2) Avg refund amount per payment (including 0s)
                "avg_refund_per_payment",
                F.when(
                    F.col("total_payments") > 0,
                    F.col("total_refund_amount") / F.col("total_payments"),
                ).otherwise(F.lit(0.0)),
            )
            .orderBy(F.col("refund_rate_payments").desc_nulls_last())
        )



    has_payments = "agg_payments" in dataframes and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_id") and not is_column_all_null_or_zero(dataframes["agg_payments"], "refund_amount")
    has_order_items = "agg_order_items" in dataframes and not is_column_all_null_or_zero(dataframes["agg_order_items"], "order_item_id") and not is_column_all_null_or_zero(dataframes["agg_order_items"], "order_id") and not is_column_all_null_or_zero(dataframes["agg_order_items"], "product_id") and not is_column_all_null_or_zero(dataframes["agg_order_items"], "quantity")
    has_products = "agg_products" in dataframes and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")


    if not (has_payments and has_order_items):
        print(
            "agg_payments or agg_order_items missing/invalid; "
            "cannot compute refund rate by product."
        )
    else:
        # Payments: which orders had refunds?
        payments = dataframes["agg_payments"].select(
            "payment_id",
            "order_id",
            "refund_amount",
        ).fillna({"refund_amount": 0.0})

        # Flag refunded orders
        refunded_orders = (
            payments.withColumn(
                "has_refund",
                F.when(F.col("refund_amount") > 0, 1).otherwise(0),
            )
            .groupBy("order_id")
            .agg(
                F.max("has_refund").alias("order_has_refund"),
                F.sum("refund_amount").alias("order_total_refund_amount"),
            )
        )

        # Order items: link orders to products
        order_items = dataframes["agg_order_items"].select(
            "order_id",
            "order_item_id",
            "product_id",
            "quantity",
        )

        # Attach refund flags to each product in the order
        prod_refunds = (
            order_items.alias("oi")
            .join(refunded_orders.alias("r"), on="order_id", how="left")
            .fillna({"order_has_refund": 0, "order_total_refund_amount": 0.0})
        )

        # Optional: bring in product names
        if has_products:
            prod_meta = dataframes["agg_products"].select(
                "product_id",
                "product_name",
                "category",
            )
            prod_refunds = prod_refunds.join(prod_meta, on="product_id", how="left")

        # Aggregate by product
        analysis["refund_rate_by_product"] = (
            prod_refunds.groupBy("product_id", *[c for c in ["product_name", "category"] if c in prod_refunds.columns])
            .agg(
                F.countDistinct("order_id").alias("orders_for_product"),
                F.sum("order_has_refund").alias("orders_with_refund"),
                F.sum("order_total_refund_amount").alias("total_refund_amount"),
            )
            .withColumn(
                "refund_rate_orders",
                F.when(
                    F.col("orders_for_product") > 0,
                    F.col("orders_with_refund") / F.col("orders_for_product"),
                ).otherwise(F.lit(0.0)),
            )
            .orderBy(F.col("refund_rate_orders").desc_nulls_last())
        )




    has_orders = "agg_orders" in dataframes and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id") and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_placed_year") and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_placed_month")

    if not (has_payments and has_orders):
        print(
            "agg_payments or agg_orders missing/invalid; "
            "cannot compute refund rate by month."
        )
    else:
        payments = dataframes["agg_payments"].select(
            "payment_id",
            "order_id",
            "refund_amount",
        ).fillna({"refund_amount": 0.0})

        # Order-level refund flags
        refunded_orders = (
            payments.withColumn(
                "has_refund",
                F.when(F.col("refund_amount") > 0, 1).otherwise(0),
            )
            .groupBy("order_id")
            .agg(
                F.max("has_refund").alias("order_has_refund"),
                F.sum("refund_amount").alias("order_total_refund_amount"),
            )
        )

        orders = dataframes["agg_orders"].select(
            "order_id",
            "order_placed_year",
            "order_placed_month",
            "total_amount",
        ).fillna({"total_amount": 0.0})

        # Join refund info to orders with date parts
        orders_with_refund = (
            orders.alias("o")
            .join(refunded_orders.alias("r"), on="order_id", how="left")
            .fillna({"order_has_refund": 0, "order_total_refund_amount": 0.0})
        )

        # Aggregate by year-month
        analysis["refund_rate_by_month"] = (
            orders_with_refund.groupBy("order_placed_year", "order_placed_month")
            .agg(
                F.countDistinct("order_id").alias("orders"),
                F.sum("order_has_refund").alias("orders_with_refund"),
                F.sum("order_total_refund_amount").alias("total_refund_amount"),
                F.sum("total_amount").alias("total_order_amount"),
            )
            .withColumn(
                "refund_rate_orders",
                F.when(
                    F.col("orders") > 0,
                    F.col("orders_with_refund") / F.col("orders"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "refund_rate_amount",
                F.when(
                    F.col("total_order_amount") > 0,
                    F.col("total_refund_amount") / F.col("total_order_amount"),
                ).otherwise(F.lit(0.0)),
            )
            .orderBy("order_placed_year", "order_placed_month")
        )




    has_payments = "agg_payments" in dataframes and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_id") and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_method") and not is_column_all_null_or_zero(dataframes["agg_payments"], "payment_date") and not is_column_all_null_or_zero(dataframes["agg_payments"], "refund_date") and not is_column_all_null_or_zero(dataframes["agg_payments"], "refund_amount")

    if not has_payments:
        print(
            "agg_payments not available, or payment_id all NULL/zero; "
            "skipping time to refund analysis."
        )
    else:
        payments = dataframes["agg_payments"].select(
            "payment_id",
            "order_id",
            "payment_method",
            "payment_status",
            "payment_date",
            "refund_date",
            "refund_amount",
        ).fillna(
            {
                "payment_method": "unknown",
                "payment_status": "unknown",
            }
        )

        # Only keep rows that actually have a refund date (i.e., a refund happened)
        refunded = payments.filter(F.col("refund_date").isNotNull())

        # Compute time to refund in days (you can also compute hours if these were TIMESTAMPs)
        refunded = refunded.withColumn(
            "days_to_refund",
            F.datediff(F.col("refund_date"), F.col("payment_date"))
        )

        # Basic distribution by payment method
        analysis["time_to_refund_by_payment_method"] = (
            refunded.groupBy("payment_method")
            .agg(
                F.countDistinct("payment_id").alias("refunded_payments"),
                F.sum("refund_amount").alias("total_refund_amount"),
                F.avg("days_to_refund").alias("avg_days_to_refund"),
                F.expr("percentile(days_to_refund, array(0.25, 0.5, 0.75))").alias(
                    "days_to_refund_percentiles"
                ),
                F.min("days_to_refund").alias("min_days_to_refund"),
                F.max("days_to_refund").alias("max_days_to_refund"),
            )
            .orderBy(F.col("avg_days_to_refund").asc_nulls_last())
        )



    # -------------------------------------------------------------------
    # Base reviews table: agg_reviews
    # -------------------------------------------------------------------
    if (
        "agg_reviews" in dataframes and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_reviews"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_reviews"], "review_date")
        and not is_column_all_null_or_zero(dataframes["agg_reviews"], "rating")
    ):
        reviews = dataframes["agg_reviews"].select(
            "product_id",
            F.col("review_date").cast("date").alias("review_date"),
            "rating",
        ).filter(F.col("review_date").isNotNull())

        # Optional enrichment with product metadata
        if (
            "agg_products" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
            and not is_column_all_null_or_zero(dataframes["agg_products"], "category")
        ):
            prod = dataframes["agg_products"].select(
                "product_id",
                "product_name",
                "category",
                "total_units_sold",
                "total_revenue",
            )
            reviews = reviews.alias("r").join(prod.alias("p"), on="product_id", how="left")

        # ----------------------------------------------------------------
        # 1) Daily review velocity per product
        # ----------------------------------------------------------------
        analysis["review_velocity_daily"] = (
            reviews.groupBy("product_id", "review_date")
            .agg(
                F.count("*").alias("daily_reviews"),
                F.avg("rating").alias("avg_rating_daily"),
            )
            .orderBy("product_id", "review_date")
        )

        # ----------------------------------------------------------------
        # 2) Weekly review velocity per product
        # ----------------------------------------------------------------
        reviews_weekly = (
            reviews
            .withColumn("review_year", F.year("review_date"))
            .withColumn("review_week", F.weekofyear("review_date"))
            .withColumn("year_week", F.date_format(F.col("review_date"), "yyyy-MM-dd"))
        )

        analysis["review_velocity_weekly"] = (
            reviews_weekly.groupBy("product_id", "year_week", "review_year", "review_week")
            .agg(
                F.count("*").alias("weekly_reviews"),
                F.avg("rating").alias("avg_rating_weekly"),
            )
            .orderBy("product_id", "review_year", "review_week")
        )

        # ----------------------------------------------------------------
        # 3) Monthly review velocity per product
        # ----------------------------------------------------------------
        reviews_monthly = (
            reviews
            .withColumn("review_year", F.year("review_date"))
            .withColumn("review_month", F.month("review_date"))
            .withColumn("year_month", F.date_format(F.col("review_date"), "yyyy-MM"))
        )

        analysis["review_velocity_monthly"] = (
            reviews_monthly.groupBy("product_id", "year_month", "review_year", "review_month")
            .agg(
                F.count("*").alias("monthly_reviews"),
                F.avg("rating").alias("avg_rating_monthly"),
            )
            .orderBy("product_id", "review_year", "review_month")
        )

    else:
        print(
            "agg_reviews not available, or product_id all NULL/zero; "
            "skipping review velocity analysis."
        )



    # Sentiment by Product Category
    if (
        "agg_reviews" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_reviews"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_reviews"], "review_sentiment")
        and not is_column_all_null_or_zero(dataframes["agg_reviews"], "rating")
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "category")

    ):
        reviews = dataframes["agg_reviews"].select(
            "product_id",
            "rating",
            "review_sentiment"
        )

        products = dataframes["agg_products"].select(
            "product_id",
            "category"
        )

        rev_prod = (
            reviews.alias("r")
            .join(products.alias("p"), on="product_id", how="inner")
        )

        # Optional: turn sentiment into numeric for scoring (simple mapping)
        rev_prod = rev_prod.withColumn(
            "sentiment_score",
            F.when(F.lower(F.col("review_sentiment")) == "positive", 1.0)
            .when(F.lower(F.col("review_sentiment")) == "negative", -1.0)
            .when(F.lower(F.col("review_sentiment")) == "neutral", 0.0)
            .otherwise(F.lit(0.0))
        )

        analysis["sentiment_by_category"] = (
            rev_prod.groupBy("category")
            .agg(
                F.count("*").alias("total_reviews"),
                F.avg("rating").alias("avg_rating"),
                F.expr("sum(CASE WHEN lower(review_sentiment) = 'positive' THEN 1 ELSE 0 END)").alias("positive_reviews"),
                F.expr("sum(CASE WHEN lower(review_sentiment) = 'negative' THEN 1 ELSE 0 END)").alias("negative_reviews"),
                F.expr("sum(CASE WHEN lower(review_sentiment) = 'neutral' THEN 1 ELSE 0 END)").alias("neutral_reviews"),
                F.avg("sentiment_score").alias("avg_sentiment_score"),
            )
            .withColumn(
                "positive_share",
                F.col("positive_reviews") / F.col("total_reviews")
            )
            .withColumn(
                "negative_share",
                F.col("negative_reviews") / F.col("total_reviews")
            )
            .orderBy(
                F.col("avg_sentiment_score").desc_nulls_last(),
                F.col("avg_rating").desc_nulls_last()
            )
        )

    else:
        print(
            "agg_reviews or agg_products not available, or product_id all NULL/zero; "
            "skipping sentiment by product category analysis."
        )




    # Low-rated product monthly trends (rating only)
    if (
        "agg_reviews" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_reviews"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_reviews"], "rating")
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue")
    ):
        # 1) Base reviews with date + rating
        reviews = dataframes["agg_reviews"].select(
            "product_id",
            F.col("review_date").cast("date").alias("review_date"),
            "rating",
        ).filter(F.col("review_date").isNotNull())

        # 2) Derive year/month keys
        reviews_monthly = (
            reviews
            .withColumn("review_year", F.year("review_date"))
            .withColumn("review_month", F.month("review_date"))
            .withColumn("year_month", F.date_format(F.col("review_date"), "yyyy-MM"))
        )

        # 3) Monthly rating per product
        monthly_ratings = (
            reviews_monthly.groupBy("product_id", "year_month", "review_year", "review_month")
            .agg(
                F.count("*").alias("monthly_reviews"),
                F.avg("rating").alias("avg_rating_month"),
            )
        )

        # 4) Optional: join with product metadata (only unique columns)
        if (
            "agg_products" in dataframes
            and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        ):
            prod = dataframes["agg_products"].select(
                "product_id",
                "product_name",
                "category",
                "total_units_sold",
                "total_revenue",
            )

            monthly_with_meta = (
                monthly_ratings.alias("m")
                .join(prod.alias("p"), on="product_id", how="left")
            )
        else:
            monthly_with_meta = monthly_ratings

        # 5) Full monthly rating trends
        analysis["product_monthly_rating_trends"] = (
            monthly_with_meta.select(
                "product_id",
                *[c for c in ["product_name", "category", "total_units_sold", "total_revenue"]
                if c in monthly_with_meta.columns],
                "year_month",
                "review_year",
                "review_month",
                "monthly_reviews",
                "avg_rating_month",
            )
            .orderBy("product_id", "review_year", "review_month")
        )

        # 6) Low-rated monthly periods (rating-only thresholds)
        MIN_REVIEWS_MONTH = monthly_with_meta.select(F.min("monthly_reviews")).collect()[0][0]
        RATING_THRESHOLD = monthly_with_meta.select(F.avg("avg_rating_month")).collect()[0][0]  # <= 3.5 stars is considered low

        low_rated_months = (
            monthly_with_meta
            .filter(
                (F.col("monthly_reviews") >= MIN_REVIEWS_MONTH)
                & (F.col("avg_rating_month") <= RATING_THRESHOLD)
            )
            .select(
                "product_id",
                *[c for c in ["product_name", "category", "total_units_sold", "total_revenue"]
                if c in monthly_with_meta.columns],
                "year_month",
                "review_year",
                "review_month",
                "monthly_reviews",
                "avg_rating_month",
            )
            .orderBy("product_id", "review_year", "review_month")
        )

        analysis["low_rated_product_monthly_trends_rating_only"] = low_rated_months

    else:
        print(
            "agg_reviews not available, or product_id all NULL/zero; "
            "skipping rating-only low-rated product monthly trends."
        )



    # Rating Threshold Analysis: Sales velocity at different rating tiers
    if (
        "agg_reviews" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_reviews"], "product_id")
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_reviews"], "rating")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_units_sold")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "total_revenue")
    ):
        # 1) Per-product average rating from reviews
        reviews = dataframes["agg_reviews"].select(
            "product_id",
            "rating",
        )

        per_product_rating = (
            reviews.groupBy("product_id")
            .agg(
                F.count("*").alias("total_reviews"),
                F.avg("rating").alias("avg_rating_product"),
            )
        )

        # 2) Join with product sales metrics
        products = dataframes["agg_products"].select(
            "product_id",
            "product_name",
            "category",
            "total_units_sold",
            "total_revenue",
        )

        rating_sales = (
            per_product_rating.alias("r")
            .join(products.alias("p"), on="product_id", how="inner")
        )

        # Optional filter: ignore products with too few reviews
        MIN_REVIEWS = 5
        rating_sales = rating_sales.filter(F.col("total_reviews") >= MIN_REVIEWS)

        # 3) Define rating tiers (you can tweak ranges / labels)
        rating_sales = rating_sales.withColumn(
            "rating_tier",
            F.when(F.col("avg_rating_product") >= 4.5, "4.5 - 5.0")
            .when((F.col("avg_rating_product") >= 4.0) & (F.col("avg_rating_product") < 4.5), "4.0 - 4.5")
            .when((F.col("avg_rating_product") >= 3.5) & (F.col("avg_rating_product") < 4.0), "3.5 - 4.0")
            .when((F.col("avg_rating_product") >= 3.0) & (F.col("avg_rating_product") < 3.5), "3.0 - 3.5")
            .otherwise("< 3.0")
        )

        # 4) Aggregate sales velocity per rating tier
        # (velocity here = total_units_sold; if you have time-normalized fields, plug them in)
        tier_stats = (
            rating_sales.groupBy("rating_tier")
            .agg(
                F.count("*").alias("products_in_tier"),
                F.sum("total_units_sold").alias("total_units_sold_tier"),
                F.sum("total_revenue").alias("total_revenue_tier"),
                F.avg("total_units_sold").alias("avg_units_sold_per_product"),
                F.avg("total_revenue").alias("avg_revenue_per_product"),
                F.avg("avg_rating_product").alias("avg_rating_in_tier"),
                F.avg("total_reviews").alias("avg_reviews_per_product"),
            )
            .orderBy(
                F.col("rating_tier").desc()
            )
        )

        # 5) Save detailed per-product mapping and tier summary
        analysis["rating_tier_per_product"] = rating_sales.select(
            "product_id",
            "product_name",
            "category",
            "total_reviews",
            "avg_rating_product",
            "total_units_sold",
            "total_revenue",
            "rating_tier",
        )

        analysis["rating_tier_sales_velocity"] = tier_stats

    else:
        print(
            "agg_reviews or agg_products not available, or product_id all NULL/zero; "
            "skipping rating threshold sales velocity analysis."
        )



    # =============================================================================
    # 1) Order Processing Time by Product Category
    # =============================================================================
    if (
        "agg_orders" in dataframes
        and "agg_order_items" in dataframes
        and "agg_products" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id")
        and not is_column_all_null_or_zero(dataframes["agg_products"], "product_id")
        and not is_column_all_null_or_zero(dataframes["agg_order_items"], "order_id")
        and not is_column_all_null_or_zero(dataframes["agg_order_items"], "product_id")
    ):
        orders_with_cat = (
            dataframes["agg_order_items"]. select("order_id", "product_id", "quantity")
            .join(dataframes["agg_products"]. select("product_id", "category", "sub_category"), on="product_id", how="left")
            .join(
                dataframes["agg_orders"].select(
                    "order_id", "order_processing_days_diff", "delivery_days_diff", "total_order_fulfillment_time_days"
                ),
                on="order_id", how="inner"
            )
        )

        # Category-level aggregation
        analysis["processing_by_category"] = (
            orders_with_cat
            .groupBy("category")
            .agg(
                F.countDistinct("order_id").alias("orders"),
                F.sum("quantity").alias("total_units"),
                F.avg("order_processing_days_diff").alias("avg_processing_days"),
                F.avg("delivery_days_diff").alias("avg_delivery_days"),
                F.avg("total_order_fulfillment_time_days").alias("avg_total_fulfillment_days"),
                F.expr("percentile_approx(order_processing_days_diff, 0.5)").alias("median_processing_days"),
                F.max("order_processing_days_diff").alias("max_processing_days")
            )
            .orderBy(F.col("avg_processing_days").desc_nulls_last())
        )

        # Sub-category drill-down
        if not is_column_all_null_or_zero(orders_with_cat, "sub_category"):
            analysis["processing_by_subcategory"] = (
                orders_with_cat
                .filter(F.col("sub_category").isNotNull())
                .groupBy("category", "sub_category")
                .agg(
                    F. countDistinct("order_id").alias("orders"),
                    F. avg("order_processing_days_diff").alias("avg_processing_days"),
                    F.avg("delivery_days_diff").alias("avg_delivery_days"),
                    F.expr("percentile_approx(order_processing_days_diff, 0.5)").alias("median_processing_days")
                )
                .orderBy(F.col("avg_processing_days").desc_nulls_last())
            )

    # =============================================================================
    # 2) Peak Processing Times Analysis
    # =============================================================================
    if "agg_orders" in dataframes and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id"):
        
        orders_time = (
            dataframes["agg_orders"]
            .select("order_id", "order_placed_at", "order_processing_days_diff", "delivery_days_diff", "total_amount")
            .filter(F.col("order_placed_at").isNotNull())
            .withColumn("order_hour", F.hour("order_placed_at"))
            .withColumn("order_dow", F.dayofweek("order_placed_at"))
            .withColumn("order_dow_name", F.date_format("order_placed_at", "E"))
            .withColumn("is_weekend", F.when(F.col("order_dow").isin([1, 7]), 1).otherwise(0))
        )

        # Processing time by hour of day
        analysis["processing_by_hour"] = (
            orders_time
            .groupBy("order_hour")
            .agg(
                F.count("*").alias("orders"),
                F.avg("order_processing_days_diff").alias("avg_processing_days"),
                F.avg("delivery_days_diff").alias("avg_delivery_days"),
                F.sum("total_amount").alias("total_revenue")
            )
            .orderBy(F.col("order_hour").asc())
        )

        # Processing time by day of week
        analysis["processing_by_day_of_week"] = (
            orders_time
            .groupBy("order_dow", "order_dow_name")
            .agg(
                F.count("*").alias("orders"),
                F.avg("order_processing_days_diff").alias("avg_processing_days"),
                F.avg("delivery_days_diff").alias("avg_delivery_days"),
                F.sum("total_amount").alias("total_revenue")
            )
            .orderBy(F.col("order_dow").asc())
        )

        # Weekend vs Weekday comparison
        analysis["weekend_vs_weekday"] = (
            orders_time
            .groupBy("is_weekend")
            .agg(
                F. count("*").alias("orders"),
                F.avg("order_processing_days_diff").alias("avg_processing_days"),
                F.avg("delivery_days_diff").alias("avg_delivery_days"),
                F.sum("total_amount").alias("total_revenue")
            )
            .withColumn("day_type", F.when(F.col("is_weekend") == 1, "Weekend").otherwise("Weekday"))
            .select("day_type", "orders", "avg_processing_days", "avg_delivery_days", "total_revenue")
            .orderBy(F.col("is_weekend").asc())
        )



    # Base orders with delivery info
    if (
        "agg_orders" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id")
        and not is_column_all_null_or_zero(dataframes["agg_orders"], "customer_id")
        and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_delivered_at")
        and not is_column_all_null_or_zero(dataframes["agg_orders"], "delivery_days_diff")
        and "agg_customers" in dataframes
        and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id")
        and not is_column_all_null_or_zero(dataframes["agg_customers"], "country")
        and not is_column_all_null_or_zero(dataframes["agg_customers"], "state_province")
        and not is_column_all_null_or_zero(dataframes["agg_customers"], "city")
    ):
        orders = dataframes["agg_orders"].select(
            "order_id",
            "customer_id",
            "order_delivered_at",
            "delivery_days_diff",
        ).filter(F.col("order_delivered_at").isNotNull())

        customers = dataframes["agg_customers"].select(
            "customer_id",
            "country",
            "state_province",
            "city",
        )

        # Join orders -> customers to bring geography in
        orders_geo = (
            orders.alias("o")
            .join(customers.alias("c"), on="customer_id", how="left")
        )

        # --- By country ---
        analysis["delivery_days_by_country"] = (
            orders_geo.groupBy("country")
            .agg(
                F.count("*").alias("delivered_orders"),
                F.avg("delivery_days_diff").alias("avg_delivery_days"),
                F.expr("percentile_approx(delivery_days_diff, 0.5)").alias("median_delivery_days"),
                F.max("delivery_days_diff").alias("max_delivery_days"),
            )
            .orderBy(F.col("avg_delivery_days").desc_nulls_last())
        )

        # --- By state (within country) ---
        analysis["delivery_days_by_state"] = (
            orders_geo.groupBy("country", "state_province")
            .agg(
                F.count("*").alias("delivered_orders"),
                F.avg("delivery_days_diff").alias("avg_delivery_days"),
                F.expr("percentile_approx(delivery_days_diff, 0.5)").alias("median_delivery_days"),
                F.max("delivery_days_diff").alias("max_delivery_days"),
            )
            .orderBy(
                F.col("avg_delivery_days").desc_nulls_last(),
                "country",
                "state_province",
            )
        )

        # --- By city (within state/country) ---
        analysis["delivery_days_by_city"] = (
            orders_geo.groupBy("country", "state_province", "city")
            .agg(
                F.count("*").alias("delivered_orders"),
                F.avg("delivery_days_diff").alias("avg_delivery_days"),
                F.expr("percentile_approx(delivery_days_diff, 0.5)").alias("median_delivery_days"),
                F.max("delivery_days_diff").alias("max_delivery_days"),
            )
            .orderBy(
                F.col("avg_delivery_days").desc_nulls_last(),
                "country",
                "state_province",
                "city",
            )
        )

    else:
        print(
            "agg_orders or agg_customers not available, or key ID columns NULL/zero; "
            "skipping delivery_days_diff by geography."
        )




    # Approximate on-time delivery using a simple SLA threshold
    # adjust to your promised standard (e.g., 2, 3, 5)
    # Layer 1: Table Existence Check
    if (
        "agg_orders" in dataframes 
        and "agg_customers" in dataframes
    ):
        # Layer 2: Column Null/Zero Check
        if (
            not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id")
            and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id")
            and not is_column_all_null_or_zero(dataframes["agg_orders"], "delivery_days_diff")
        ):
            orders = dataframes["agg_orders"].select(
                "order_id",
                "customer_id",
                "order_delivered_at",
                "delivery_days_diff",
            ).filter(F.col("order_delivered_at").isNotNull())

            customers = dataframes["agg_customers"].select(
                "customer_id",
                "country",
                "state_province",
                "city",
            )

            orders_geo = (
                orders.alias("o")
                .join(customers.alias("c"), on="customer_id", how="left")
            )
            SLA_DAYS = dataframes["agg_orders"].select(F.avg("delivery_days_diff")).collect()[0][0]
            orders_geo = orders_geo.withColumn(
                "is_on_time",
                F.when(F.col("delivery_days_diff") <= SLA_DAYS, F.lit(1)).otherwise(F.lit(0)),
            )

            # --- On-time delivery % by country ---
            analysis["ontime_delivery_by_country"] = (
                orders_geo.groupBy("country")
                .agg(
                    F.count("*").alias("delivered_orders"),
                    F.sum("is_on_time").alias("on_time_orders"),
                    (F.sum("is_on_time") / F.count("*")).alias("on_time_rate"),
                    F.floor(F.avg("delivery_days_diff")).alias("avg_delivery_days"),
                )
                .orderBy(F.col("on_time_rate").asc_nulls_last())
            )

            # --- On-time delivery % by state ---
            analysis["ontime_delivery_by_state"] = (
                orders_geo.groupBy("country", "state_province")
                .agg(
                    F.count("*").alias("delivered_orders"),
                    F.sum("is_on_time").alias("on_time_orders"),
                    (F.sum("is_on_time") / F.count("*")).alias("on_time_rate"),
                    F.floor(F.avg("delivery_days_diff")).alias("avg_delivery_days"),
                )
                .orderBy(F.col("on_time_rate").asc_nulls_last())
            )

            # --- On-time delivery % by city ---
            analysis["ontime_delivery_by_city"] = (
                orders_geo.groupBy("country", "state_province", "city")
                .agg(
                    F.count("*").alias("delivered_orders"),
                    F.sum("is_on_time").alias("on_time_orders"),
                    (F.sum("is_on_time") / F.count("*")).alias("on_time_rate"),
                    F.floor(F.avg("delivery_days_diff")).alias("avg_delivery_days"),
                )
                .orderBy(F.col("on_time_rate").asc_nulls_last())
            )

    else:
        print(
            "agg_orders or agg_customers not available, or key ID columns NULL/zero; "
            "skipping on-time delivery approximation."
        )



    # Shipping Cost Efficiency by Region: Analyzing costs across Geo-grains
    # ------------------------------------------------------------------

    # Layer 1: Table Existence Check
    if "agg_orders" in dataframes and "agg_customers" in dataframes:
        # Layer 2: Critical Column Validation
        if (
            not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id")
            and not is_column_all_null_or_zero(dataframes["agg_orders"], "customer_id")
            and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id")
        ):
            # 1) Base orders: keep only fields needed
            orders = dataframes["agg_orders"].select(
                "order_id",
                "customer_id",
                "subtotal",
                "shipping_cost",
            )

            # Avoid div-by-zero: ship% only where subtotal > 0
            orders = orders.withColumn(
                "shipping_pct_of_subtotal",
                F.when(
                    F.col("subtotal") > 0,
                    F.col("shipping_cost") / F.col("subtotal")
                ).otherwise(F.lit(None)),
            )

            # 2) Customers: region info
            customers = dataframes["agg_customers"].select(
                "customer_id",
                "country",
                "state_province",
                "city",
            )

            # 3) Join orders -> customers
            orders_geo = (
                orders.alias("o")
                .join(customers.alias("c"), on="customer_id", how="left")
            )

            # Common aggregation helper
            def region_agg(df, group_cols):
                return (
                    df.groupBy(*group_cols)
                    .agg(
                        F.count("*").alias("orders"),
                        F.sum("shipping_cost").alias("total_shipping_cost"),
                        F.sum("subtotal").alias("total_subtotal"),
                        F.avg("shipping_pct_of_subtotal").alias("avg_shipping_pct_of_subtotal"),
                        F.expr(
                            "percentile_approx(shipping_pct_of_subtotal, 0.5)"
                        ).alias("median_shipping_pct_of_subtotal"),
                    )
                    .withColumn(
                        "shipping_pct_of_subtotal_overall",
                        F.when(
                            F.col("total_subtotal") > 0,
                            F.col("total_shipping_cost") / F.col("total_subtotal"),
                        ).otherwise(F.lit(None)),
                    )
                )

            # 4) By country
            analysis["shipping_efficiency_by_country"] = (
                region_agg(orders_geo, ["country"])
                .orderBy(
                    F.col("avg_shipping_pct_of_subtotal").desc_nulls_last(),
                    "country",
                )
            )

            # 5) By state (within country)
            analysis["shipping_efficiency_by_state"] = (
                region_agg(orders_geo, ["country", "state_province"])
                .orderBy(
                    F.col("avg_shipping_pct_of_subtotal").desc_nulls_last(),
                    "country",
                    "state_province",
                )
            )

            # 6) By city (within state/country)
            analysis["shipping_efficiency_by_city"] = (
                region_agg(orders_geo, ["country", "state_province", "city"])
                .orderBy(
                    F.col("avg_shipping_pct_of_subtotal").desc_nulls_last(),
                    "country",
                    "state_province",
                    "city",
                )
            )

    else:
        print(
            "agg_orders or agg_customers not available, or key ID columns NULL/zero; "
            "skipping shipping cost efficiency by region."
        )




    # Approximate on-time delivery using a simple SLA threshold
    # adjust to your promised standard (e.g., 2, 3, 5)
    # Layer 1: Table Existence Check
    if (
        "agg_orders" in dataframes 
        and "agg_customers" in dataframes
    ):
        # Layer 2: Column Null/Zero Check
        if (
            not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id")
            and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id")
            and not is_column_all_null_or_zero(dataframes["agg_orders"], "delivery_days_diff")
        ):
            orders = dataframes["agg_orders"].select(
                "order_id",
                "customer_id",
                "order_delivered_at",
                "delivery_days_diff",
            ).filter(F.col("order_delivered_at").isNotNull())

            customers = dataframes["agg_customers"].select(
                "customer_id",
                "country",
                "state_province",
                "city",
            )

            orders_geo = (
                orders.alias("o")
                .join(customers.alias("c"), on="customer_id", how="left")
            )
            SLA_DAYS = dataframes["agg_orders"].select(F.avg("delivery_days_diff")).collect()[0][0]
            orders_geo = orders_geo.withColumn(
                "is_on_time",
                F.when(F.col("delivery_days_diff") <= SLA_DAYS, F.lit(1)).otherwise(F.lit(0)),
            )

            # --- On-time delivery % by country ---
            analysis["ontime_delivery_by_country"] = (
                orders_geo.groupBy("country")
                .agg(
                    F.count("*").alias("delivered_orders"),
                    F.sum("is_on_time").alias("on_time_orders"),
                    (F.sum("is_on_time") / F.count("*")).alias("on_time_rate"),
                    F.floor(F.avg("delivery_days_diff")).alias("avg_delivery_days"),
                )
                .orderBy(F.col("on_time_rate").asc_nulls_last())
            )

            # --- On-time delivery % by state ---
            analysis["ontime_delivery_by_state"] = (
                orders_geo.groupBy("country", "state_province")
                .agg(
                    F.count("*").alias("delivered_orders"),
                    F.sum("is_on_time").alias("on_time_orders"),
                    (F.sum("is_on_time") / F.count("*")).alias("on_time_rate"),
                    F.floor(F.avg("delivery_days_diff")).alias("avg_delivery_days"),
                )
                .orderBy(F.col("on_time_rate").asc_nulls_last())
            )

            # --- On-time delivery % by city ---
            analysis["ontime_delivery_by_city"] = (
                orders_geo.groupBy("country", "state_province", "city")
                .agg(
                    F.count("*").alias("delivered_orders"),
                    F.sum("is_on_time").alias("on_time_orders"),
                    (F.sum("is_on_time") / F.count("*")).alias("on_time_rate"),
                    F.floor(F.avg("delivery_days_diff")).alias("avg_delivery_days"),
                )
                .orderBy(F.col("on_time_rate").asc_nulls_last())
            )

    else:
        print(
            "agg_orders or agg_customers not available, or key ID columns NULL/zero; "
            "skipping on-time delivery approximation."
        )




    # Shipping Cost Efficiency by Region: Analyzing costs across Geo-grains
    # ------------------------------------------------------------------

    # Layer 1: Table Existence Check
    if "agg_orders" in dataframes and "agg_customers" in dataframes:
        # Layer 2: Critical Column Validation
        if (
            not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id")
            and not is_column_all_null_or_zero(dataframes["agg_orders"], "customer_id")
            and not is_column_all_null_or_zero(dataframes["agg_customers"], "customer_id")
        ):
            # 1) Base orders: keep only fields needed
            orders = dataframes["agg_orders"].select(
                "order_id",
                "customer_id",
                "subtotal",
                "shipping_cost",
            )

            # Avoid div-by-zero: ship% only where subtotal > 0
            orders = orders.withColumn(
                "shipping_pct_of_subtotal",
                F.when(
                    F.col("subtotal") > 0,
                    F.col("shipping_cost") / F.col("subtotal")
                ).otherwise(F.lit(None)),
            )

            # 2) Customers: region info
            customers = dataframes["agg_customers"].select(
                "customer_id",
                "country",
                "state_province",
                "city",
            )

            # 3) Join orders -> customers
            orders_geo = (
                orders.alias("o")
                .join(customers.alias("c"), on="customer_id", how="left")
            )

            # Common aggregation helper
            def region_agg(df, group_cols):
                return (
                    df.groupBy(*group_cols)
                    .agg(
                        F.count("*").alias("orders"),
                        F.sum("shipping_cost").alias("total_shipping_cost"),
                        F.sum("subtotal").alias("total_subtotal"),
                        F.avg("shipping_pct_of_subtotal").alias("avg_shipping_pct_of_subtotal"),
                        F.expr(
                            "percentile_approx(shipping_pct_of_subtotal, 0.5)"
                        ).alias("median_shipping_pct_of_subtotal"),
                    )
                    .withColumn(
                        "shipping_pct_of_subtotal_overall",
                        F.when(
                            F.col("total_subtotal") > 0,
                            F.col("total_shipping_cost") / F.col("total_subtotal"),
                        ).otherwise(F.lit(None)),
                    )
                )

            # 4) By country
            analysis["shipping_efficiency_by_country"] = (
                region_agg(orders_geo, ["country"])
                .orderBy(
                    F.col("avg_shipping_pct_of_subtotal").desc_nulls_last(),
                    "country",
                )
            )

            # 5) By state (within country)
            analysis["shipping_efficiency_by_state"] = (
                region_agg(orders_geo, ["country", "state_province"])
                .orderBy(
                    F.col("avg_shipping_pct_of_subtotal").desc_nulls_last(),
                    "country",
                    "state_province",
                )
            )

            # 6) By city (within state/country)
            analysis["shipping_efficiency_by_city"] = (
                region_agg(orders_geo, ["country", "state_province", "city"])
                .orderBy(
                    F.col("avg_shipping_pct_of_subtotal").desc_nulls_last(),
                    "country",
                    "state_province",
                    "city",
                )
            )

    else:
        print(
            "agg_orders or agg_customers not available, or key ID columns NULL/zero; "
            "skipping shipping cost efficiency by region."
        )



    # -------------------------------------------------------
    # Seasonal Processing Delay Analysis
    # -------------------------------------------------------

    # Layer 1: Table Existence Check
    if "agg_orders" in dataframes:
        # Layer 2: Column Null/Zero Check
        if (
            not is_column_all_null_or_zero(dataframes["agg_orders"], "order_id")
            and not is_column_all_null_or_zero(dataframes["agg_orders"], "season")
            and not is_column_all_null_or_zero(dataframes["agg_orders"], "order_processing_days_diff")
        ):
            orders = dataframes["agg_orders"].select(
                "order_id",
                "order_status",
                "season",
                "order_processing_days_diff",
            ).filter(F.col("season").isNotNull())

            # 1) Processing delays by season
            analysis["processing_by_season"] = (
                orders.groupBy("season")
                .agg(
                    F.count("*").alias("orders"),
                    F.avg("order_processing_days_diff").alias("avg_processing_days"),
                    F.expr(
                        "percentile_approx(order_processing_days_diff, 0.5)"
                    ).alias("median_processing_days"),
                    F.max("order_processing_days_diff").alias("max_processing_days"),
                )
                .orderBy(F.col("avg_processing_days").desc_nulls_last())
            )

            # 2) Breakdown by season + order_status
            analysis["processing_by_season_and_status"] = (
                orders.groupBy("season", "order_status")
                .agg(
                    F.count("*").alias("orders"),
                    F.avg("order_processing_days_diff").alias("avg_processing_days"),
                    F.expr(
                        "percentile_approx(order_processing_days_diff, 0.5)"
                    ).alias("median_processing_days"),
                )
                .orderBy("season", F.col("avg_processing_days").desc_nulls_last())
            )
        else:
            print("Required columns (season or processing days) are NULL or zero; skipping analysis.")
    else:
        print("agg_orders not available; skipping processing delays by season analysis.")


    print("customer analysis count")
    count = 0
    for key in analysis.keys():
        count+=1
    print(count)


    print("Product analysis count")
    count = 0
    for key in product_analysis.keys():
        count+=1
    print(count)

    print("supplier analysis count")
    count = 0
    for key in supplier_analysis.keys():
        count+=1
    print(count)
    

    print("\n✅ Analysis Complete.")

if __name__ == "__main__":
    main()