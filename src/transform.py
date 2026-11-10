import structlog
import polars as pl
import os
from pathlib import Path
from datetime import datetime

logger = structlog.get_logger()

BRONZE_DIR = "/opt/airflow/data/bronze"
SILVER_DIR = "/opt/airflow/data/silver"
DLQ_DIR = "/opt/airflow/data/dlq"

def clean_to_silver(**kwargs):
    """
    Clean data from Bronze layer (Parquet) and load into Silver layer (Parquet).
    Implement Data Contracts and DLQ for invalid data.
    """
    run_id = kwargs.get('dag_run', None)
    log = logger.bind(run_id=run_id.run_id if run_id else "manual")
    
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(DLQ_DIR, exist_ok=True)
    
    try:
        # TODO: Đọc các file parquet từ BRONZE_DIR vào các DataFrame của polars
        # Gợi ý: pl.read_parquet(f"{BRONZE_DIR}/<table_name>.parquet")
        
        # YOUR CODE HERE
        customers_df = pl.read_parquet(f"{BRONZE_DIR}/customers_dataset.parquet")
        orders_df = pl.read_parquet(f"{BRONZE_DIR}/orders_dataset.parquet")
        order_items_df = pl.read_parquet(f"{BRONZE_DIR}/order_items_dataset.parquet")
        products_df = pl.read_parquet(f"{BRONZE_DIR}/products_dataset.parquet")
        sellers_df = pl.read_parquet(f"{BRONZE_DIR}/sellers_dataset.parquet")
        geolocation_df = pl.read_parquet(f"{BRONZE_DIR}/geolocation_dataset.parquet")
        payments_df = pl.read_parquet(f"{BRONZE_DIR}/order_payments_dataset.parquet")
        reviews_df = pl.read_parquet(f"{BRONZE_DIR}/order_reviews_dataset.parquet")
        translation_df = pl.read_parquet(f"{BRONZE_DIR}/product_category_name_translation.parquet")

        
        # 1. Clean Customers
        log.info("Cleaning Customers...")
        # TODO: Loại bỏ các dòng trùng lặp theo customer_id
        # TODO: Chuẩn hóa cột customer_city thành chữ thường (lowercase)
        # TODO: Chuẩn hóa cột customer_state thành chữ HOA (uppercase)
        # TODO: Join với bảng geolocation_df (đã được aggregate theo zip_code_prefix) để lấy lat/lng
        # TODO: Lưu kết quả thành dim_customers.parquet vào SILVER_DIR
        
        # YOUR CODE HERE
        geo_agg = (
            geolocation_df
            .group_by("geolocation_zip_code_prefix")
            .agg(
                pl.col("geolocation_lat").mean(),
                pl.col("geolocation_lng").mean(),
            )
        )
        dim_customers = (
            customers_df
            .unique(subset=["customer_id"], keep="first")
            .with_columns(
                pl.col("customer_city").str.to_lowercase().str.strip_chars(),
                pl.col("customer_state").str.to_lowercase().str.strip_chars(),
            )
            .join(
                geo_agg,
                left_on="customer_zip_code_prefix",
                right_on="geolocation_zip_code_prefix",
                how="left"
            )
            .rename({
                "geolocation_lat": "customer_lat",
                "geolocation_lng": "customer_lng",
            })
        )
        dim_customers.write_parquet(f"{SILVER_DIR}/dim_customers.parquet")
        
        # 2. Clean Sellers
        log.info("Cleaning Sellers...")
        # TODO: Loại bỏ các dòng trùng lặp và null ở seller_id
        # TODO: Join với geolocation_df để lấy lat/lng
        # TODO: Lưu kết quả thành dim_sellers.parquet vào SILVER_DIR
        
        # YOUR CODE HERE
        dim_sellers = (
            sellers_df
            .drop_nulls(subset=["seller_id"])
            .unique(subset=["seller_id"], keep="first")
            .with_columns(
                pl.col("seller_city").str.to_lowercase().str.strip_chars(),
                pl.col("seller_state").str.to_lowercase().str.strip_chars(),
            )
            .join(
                geo_agg,
                left_on="seller_zip_code_prefix",
                right_on="geolocation_zip_code_prefix",
                how="left"
            )
            .rename({
                "geolocation_lat": "seller_lat",
                "geolocation_lng": "seller_lng",
            })
            .drop("seller_city")
        )

        dim_sellers.write_parquet(f"{SILVER_DIR}/dim_sellers.parquet")
        
        # 3. Clean Products
        log.info("Cleaning Products...")
        # TODO: Loại bỏ các dòng trùng lặp ở product_id và fill null bằng strategy forward
        # TODO: Đổi tên các cột bị sai chính tả (vd: product_name_lenght -> product_name_length)
        # TODO: Join với translation_df để lấy tên danh mục tiếng anh
        # TODO: Lưu kết quả thành dim_products.parquet vào SILVER_DIR
        
        # YOUR CODE HERE
        dim_products = (
            products_df
            .unique(subset=["product_id"], keep="first")
            .fill_null(strategy="forward")
            .rename({
                "product_name_lenght": "product_name_length",
                "product_description_lenght": "product_description_length",
            })
            .join(translation_df, on="product_category_name", how="left")
            .drop("product_category_name")
        )

        dim_products.write_parquet(f"{SILVER_DIR}/dim_products.parquet")
        
        
        # 4. Transform Fact Order Items
        log.info("Transforming Fact Order Items...")
        # TODO: Lọc các đơn hàng hợp lệ (vd: nếu delivered thì phải có đủ các trường ngày tháng)
        # TODO: Join order_items_df với các đơn hàng hợp lệ (orders_df) để tạo fct_items_df
        # TODO: Kiểm tra Data Contract (price > 0 và freight_value >= 0)
        # TODO: Lưu các record HỢP LỆ vào fct_order_items.parquet trong SILVER_DIR
        # TODO: Đưa các record KHÔNG HỢP LỆ vào thư mục DLQ (Dead Letter Queue)
        
        # YOUR CODE HERE
        valid_orders = orders_df.filter(
            (pl.col("order_status") != "delivered")
            | (
                pl.col("order_purchase_timestamp").is_not_null()
                & pl.col("order_delivered_customer_date").is_not_null()
                & pl.col("order_delivered_carrier_date").is_not_null()
            )
        ).select(
            "order_id", "customer_id", "order_status", "order_purchase_timestamp"
        )

        fct_items_df = (
            order_items_df
            .join(
                valid_orders,
                on="order_id",
                how="inner"
            )
            .with_columns(
                pl.col("shipping_limit_date").str.to_datetime("%Y-%m-%d %H:%M:%S"),
                pl.col("order_purchase_timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S"),
            )
        )
        is_valid = (pl.col("price") > 0) & (pl.col("freight_value") >= 0)
        fct_order_items = fct_items_df.filter(is_valid)
        invalid_items = fct_items_df.filter(~is_valid)

        fct_order_items.write_parquet(f"{SILVER_DIR}/fct_order_items.parquet")
        if invalid_items.height > 0:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            invalid_items.write_parquet(f"{DLQ_DIR}/fct_order_items_invalid_{ts}.parquet")

        error_rate = 0
        if fct_items_df.height > 0:
            error_rate = invalid_items.height / fct_items_df.height

        # 5. Transform Fact Order Payments
        log.info("Transforming Fact Order Payments...")
        # TODO: Join payments_df với orders_df (lấy order_id, customer_id)
        # TODO: Loại bỏ trùng lặp
        # TODO: Lưu fct_order_payments.parquet
        
        # YOUR CODE HERE
        fct_order_payments = (
            payments_df
            .join(
                orders_df.select("order_id", "customer_id"),
                on="order_id",
                how="inner"
            )
            .unique(subset=["order_id", "payment_sequential"], keep="first")
        )

        fct_order_payments.write_parquet(f"{SILVER_DIR}/fct_order_payments.parquet")
        
        # 6. Transform Fact Order Reviews
        log.info("Transforming Fact Order Reviews...")
        # TODO: Join reviews_df với orders_df
        # TODO: Loại bỏ trùng lặp
        # TODO: Lưu fct_order_reviews.parquet
        
        # YOUR CODE HERE
        fct_order_reviews = (
            reviews_df
            .join(
                orders_df.select("order_id", "customer_id"),
                on="order_id",
                how="inner"
            )
            .unique(subset=["review_id", "order_id"], keep="first")
            .with_columns(
                pl.col("review_creation_date").str.to_datetime("%Y-%m-%d %H:%M:%S"),
                pl.col("review_answer_timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S"),
            )
        )

        fct_order_reviews.write_parquet(f"{SILVER_DIR}/fct_order_reviews.parquet")


        # Lưu thông tin chất lượng dữ liệu để task check_quality kiểm tra
        # TODO: Cập nhật biến pass_quality. Ví dụ: pass_quality = True nếu số record bị lỗi < 10%
        pass_quality = error_rate < 0.1
        kwargs['ti'].xcom_push(key="pass_quality", value=pass_quality)
        
        log.info("transform_silver_success")
                 
    except Exception as e:
        log.exception("transform_silver_failed")
        raise e

def check_quality(**kwargs):
    """
    Perform Data Quality checks on Silver data (inspired by Great Expectations).
    """
    passed = kwargs['ti'].xcom_pull(task_ids="transform_silver", key="pass_quality")
    log = logger.bind(task="check_quality")
    passed = bool(passed)
    
    try:
        # TODO: Thực hiện Data Quality check dựa vào thư viện polars
        # 1. Đọc file dim_customers.parquet
        # 2. Kiểm tra số lượng dòng (phải lớn hơn 0)
        # 3. Kiểm tra xem có customer_id nào bị null không
        # 4. Kiểm tra tính duy nhất của customer_id (số lượng unique bằng tổng số record)
        # Nếu bất kỳ check nào fail, gán passed = False
        
        # YOUR CODE HERE
        dim_customers = pl.read_parquet(f"{SILVER_DIR}/dim_customers.parquet")
        check_empty = dim_customers.height <= 0
        check_null = dim_customers["customer_id"].is_null().sum() > 0
        check_not_unique = dim_customers["customer_id"].n_unique() != dim_customers.height

        # TODO: Kiểm tra với bảng fct_order_items.parquet
        # Kiểm tra order_status chỉ thuộc tập giá trị hợp lệ 
        # (vd: ["delivered", "shipped", "invoiced", "processing", "approved", "canceled", "unavailable", "created"])
        
        # YOUR CODE HERE
        fct_order_items = pl.read_parquet(f"{SILVER_DIR}/fct_order_items.parquet")
        valid_statuses = ["delivered", "shipped", "invoiced", "processing", "approved", "canceled", "unavailable", "created"]
        check_invalid_status = fct_order_items.filter(
            ~pl.col("order_status") in valid_statuses
        ).height != 0

        if check_empty or check_null or check_not_unique or check_invalid_status: passed = False

        if passed:
            log.info("All Data Quality checks passed successfully!")
            return "load_to_gold"
        else:
            log.warning("Data Quality checks failed. Routing to alert.")
            return "alert_quality"
            
    except Exception as e:
        log.exception(f"Data Quality check encountered an error: {e}")
        return "alert_quality"

if __name__ == "__main__":
    # For local testing
    clean_to_silver(ti=type('obj', (object,), {'xcom_push': lambda self, **kw: None})())
