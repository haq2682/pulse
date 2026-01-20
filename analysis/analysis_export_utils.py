import json
from io import BytesIO
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import DataFrame
from minio import Minio

from analysis_export_config import (
    create_minio_client,
    get_bucket_name,
    ensure_bucket_exists,
    get_category_for_key,
    SUPPORTED_FORMATS,
)


def get_object_path(category: str, key:str, file_format: str) -> str:
    return f"analytics/{category}/{key}.{file_format}"


def get_content_type(file_format:str) -> str:
    return {
        "parquet": "application/octet-stream",
        "csv": "text/csv",
        "json": "application/json"
    }.get(file_format, "application/octet-stream")


def serialize_dataframe(df: DataFrame, file_format:str) -> Tuple[BytesIO, int]:
    pdf = df.toPandas()
    buffer = BytesIO()
    
    if file_format == "parquet":
        pdf.to_parquet(buffer, index=False, compression="snappy")
    elif file_format == "csv": 
        pdf.to_csv(buffer, index=False)
    elif file_format == "json": 
        pdf.to_json(buffer, orient="records", lines=True)
    
    buffer.seek(0)
    return buffer, len(buffer.getvalue())


def upload_single(
    minio_client: Minio,
    bucket_name: str,
    key: str,
    df: DataFrame,
    category: str,
    file_format: str,
) -> Dict[str, Any]: 
    result = {
        "key": key,
        "category": category,
        "success": False,
        "object_path": None,
        "rows":0,
        "bytes": 0,
        "error": None
    }
    
    try:
        if df is None:
            result["error"] = "DataFrame is None"
            return result
        
        row_count = df.count()
        if row_count == 0:
            result["error"] = "DataFrame is empty"
            return result
        
        result["rows"] = row_count
        buffer, length = serialize_dataframe(df, file_format)
        result["bytes"] = length
        
        object_path = get_object_path(category, key, file_format)
        result["object_path"] = object_path
        
        minio_client.put_object(
            bucket_name,
            object_path,
            buffer,
            length=length,
            content_type=get_content_type(file_format)
        )
        
        buffer.close()
        result["success"] = True
        
    except Exception as e: 
        result["error"] = str(e)
    
    return result


def log_upload_result(result: Dict[str, Any]) -> None:
    key = result["key"]
    if result["success"]: 
        print(f"  ✅ {key}: {result['rows']: ,} rows ({result['bytes'] / 1024:.1f} KB)")
    elif result["error"] in ["DataFrame is empty", "DataFrame is None"]:
        print(f"  ⏭️  {key}:Skipped ({result['error']})")
    else:
        print(f"  ❌ {key}:{result['error']}")


def export_metadata(
    minio_client: Minio,
    bucket_name: str,
    analysis: Dict,
    product_analysis: Dict,
    supplier_analysis: Dict,
    stats: Dict,
) -> None:
    metadata = {
        "last_updated_utc": datetime.utcnow().isoformat(),
        "bucket_name": bucket_name,
        "analytics_counts": {
            "analysis": len([k for k, v in analysis.items() if v is not None]),
            "product_analysis": len([k for k, v in product_analysis.items() if v is not None]),
            "supplier_analysis": len([k for k, v in supplier_analysis.items() if v is not None]),
        },
        "exported_keys": {
            "analysis":[k for k, v in analysis.items() if v is not None],
            "product_analysis": [k for k, v in product_analysis.items() if v is not None],
            "supplier_analysis":[k for k, v in supplier_analysis.items() if v is not None],
        },
        "export_stats": stats,
    }
    
    try:
        buffer = BytesIO()
        buffer.write(json.dumps(metadata, indent=2, default=str).encode("utf-8"))
        buffer.seek(0)
        
        minio_client.put_object(
            bucket_name,
            "analytics/metadata/export_metadata.json",
            buffer,
            length=len(buffer.getvalue()),
            content_type="application/json"
        )
        
        buffer.close()
        print(f"\n📋 Metadata exported to:analytics/metadata/export_metadata.json")
    except Exception as e: 
        print(f"⚠️  Failed to export metadata: {str(e)}")


def print_summary(stats:Dict) -> None:
    print("\n" + "=" * 60)
    print("📊 EXPORT SUMMARY")
    print("=" * 60)
    print(f"Total Analytics Found: {stats['total_exports']}")
    print(f"Exported:{stats['successful']}")
    print(f"Skipped (not generated): {stats['skipped']}")
    print(f"Failed: {stats['failed']}")
    
    if stats["errors"]:
        print("\nErrors:")
        for error in stats["errors"][:5]: 
            print(f"  - {error['key']}: {error['error']}")
        if len(stats["errors"]) > 5:
            print(f"  ... and {len(stats['errors']) - 5} more")
    print("=" * 60)


def export_analytics_to_minio(
    analysis: Dict[str, DataFrame],
    product_analysis: Dict[str, DataFrame],
    supplier_analysis: Dict[str, DataFrame],
    business_id: Optional[str] = None,
    file_format: str = "parquet",
    parallel: bool = True,
    max_workers: int = 4,
) -> Dict[str, Any]: 
    if file_format not in SUPPORTED_FORMATS: 
        raise ValueError(f"Unsupported format: {file_format}.Supported: {SUPPORTED_FORMATS}")
    
    minio_client = create_minio_client()
    bucket_name = get_bucket_name(business_id)
    stats = {"total_exports": 0, "successful":0, "failed": 0, "skipped": 0, "errors": []}
    
    ensure_bucket_exists(minio_client, bucket_name)
    
    print("\n" + "=" * 60)
    print("📤 EXPORTING ANALYTICS TO MINIO DATA LAKE")
    print("=" * 60)
    print(f"Bucket: {bucket_name}")
    print(f"Format:{file_format}")
    print("=" * 60)
    
    all_analytics = []
    
    for key, df in analysis.items():
        category = get_category_for_key(key)
        all_analytics.append((key, df, category))
    
    for key, df in product_analysis.items():
        category = get_category_for_key(key)
        all_analytics.append((key, df, category if category != "other" else "product_analytics"))
    
    for key, df in supplier_analysis.items():
        category = get_category_for_key(key)
        all_analytics.append((key, df, category if category != "other" else "supplier_analytics"))
    
    print(f"\nTotal analytics keys:{len(all_analytics)}")
    
    results = []
    
    if parallel and len(all_analytics) > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    upload_single, minio_client, bucket_name, key, df, category, file_format
                ): key
                for key, df, category in all_analytics
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                    log_upload_result(result)
                except Exception as e:
                    key = futures[future]
                    print(f"  ❌ {key}:Exception - {str(e)}")
                    stats["failed"] += 1
                    stats["errors"].append({"key": key, "error": str(e)})
    else:
        for key, df, category in all_analytics:
            result = upload_single(minio_client, bucket_name, key, df, category, file_format)
            results.append(result)
            log_upload_result(result)
    
    for result in results: 
        stats["total_exports"] += 1
        if result["success"]:
            stats["successful"] += 1
        elif result["error"] in ["DataFrame is empty", "DataFrame is None"]: 
            stats["skipped"] += 1
        else:
            stats["failed"] += 1
            if result["error"]: 
                stats["errors"].append({"key": result["key"], "error": result["error"]})
    
    export_metadata(minio_client, bucket_name, analysis, product_analysis, supplier_analysis, stats)
    print_summary(stats)
    
    return {
        "bucket": bucket_name,
        "format": file_format,
        "stats":stats,
        "results": results
    }


def list_exported_analytics(
    business_id: Optional[str] = None,
    category: Optional[str] = None
) -> List[Dict[str, str]]:
    minio_client = create_minio_client()
    bucket_name = get_bucket_name(business_id)
    prefix = f"analytics/{category}/" if category else "analytics/"
    
    objects = []
    try:
        for obj in minio_client.list_objects(bucket_name, prefix=prefix, recursive=True):
            objects.append({
                "name":obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
            })
    except Exception as e:
        print(f"Error listing objects: {e}")
    
    return objects