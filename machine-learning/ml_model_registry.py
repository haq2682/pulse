"""
ML Model Registry - Refactored to Functional Style

REFACTORED: Simple functions for model management, no class needed.

Key Changes:
- Removed MLModelRegistry class → simple pure functions
- Minimal wrapper around MinIO operations
- Functional programming style

Features:
- Load/save models from MinIO
- Simple version management
"""

import os
import joblib
from datetime import datetime
from minio import Minio


def get_minio_client():
    """
    Get MinIO client.
    Simple factory function.
    """
    return Minio(
        "localhost:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False
    )


def save_model(model, model_name, bucket_name="pulse-bucket-1", 
               model_type="general", metadata=None):
    """
    Save model to MinIO with versioning.
    Pure function for saving.
    
    Args:
        model: Model object to save
        model_name: Name of the model
        bucket_name: MinIO bucket
        model_type: "general" or "specific"
        metadata: Optional metadata dict
    """
    client = get_minio_client()
    
    # Create version timestamp
    version = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save locally first
    local_path = f"/tmp/{model_name}_{version}.pkl"
    joblib.dump(model, local_path)
    
    # Upload to MinIO
    object_path = f"models/{model_type}/{model_name}/{version}.pkl"
    client.fput_object(bucket_name, object_path, local_path)
    
    # Also save as "latest"
    latest_path = f"models/{model_type}/{model_name}/latest.pkl"
    client.fput_object(bucket_name, latest_path, local_path)
    
    print(f"✅ Saved model: {model_name} (version: {version})")
    
    # Cleanup
    os.remove(local_path)
    
    return version


def load_model(model_name, bucket_name="pulse-bucket-1",
               model_type="general", version="latest"):
    """
    Load model from MinIO.
    Pure function for loading.
    
    Args:
        model_name: Name of the model
        bucket_name: MinIO bucket
        model_type: "general" or "specific"
        version: Version to load or "latest"
        
    Returns:
        Loaded model object
    """
    client = get_minio_client()
    
    # Determine object path
    if version == "latest":
        object_path = f"models/{model_type}/{model_name}/latest.pkl"
    else:
        object_path = f"models/{model_type}/{model_name}/{version}.pkl"
    
    # Download and load
    local_path = f"/tmp/{model_name}_loaded.pkl"
    
    try:
        client.fget_object(bucket_name, object_path, local_path)
        model = joblib.load(local_path)
        print(f"✅ Loaded model: {model_name} (version: {version})")
        os.remove(local_path)
        return model
    except Exception as e:
        print(f"❌ Failed to load model {model_name}: {e}")
        return None


def list_models(bucket_name="pulse-bucket-1", model_type="general"):
    """
    List available models.
    Pure function for listing.
    
    Args:
        bucket_name: MinIO bucket
        model_type: "general" or "specific"
        
    Returns:
        List of model names
    """
    client = get_minio_client()
    prefix = f"models/{model_type}/"
    
    models = set()
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    
    for obj in objects:
        # Extract model name from path
        parts = obj.object_name.split('/')
        if len(parts) >= 3:
            model_name = parts[2]
            models.add(model_name)
    
    return list(models)


def main():
    """
    Main entry point for model registry operations.
    Pure function for CLI.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="ML Model Registry")
    parser.add_argument("action", choices=["list", "save", "load"],
                       help="Action to perform")
    parser.add_argument("--model-name", help="Model name")
    parser.add_argument("--bucket-name", default="pulse-bucket-1", help="MinIO bucket")
    parser.add_argument("--model-type", choices=["general", "specific"],
                       default="general", help="Model type")
    
    args = parser.parse_args()
    
    if args.action == "list":
        models = list_models(args.bucket_name, args.model_type)
        print(f"\n📋 Available {args.model_type} models:")
        for model in models:
            print(f"   - {model}")
    elif args.action == "load":
        if not args.model_name:
            print("❌ --model-name required for load")
            return
        model = load_model(args.model_name, args.bucket_name, args.model_type)
        if model:
            print(f"✅ Model loaded successfully")
    else:
        print("❌ Save action requires programmatic usage")


if __name__ == "__main__":
    main()
