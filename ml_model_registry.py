"""
ML Model Registry
Manages ML model lifecycle, versioning, and metadata
"""

import os
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional


class MLModelRegistry:
    """
    Model registry for tracking and managing ML model versions
    """
    
    def __init__(self, bucket_name="pulse-bucket-1"):
        """
        Initialize model registry
        
        Args:
            bucket_name: MinIO bucket name
        """
        self.bucket_name = bucket_name
        self.models_base_path = f"s3a://{bucket_name}/models"
        self.metadata_file = f"s3a://{bucket_name}/models/registry.json"
        
        print(f"✅ MLModelRegistry initialized")
        print(f"   Bucket: {bucket_name}")
        print(f"   Models path: {self.models_base_path}")
    
    def list_models(self, model_category="general") -> List[str]:
        """
        List all available models
        
        Args:
            model_category: 'general' or 'specific'
        
        Returns:
            List of model names
        """
        # In production, this would query MinIO
        # For now, return known models
        
        if model_category == "general":
            models = [
                # Classification
                "cart_abandonment",
                "customer_churn",
                "customer_segments",
                "payment_success",
                "review_sentiment",
                "stock_status",
                # Regression
                "aov",
                "clv",
                "restock_quantity",
                "revenue_forecast",
                "safety_stock",
                "session_conversion",
                "stockout_probability",
                # Clustering
                "customer_segment",
                "geo_cluster",
                "session_behavior",
                "supplier_performance",
            ]
        else:  # specific
            models = [
                # Classification
                "fulfillment_risk",
                "product_bundling",
                # Regression
                "campaign_roi",
                "delivery_time",
                "demand_forecast",
                "price_optimization",
                # Clustering
                "product_affinity",
                "product_lifecycle",
            ]
        
        return models
    
    def list_model_versions(self, model_name: str, model_type: str = "classification") -> List[str]:
        """
        List all versions of a specific model
        
        Args:
            model_name: Name of the model
            model_type: Type of model
        
        Returns:
            List of version timestamps
        """
        # In production, this would:
        # 1. List directories in MinIO under models/general/{model_type}/{model_name}/
        # 2. Parse version timestamps
        # 3. Return sorted list (newest first)
        
        # Mock versions
        versions = [
            "20260215_203000",
            "20260208_203000",
            "20260201_203000",
        ]
        
        return versions
    
    def get_latest_version(self, model_name: str, model_type: str = "classification") -> str:
        """
        Get latest version timestamp for a model
        
        Args:
            model_name: Name of the model
            model_type: Type of model
        
        Returns:
            Latest version timestamp
        """
        versions = self.list_model_versions(model_name, model_type)
        return versions[0] if versions else None
    
    def load_model_metadata(self, model_name: str, version: str = "latest") -> Dict:
        """
        Load model metadata
        
        Args:
            model_name: Name of the model
            version: Version timestamp or 'latest'
        
        Returns:
            Model metadata dictionary
        """
        # In production, this would load metadata from MinIO
        # metadata.json stored alongside model
        
        if version == "latest":
            version = self.get_latest_version(model_name)
        
        # Mock metadata
        metadata = {
            "model_name": model_name,
            "version": version,
            "trained_at": datetime.now().isoformat(),
            "training_records": 50000,
            "metrics": {
                "accuracy": 0.92,
                "precision": 0.89,
                "recall": 0.91,
                "f1_score": 0.90
            },
            "features": [
                "feature1", "feature2", "feature3"
            ],
            "model_type": "RandomForestClassifier",
            "hyperparameters": {
                "n_estimators": 100,
                "max_depth": 10
            }
        }
        
        return metadata
    
    def save_model_metadata(self, model_name: str, version: str, metadata: Dict):
        """
        Save model metadata
        
        Args:
            model_name: Name of the model
            version: Version timestamp
            metadata: Metadata dictionary to save
        """
        # In production, this would:
        # 1. Serialize metadata to JSON
        # 2. Save to MinIO alongside model
        # 3. Update registry index
        
        print(f"   💾 Saving metadata for {model_name} v{version}")
        
        # Mock save
        metadata_path = f"{self.models_base_path}/general/{model_name}/{version}/metadata.json"
        print(f"   ✅ Metadata saved: {metadata_path}")
    
    def get_model_performance_history(self, model_name: str) -> List[Dict]:
        """
        Get performance history across versions
        
        Args:
            model_name: Name of the model
        
        Returns:
            List of performance metrics by version
        """
        # In production, this would query metadata for all versions
        
        history = [
            {
                "version": "20260215_203000",
                "trained_at": "2026-02-15T20:30:00",
                "accuracy": 0.92,
                "records": 50000
            },
            {
                "version": "20260208_203000",
                "trained_at": "2026-02-08T20:30:00",
                "accuracy": 0.90,
                "records": 48000
            },
            {
                "version": "20260201_203000",
                "trained_at": "2026-02-01T20:30:00",
                "accuracy": 0.88,
                "records": 45000
            }
        ]
        
        return history
    
    def compare_versions(self, model_name: str, version1: str, version2: str) -> Dict:
        """
        Compare two versions of a model
        
        Args:
            model_name: Name of the model
            version1: First version
            version2: Second version
        
        Returns:
            Comparison dictionary
        """
        meta1 = self.load_model_metadata(model_name, version1)
        meta2 = self.load_model_metadata(model_name, version2)
        
        comparison = {
            "model_name": model_name,
            "version1": {
                "version": version1,
                "metrics": meta1.get("metrics", {})
            },
            "version2": {
                "version": version2,
                "metrics": meta2.get("metrics", {})
            },
            "improvements": {}
        }
        
        # Calculate improvements
        for metric in ["accuracy", "precision", "recall", "f1_score"]:
            v1_val = meta1.get("metrics", {}).get(metric, 0)
            v2_val = meta2.get("metrics", {}).get(metric, 0)
            improvement = v2_val - v1_val
            comparison["improvements"][metric] = {
                "absolute": improvement,
                "relative": (improvement / v1_val * 100) if v1_val > 0 else 0
            }
        
        return comparison
    
    def archive_old_versions(self, model_name: str, keep_latest: int = 3):
        """
        Archive old model versions
        
        Args:
            model_name: Name of the model
            keep_latest: Number of latest versions to keep
        """
        versions = self.list_model_versions(model_name)
        
        if len(versions) <= keep_latest:
            print(f"   ℹ️  No archiving needed for {model_name} (only {len(versions)} versions)")
            return
        
        versions_to_archive = versions[keep_latest:]
        
        print(f"   📦 Archiving {len(versions_to_archive)} old versions of {model_name}")
        
        for version in versions_to_archive:
            # In production, this would:
            # 1. Move model from models/ to models/archive/
            # 2. Update registry
            print(f"      Archived: {version}")
        
        print(f"   ✅ Archived {len(versions_to_archive)} versions")


def main():
    """Example usage"""
    print("\n" + "="*70)
    print("ML MODEL REGISTRY - EXAMPLE USAGE")
    print("="*70)
    
    registry = MLModelRegistry(bucket_name="pulse-bucket-1")
    
    # List models
    print("\n📋 General Models:")
    models = registry.list_models("general")
    for model in models[:5]:
        print(f"   - {model}")
    print(f"   ... and {len(models) - 5} more")
    
    # List versions
    print("\n📦 Versions of 'customer_churn':")
    versions = registry.list_model_versions("customer_churn")
    for version in versions:
        print(f"   - {version}")
    
    # Load metadata
    print("\n📊 Metadata for 'customer_churn' (latest):")
    metadata = registry.load_model_metadata("customer_churn", "latest")
    print(f"   Trained at: {metadata['trained_at']}")
    print(f"   Records: {metadata['training_records']:,}")
    print(f"   Accuracy: {metadata['metrics']['accuracy']:.2%}")
    
    # Performance history
    print("\n📈 Performance History:")
    history = registry.get_model_performance_history("customer_churn")
    for entry in history:
        print(f"   {entry['version']}: Accuracy {entry['accuracy']:.2%}")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    main()
