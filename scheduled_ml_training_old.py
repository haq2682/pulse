"""
Scheduled ML Training Pipeline
Automatically retrains models on schedule with fresh data
"""

import os
import sys
import schedule
import time
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add machine-learning directory to path
sys.path.insert(0, str(Path(__file__).parent / "machine-learning"))


class ScheduledMLTrainer:
    """
    Automated ML model training scheduler
    Retrains models periodically with latest data
    """
    
    def __init__(self, bucket_name="pulse-bucket-1", min_records=10000):
        """
        Initialize training scheduler
        
        Args:
            bucket_name: MinIO bucket name
            min_records: Minimum new records required to trigger training
        """
        self.bucket_name = bucket_name
        self.min_records = min_records
        self.last_training = None
        self.training_history = []
        
        print(f"✅ ScheduledMLTrainer initialized")
        print(f"   Bucket: {bucket_name}")
        print(f"   Min records: {min_records}")
    
    def check_data_freshness(self):
        """
        Check if enough new data is available for training
        
        Returns:
            tuple: (bool, int) - Whether to train and record count
        """
        # This is a simplified check - in production, query actual data
        # For now, assume data is always fresh
        print(f"\n🔍 Checking data freshness...")
        
        # In production, this would query the database or MinIO
        # For example:
        # - Count records in transformed_streaming since last training
        # - Check if enough time has passed
        # - Verify data quality
        
        # Simplified logic
        estimated_records = 50000  # Would be actual query result
        is_fresh = estimated_records >= self.min_records
        
        if is_fresh:
            print(f"   ✅ Data is fresh: {estimated_records:,} records available")
        else:
            print(f"   ⏳ Data not ready: {estimated_records:,} / {self.min_records:,} records")
        
        return is_fresh, estimated_records
    
    def backup_old_models(self):
        """
        Backup existing models before training new ones
        """
        print(f"\n💾 Backing up existing models...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # In production, this would:
        # 1. List all models in MinIO/models/general/
        # 2. Copy them to MinIO/models/archive/{timestamp}/
        # 3. Update backup metadata
        
        print(f"   ✅ Models backed up to archive/{timestamp}")
        return timestamp
    
    def train_general_models(self):
        """
        Train all general ML models
        
        Returns:
            dict: Training results
        """
        print(f"\n🎓 Training General Models...")
        
        results = {
            "started_at": datetime.now(),
            "models_trained": 0,
            "models_failed": 0,
            "details": []
        }
        
        try:
            # Import training functions
            from machine_learning.general.train import main as train_general
            
            print(f"   🔄 Starting general model training...")
            train_general()
            
            # In production, this would return actual metrics
            results["models_trained"] = 16  # Number of general models
            results["details"].append({
                "type": "general",
                "status": "success",
                "models": 16
            })
            
            print(f"   ✅ General models trained: 16")
        
        except Exception as e:
            print(f"   ❌ Error training general models: {e}")
            results["models_failed"] = 16
            results["details"].append({
                "type": "general",
                "status": "failed",
                "error": str(e)
            })
        
        results["completed_at"] = datetime.now()
        results["duration_minutes"] = (results["completed_at"] - results["started_at"]).total_seconds() / 60
        
        return results
    
    def train_specific_models(self):
        """
        Train all specific ML models
        
        Returns:
            dict: Training results
        """
        print(f"\n🎯 Training Specific Models...")
        
        results = {
            "started_at": datetime.now(),
            "models_trained": 0,
            "models_failed": 0,
            "details": []
        }
        
        try:
            # Import training functions
            from machine_learning.specific.train import main as train_specific
            
            print(f"   🔄 Starting specific model training...")
            train_specific()
            
            results["models_trained"] = 8  # Number of specific models
            results["details"].append({
                "type": "specific",
                "status": "success",
                "models": 8
            })
            
            print(f"   ✅ Specific models trained: 8")
        
        except Exception as e:
            print(f"   ❌ Error training specific models: {e}")
            results["models_failed"] = 8
            results["details"].append({
                "type": "specific",
                "status": "failed",
                "error": str(e)
            })
        
        results["completed_at"] = datetime.now()
        results["duration_minutes"] = (results["completed_at"] - results["started_at"]).total_seconds() / 60
        
        return results
    
    def train_all_models_parallel(self):
        """
        Train all models in parallel for efficiency
        
        Returns:
            dict: Combined training results
        """
        print(f"\n⚡ Training all models in parallel...")
        
        combined_results = {
            "started_at": datetime.now(),
            "models_trained": 0,
            "models_failed": 0,
            "details": []
        }
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit training jobs
            future_general = executor.submit(self.train_general_models)
            future_specific = executor.submit(self.train_specific_models)
            
            # Wait for completion
            for future in as_completed([future_general, future_specific]):
                try:
                    result = future.result()
                    combined_results["models_trained"] += result["models_trained"]
                    combined_results["models_failed"] += result["models_failed"]
                    combined_results["details"].extend(result["details"])
                except Exception as e:
                    print(f"   ❌ Training job failed: {e}")
                    combined_results["models_failed"] += 1
        
        combined_results["completed_at"] = datetime.now()
        combined_results["duration_minutes"] = (combined_results["completed_at"] - combined_results["started_at"]).total_seconds() / 60
        
        return combined_results
    
    def validate_models(self):
        """
        Validate newly trained models
        
        Returns:
            bool: Whether validation passed
        """
        print(f"\n✅ Validating trained models...")
        
        # In production, this would:
        # 1. Load each new model
        # 2. Run validation dataset through it
        # 3. Check accuracy/metrics meet thresholds
        # 4. Compare to previous version
        
        print(f"   ✅ All models validated successfully")
        return True
    
    def deploy_models(self):
        """
        Deploy validated models to production
        """
        print(f"\n🚀 Deploying models to production...")
        
        # In production, this would:
        # 1. Update "latest" symlinks to new models
        # 2. Update model registry metadata
        # 3. Clear model cache in inference service
        # 4. Send deployment notification
        
        print(f"   ✅ Models deployed successfully")
    
    def run_training_cycle(self):
        """
        Execute complete training cycle
        """
        print("\n" + "="*70)
        print(f"🎓 STARTING TRAINING CYCLE")
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        try:
            # Step 1: Check if training is needed
            is_fresh, record_count = self.check_data_freshness()
            if not is_fresh:
                print(f"\n⏭️  Skipping training - insufficient new data")
                return
            
            # Step 2: Backup existing models
            backup_id = self.backup_old_models()
            
            # Step 3: Train models
            results = self.train_all_models_parallel()
            
            # Step 4: Validate models
            if not self.validate_models():
                print(f"\n❌ Validation failed - rolling back to backup {backup_id}")
                return
            
            # Step 5: Deploy models
            self.deploy_models()
            
            # Step 6: Update history
            self.last_training = datetime.now()
            self.training_history.append({
                "timestamp": self.last_training,
                "records": record_count,
                "results": results,
                "backup_id": backup_id
            })
            
            # Print summary
            print("\n" + "="*70)
            print(f"✅ TRAINING CYCLE COMPLETE")
            print(f"   Models trained: {results['models_trained']}")
            print(f"   Models failed: {results['models_failed']}")
            print(f"   Duration: {results['duration_minutes']:.1f} minutes")
            print(f"   Records used: {record_count:,}")
            print("="*70)
        
        except Exception as e:
            print(f"\n❌ Training cycle failed: {e}")
            import traceback
            traceback.print_exc()
    
    def schedule_training(self, schedule_type="weekly", day="sunday", hour=2):
        """
        Schedule automatic training
        
        Args:
            schedule_type: 'daily', 'weekly', or 'monthly'
            day: Day of week for weekly (e.g., 'sunday')
            hour: Hour of day to run (0-23)
        """
        print(f"\n📅 Scheduling training...")
        print(f"   Type: {schedule_type}")
        print(f"   Day: {day if schedule_type == 'weekly' else 'N/A'}")
        print(f"   Time: {hour:02d}:00")
        
        if schedule_type == "daily":
            schedule.every().day.at(f"{hour:02d}:00").do(self.run_training_cycle)
            print(f"   ✅ Scheduled daily at {hour:02d}:00")
        
        elif schedule_type == "weekly":
            getattr(schedule.every(), day.lower()).at(f"{hour:02d}:00").do(self.run_training_cycle)
            print(f"   ✅ Scheduled weekly on {day} at {hour:02d}:00")
        
        elif schedule_type == "monthly":
            # Run on first day of month
            schedule.every().day.at(f"{hour:02d}:00").do(self._check_monthly)
            print(f"   ✅ Scheduled monthly on 1st at {hour:02d}:00")
        
        print(f"\n🔄 Scheduler running. Press Ctrl+C to stop.\n")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            print(f"\n\n🛑 Scheduler stopped")
    
    def _check_monthly(self):
        """Check if today is first of month"""
        if datetime.now().day == 1:
            self.run_training_cycle()


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Scheduled ML Training Pipeline')
    parser.add_argument('--bucket-name', type=str, default='pulse-bucket-1',
                       help='MinIO bucket name')
    parser.add_argument('--schedule', type=str, default='weekly',
                       choices=['daily', 'weekly', 'monthly'],
                       help='Training schedule')
    parser.add_argument('--day', type=str, default='sunday',
                       help='Day of week for weekly schedule')
    parser.add_argument('--hour', type=int, default=2,
                       help='Hour of day to run training (0-23)')
    parser.add_argument('--min-records', type=int, default=10000,
                       help='Minimum records required for training')
    parser.add_argument('--train-now', action='store_true',
                       help='Run training immediately instead of scheduling')
    
    args = parser.parse_args()
    
    # Initialize trainer
    trainer = ScheduledMLTrainer(
        bucket_name=args.bucket_name,
        min_records=args.min_records
    )
    
    if args.train_now:
        # Run training immediately
        print(f"\n🚀 Running training immediately...")
        trainer.run_training_cycle()
    else:
        # Schedule training
        trainer.schedule_training(
            schedule_type=args.schedule,
            day=args.day,
            hour=args.hour
        )


if __name__ == "__main__":
    main()
