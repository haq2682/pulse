"""
Scheduled ML Training - Refactored to Functional Style with Code Reuse

REFACTORED: Uses pure functions and imports from existing training scripts.

Key Changes:
- Removed ScheduledMLTrainer class → pure functions
- Imports from machine-learning/train_all.py
- No duplicate training code - reuses existing training logic
- Functional programming style

Features:
- Scheduled model retraining (daily/weekly/monthly)
- Reuses existing training scripts
- Automated model versioning
"""

import os
import time
import schedule
from datetime import datetime

# Import existing training function - NO DUPLICATION!
try:
    from machine_learning.train_all import main as train_all_models
except ImportError:
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), 'machine-learning'))
    from train_all import main as train_all_models


def train_models_now(bucket_name="pulse-bucket-1"):
    """
    Train all models immediately.
    Simple wrapper around existing train_all function.
    
    Args:
        bucket_name: MinIO bucket name
    """
    print(f"\n🎓 Starting model training at {datetime.now()}")
    print("=" * 60)
    
    try:
        # REUSE existing training logic - no duplication!
        train_all_models(bucket_name)
        
        print("=" * 60)
        print(f"✅ Training completed at {datetime.now()}")
        return True
    except Exception as e:
        print(f"❌ Training failed: {e}")
        return False


def schedule_training(schedule_type="weekly", day_of_week="sunday", 
                     hour=2, bucket_name="pulse-bucket-1"):
    """
    Schedule model training.
    Pure function that sets up scheduler.
    
    Args:
        schedule_type: "daily", "weekly", or "monthly"
        day_of_week: Day for weekly training (e.g., "sunday")
        hour: Hour of day to run (0-23)
        bucket_name: MinIO bucket name
    """
    print(f"📅 Scheduling {schedule_type} training")
    print(f"   Time: {hour}:00")
    if schedule_type == "weekly":
        print(f"   Day: {day_of_week}")
    
    # Define training job
    def job():
        train_models_now(bucket_name)
    
    # Schedule based on type
    if schedule_type == "daily":
        schedule.every().day.at(f"{hour:02d}:00").do(job)
    elif schedule_type == "weekly":
        getattr(schedule.every(), day_of_week).at(f"{hour:02d}:00").do(job)
    elif schedule_type == "monthly":
        schedule.every(30).days.at(f"{hour:02d}:00").do(job)
    else:
        raise ValueError(f"Unknown schedule type: {schedule_type}")
    
    print(f"✅ Training scheduled")
    
    # Run scheduler loop
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\n⚠️  Scheduler stopped")


def main():
    """
    Main entry point for scheduled training.
    Pure function for orchestration.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Schedule ML Model Training")
    parser.add_argument("--schedule", choices=["daily", "weekly", "monthly"],
                       default="weekly", help="Training schedule")
    parser.add_argument("--day", default="sunday", help="Day of week for weekly training")
    parser.add_argument("--hour", type=int, default=2, help="Hour of day (0-23)")
    parser.add_argument("--bucket-name", default="pulse-bucket-1", help="MinIO bucket")
    parser.add_argument("--train-now", action="store_true", help="Train immediately")
    
    args = parser.parse_args()
    
    print("🚀 ML Training Scheduler (Functional Style)")
    print("=" * 60)
    
    if args.train_now:
        # Train immediately
        train_models_now(args.bucket_name)
    else:
        # Schedule training
        schedule_training(
            schedule_type=args.schedule,
            day_of_week=args.day,
            hour=args.hour,
            bucket_name=args.bucket_name
        )


if __name__ == "__main__":
    main()
