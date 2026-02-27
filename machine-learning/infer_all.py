import argparse
from general.infer import main as general_infer
from specific.infer import main as specific_infer

def main():
    parser = argparse.ArgumentParser(description='Run inference on general and specific models')
    parser.add_argument('--bucket-name', type=str, required=True, help='S3 bucket name')
    parser.add_argument('--first-batch', action='store_true', help='First ever batch — train specific models before inference')
    args = parser.parse_args()
    
    general_infer(args.bucket_name)
    specific_infer(args.bucket_name, first_batch=args.first_batch)
    
if __name__ == "__main__":
    main()