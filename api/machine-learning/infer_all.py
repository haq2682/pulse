import argparse
from general.infer import main as general_infer
from specific.infer import main as specific_infer

def main():
    parser = argparse.ArgumentParser(description='Run inference on general and specific models')
    parser.add_argument('--bucket-name', type=str, required=True, help='S3 bucket name')
    args = parser.parse_args()
    
    general_infer(args.bucket_name)
    specific_infer(args.bucket_name)
    
if __name__ == "__main__":
    main()