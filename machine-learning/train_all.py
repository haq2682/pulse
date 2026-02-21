import argparse
from general.train import main as general_train
from specific.train import main as specific_train

def main():
    parser = argparse.ArgumentParser(description='Run training on general and specific models')
    parser.add_argument('--bucket-name', type=str, required=True, help='S3 bucket name')
    args = parser.parse_args()
    
    general_train(args.bucket_name)
    specific_train(args.bucket_name)
    
if __name__ == "__main__":
    main()