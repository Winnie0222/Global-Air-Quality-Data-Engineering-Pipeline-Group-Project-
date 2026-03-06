# Author: Yoo Xin Wei
import argparse
from streaming_processor import StreamingProcessor
import sys

def main():
    parser = argparse.ArgumentParser(description="Improved Air Quality Streaming Pipeline")
    parser.add_argument("--input-path", required=True, 
                       help="HDFS path to processed data from Task 2")
    parser.add_argument("--output-path", required=True,
                       help="HDFS output path for streaming results")
    
    args = parser.parse_args()
    
    processor = StreamingProcessor()
    processor.run_all_operations(args.input_path, args.output_path)

if __name__ == "__main__":
    main()