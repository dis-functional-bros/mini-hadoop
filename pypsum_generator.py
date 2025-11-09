#!/usr/bin/env python3
"""
Large Text File Generator using Pypsum
Generates large text files with Lorem Ipsum style text for testing distributed systems.
"""

import argparse
import os
import sys
from datetime import datetime

try:
    from pypsum import generate_paragraphs, generate_sentences
except ImportError:
    print("Error: pypsum is required. Install it with: pip install pypsum")
    sys.exit(1)

class PypsumTextGenerator:
    def __init__(self):
        self.generated_chars = 0
        
    def generate_text(self, target_size_mb, paragraphs_per_chunk=10, sentence_variation=True):
        """Generate text content targeting a specific file size in MB using pypsum."""
        target_size_bytes = target_size_mb * 1024 * 1024
        content = []
        self.generated_chars = 0
        
        print(f"Generating {target_size_mb}MB of text using pypsum...")
        print(f"Target size: {target_size_bytes:,} bytes")
        
        chunk_count = 0
        
        while self.generated_chars < target_size_bytes:
            # Generate paragraphs in chunks for better performance
            paragraphs = generate_paragraphs(
                paragraphs_per_chunk,
                sentence_variation=sentence_variation
            )
            
            for paragraph in paragraphs:
                content.append(paragraph)
                content.append('\n\n')  # Add paragraph separation
                self.generated_chars += len(paragraph) + 2  # +2 for '\n\n'
                
                # Check if we've reached target size
                if self.generated_chars >= target_size_bytes:
                    break
            
            chunk_count += 1
            
            # Progress indicator
            if chunk_count % 5 == 0:
                progress = (self.generated_chars / target_size_bytes) * 100
                current_mb = self.generated_chars / (1024 * 1024)
                print(f"Progress: {progress:.1f}% ({current_mb:.2f}MB / {target_size_mb}MB)")
                
                # Early break if we've significantly exceeded target
                if self.generated_chars >= target_size_bytes:
                    break
        
        return ''.join(content)
    
    def generate_sentences_only(self, target_size_mb, sentences_per_chunk=50):
        """Generate text using only sentences (no paragraph structure)."""
        target_size_bytes = target_size_mb * 1024 * 1024
        content = []
        self.generated_chars = 0
        
        print(f"Generating {target_size_mb}MB of sentence-based text...")
        
        while self.generated_chars < target_size_bytes:
            sentences = generate_sentences(sentences_per_chunk)
            text_chunk = ' '.join(sentences) + ' '
            
            content.append(text_chunk)
            self.generated_chars += len(text_chunk)
            
            # Progress indicator
            if len(content) % 20 == 0:
                progress = (self.generated_chars / target_size_bytes) * 100
                current_mb = self.generated_chars / (1024 * 1024)
                print(f"Progress: {progress:.1f}% ({current_mb:.2f}MB / {target_size_mb}MB)")
                
                if self.generated_chars >= target_size_bytes:
                    break
        
        return ''.join(content)
    
    def generate_mixed_content(self, target_size_mb, paragraph_ratio=0.7):
        """Generate mixed content with paragraphs and sentence blocks."""
        target_size_bytes = target_size_mb * 1024 * 1024
        content = []
        self.generated_chars = 0
        
        print(f"Generating {target_size_mb}MB of mixed content...")
        
        while self.generated_chars < target_size_bytes:
            # Alternate between paragraphs and sentence blocks
            if random.random() < paragraph_ratio:
                # Generate paragraph
                paragraphs = generate_paragraphs(1)
                text_chunk = paragraphs[0] + '\n\n'
            else:
                # Generate sentence block
                sentences = generate_sentences(random.randint(3, 8))
                text_chunk = ' '.join(sentences) + '\n\n'
            
            content.append(text_chunk)
            self.generated_chars += len(text_chunk)
            
            # Progress indicator
            if len(content) % 50 == 0:
                progress = (self.generated_chars / target_size_bytes) * 100
                current_mb = self.generated_chars / (1024 * 1024)
                print(f"Progress: {progress:.1f}% ({current_mb:.2f}MB / {target_size_mb}MB)")
                
                if self.generated_chars >= target_size_bytes:
                    break
        
        return ''.join(content)

def format_file_size(bytes_size):
    """Format file size in human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"

def main():
    parser = argparse.ArgumentParser(
        description='Generate large text files using pypsum Lorem Ipsum generator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Generate 10MB file with default settings
  python pypsum_generator.py --size 10 --output data.txt

  # Generate 50MB file with sentence-only content
  python pypsum_generator.py --size 50 --output sentences.txt --mode sentences

  # Generate 100MB file with custom paragraph chunks
  python pypsum_generator.py --size 100 --output large.txt --paragraphs 20 --no-variation

  # Generate mixed content file
  python pypsum_generator.py --size 25 --output mixed.txt --mode mixed --paragraph-ratio 0.8
        '''
    )
    
    # Required arguments
    parser.add_argument('--size', type=float, required=True, 
                       help='Size of the output file in MB')
    parser.add_argument('--output', type=str, required=True, 
                       help='Output file path')
    
    # Generation mode
    parser.add_argument('--mode', choices=['paragraphs', 'sentences', 'mixed'], 
                       default='paragraphs', 
                       help='Text generation mode (default: paragraphs)')
    
    # Generation parameters
    parser.add_argument('--paragraphs', type=int, default=10,
                       help='Number of paragraphs per chunk (default: 10)')
    parser.add_argument('--sentences', type=int, default=50,
                       help='Number of sentences per chunk for sentence mode (default: 50)')
    parser.add_argument('--paragraph-ratio', type=float, default=0.7,
                       help='Ratio of paragraphs in mixed mode (default: 0.7)')
    parser.add_argument('--no-variation', action='store_true',
                       help='Disable sentence variation in paragraphs')
    
    # File options
    parser.add_argument('--overwrite', action='store_true',
                       help='Overwrite existing file without confirmation')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed generation information')
    
    args = parser.parse_args()
    
    # Validation
    if args.size <= 0:
        print("Error: Size must be positive")
        sys.exit(1)
    
    if args.paragraphs <= 0:
        print("Error: Paragraphs must be positive")
        sys.exit(1)
    
    if args.sentences <= 0:
        print("Error: Sentences must be positive")
        sys.exit(1)
    
    if not 0 <= args.paragraph_ratio <= 1:
        print("Error: Paragraph ratio must be between 0 and 1")
        sys.exit(1)
    
    # Check if file exists
    if os.path.exists(args.output) and not args.overwrite:
        response = input(f"File {args.output} already exists. Overwrite? (y/n): ")
        if response.lower() != 'y':
            print("Operation cancelled.")
            return
    
    generator = PypsumTextGenerator()
    
    try:
        start_time = datetime.now()
        
        if args.verbose:
            print(f"Generation Parameters:")
            print(f"  Target size: {args.size} MB")
            print(f"  Output file: {args.output}")
            print(f"  Mode: {args.mode}")
            print(f"  Sentence variation: {not args.no_variation}")
            if args.mode == 'paragraphs':
                print(f"  Paragraphs per chunk: {args.paragraphs}")
            elif args.mode == 'sentences':
                print(f"  Sentences per chunk: {args.sentences}")
            elif args.mode == 'mixed':
                print(f"  Paragraph ratio: {args.paragraph_ratio}")
            print("-" * 40)
        
        # Generate content based on mode
        if args.mode == 'paragraphs':
            content = generator.generate_text(
                args.size, 
                paragraphs_per_chunk=args.paragraphs,
                sentence_variation=not args.no_variation
            )
        elif args.mode == 'sentences':
            content = generator.generate_sentences_only(
                args.size,
                sentences_per_chunk=args.sentences
            )
        else:  # mixed mode
            content = generator.generate_mixed_content(
                args.size,
                paragraph_ratio=args.paragraph_ratio
            )
        
        # Write to file
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(content)
        
        # Verify and report
        actual_size_bytes = os.path.getsize(args.output)
        actual_size_mb = actual_size_bytes / (1024 * 1024)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n" + "="*50)
        print(f"FILE GENERATION COMPLETE")
        print(f"="*50)
        print(f"Output file: {args.output}")
        print(f"Target size: {args.size} MB")
        print(f"Actual size: {format_file_size(actual_size_bytes)}")
        print(f"Generation time: {duration:.2f} seconds")
        print(f"Write speed: {actual_size_mb/duration:.2f} MB/s")
        print(f"Characters generated: {generator.generated_chars:,}")
        
        if args.verbose:
            lines = content.count('\n')
            paragraphs = content.count('\n\n') + 1
            print(f"Lines: {lines:,}")
            print(f"Paragraphs: {paragraphs:,}")
        
    except KeyboardInterrupt:
        print("\nGeneration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error generating file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import random
    main()