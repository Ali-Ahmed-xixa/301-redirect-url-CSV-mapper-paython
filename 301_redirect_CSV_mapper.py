import csv
import os
from difflib import SequenceMatcher

def detect_encoding(file_path):
    """Try to detect file encoding"""
    import chardet
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

def normalize_url(url):
    """Ensure URL starts with '/' and normalize for comparison"""
    # Ensure URL starts with '/'
    if not url.startswith('/'):
        url = '/' + url
    # Convert to lowercase for case-insensitive comparison
    return url.lower()

def find_best_match(new_url, old_urls):
    """Find the best matching old URL for a new URL"""
    normalized_new = normalize_url(new_url)
    best_match = None
    best_score = 0
    
    for old_url in old_urls:
        normalized_old = normalize_url(old_url)
        # Calculate similarity score using the complete URLs
        score = SequenceMatcher(None, normalized_new, normalized_old).ratio()
        
        if score > best_score:
            best_score = score
            best_match = old_url  # Return the original URL, not normalized
            
    return (best_match, best_score) if best_score > 0.6 else (None, 0)

def process_batch(batch, old_urls, batch_number, output_dir):
    """Process a batch of URLs and save to separate CSV"""
    matched_results = []
    unmatched_results = []
    
    for i, new_url in enumerate(batch, 1):
        # Ensure new URL starts with '/'
        if not new_url.startswith('/'):
            new_url = '/' + new_url
            
        matched_url, score = find_best_match(new_url, old_urls)
        
        if matched_url:
            # Ensure matched URL starts with '/'
            if not matched_url.startswith('/'):
                matched_url = '/' + matched_url
            matched_results.append([new_url, matched_url])
            print(f"Batch {batch_number}: Processing URL {i}/{len(batch)}")
            print(f"URL: {new_url}")
            print(f"✅ MATCH FOUND (score: {score:.2f}): {matched_url}")
        else:
            unmatched_results.append([new_url])
            print(f"Batch {batch_number}: Processing URL {i}/{len(batch)}")
            print(f"URL: {new_url}")
            print("❌ NO MATCH FOUND")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Write matched results to CSV only if there are matches
    if matched_results:
        matched_output_path = os.path.join(output_dir, f'matched_urls_batch_{batch_number}.csv')
        with open(matched_output_path, mode='w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['cToUrl', 'cFromUrl'])  # Updated header
            writer.writerows(matched_results)
    
    # Write unmatched results to separate CSV
    if unmatched_results:
        unmatched_output_path = os.path.join(output_dir, f'unmatched_urls_batch_{batch_number}.csv')
        with open(unmatched_output_path, mode='w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['cToUrl'])  # Header for unmatched URLs
            writer.writerows(unmatched_results)
    
    return len(matched_results), len(unmatched_results)

def map_urls_in_batches(old_csv_path, new_csv_path, output_dir, batch_size=1000):
    """Map URLs from old CSV to new CSV in batches"""
    # Detect file encodings
    old_encoding = detect_encoding(old_csv_path)
    new_encoding = detect_encoding(new_csv_path)
    
    print(f"Detected encoding for old CSV: {old_encoding}")
    print(f"Detected encoding for new CSV: {new_encoding}")
    
    # Read old URLs
    with open(old_csv_path, mode='r', encoding=old_encoding) as f:
        reader = csv.reader(f)
        old_urls = [row[0] for row in reader if row]  # Assuming URLs are in first column
    
    # Read new URLs and process in batches
    with open(new_csv_path, mode='r', encoding=new_encoding) as f:
        reader = csv.reader(f)
        new_urls = [row[0] for row in reader if row]
    
    total_matched = 0
    total_unmatched = 0
    batch_count = (len(new_urls) // batch_size) + 1
    
    print(f"\nStarting URL mapping process...")
    print(f"Total URLs to process: {len(new_urls)}")
    print(f"Batch size: {batch_size}")
    print(f"Total batches: {batch_count}\n")
    
    for batch_num in range(batch_count):
        start_idx = batch_num * batch_size
        end_idx = start_idx + batch_size
        current_batch = new_urls[start_idx:end_idx]
        
        print(f"\nProcessing batch {batch_num + 1}/{batch_count} (URLs {start_idx + 1}-{end_idx})")
        matched, unmatched = process_batch(current_batch, old_urls, batch_num + 1, output_dir)
        total_matched += matched
        total_unmatched += unmatched
    
    print(f"\nCompleted! Processed {len(new_urls)} URLs across {batch_count} batches")
    print(f"Total matched URLs: {total_matched}")
    print(f"Total unmatched URLs: {total_unmatched}")
    print(f"Output files saved in: {os.path.abspath(output_dir)}")

# Example usage
if __name__ == '__main__':
    old_csv_path = 'oldurl.csv'
    new_csv_path = 'newurl.csv'
    output_directory = 'url_mapping_batches'
    batch_size = 1000  # You can adjust this number
    
    map_urls_in_batches(old_csv_path, new_csv_path, output_directory, batch_size)
