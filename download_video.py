import csv
import os
import wget
import requests
from concurrent.futures import ThreadPoolExecutor

def download_url(url, download_path):
    try:
        wget.download(url, out=download_path)
        print(f"Downloaded: {url}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

def process_csv_file(csv_file, download_folder):
    urls_to_download = []

    # Read the CSV file and filter lines containing 'gBR'
    with open(csv_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            for field in row:
                if 'gBR' in field:
                    urls_to_download.append(field)
                    break  # No need to check other fields in this row

    return urls_to_download

def download_csv_file(url, save_path):
    response = requests.get(url)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    with open(save_path, 'wb') as file:
        file.write(response.content)
    print(f"Downloaded CSV: {url}")

def ensure_directories_exist(directories):
    for folder in directories:
        if not os.path.exists(folder):
            os.makedirs(folder)

def main(csv_files, download_folders, csv_urls):
    all_urls_to_download = []

    # Ensure each download folder exists
    ensure_directories_exist(download_folders)

    # Download CSV files
    for url, save_path in csv_urls:
        download_csv_file(url, save_path)

    # Process each CSV file and collect all URLs
    for csv_file, download_folder in zip(csv_files, download_folders):
        urls = process_csv_file(csv_file, download_folder)
        all_urls_to_download.extend([(url, download_folder) for url in urls])

    # Use ThreadPoolExecutor to download URLs concurrently
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(lambda url_folder: download_url(*url_folder), all_urls_to_download)

if __name__ == "__main__":
    csv_files = [
        'dance_video/basic/refined_10M_sBM_url.csv',
        'dance_video/advanced/refined_10M_sFM_url.csv',
        'dance_video/moved/refined_10M_sMM_url.csv'
    ]
    download_folders = [
        'dance_video/basic',
        'dance_video/advanced',
        'dance_video/moved'
    ]
    csv_urls = [
        ('https://aistdancedb.ongaaccel.jp/data/video_refined/10M/refined_10M_sBM_url.csv', 'dance_video/basic/refined_10M_sBM_url.csv'),
        ('https://aistdancedb.ongaaccel.jp/data/video_refined/10M/refined_10M_sFM_url.csv', 'dance_video/advanced/refined_10M_sFM_url.csv'),
        ('https://aistdancedb.ongaaccel.jp/data/video_refined/10M/refined_10M_sMM_url.csv', 'dance_video/moved/refined_10M_sMM_url.csv')
    ]
    main(csv_files, download_folders, csv_urls)
