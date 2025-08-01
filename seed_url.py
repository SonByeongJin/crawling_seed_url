from playwright.sync_api import sync_playwright
from urllib.parse import urlparse
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# 크롤링 설정
CONCURRENT_WORKERS = 16
BASE_DOMAIN = "yc.go.kr"
START_URL = "https://yc.go.kr/main.do"
MAX_PAGES = 80000
CHECK_INTERVAL = 500

# 무시할 도메인 및 확장자
IGNORED_DOMAINS = [
    "facebook.com", "instagram.com", "youtube.com", "twitter.com",
    "naver.com", "daum.net", "kakao.com", "google.com", "gov.kr", "mail."
]
EXCLUDED_EXTENSIONS = [
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg",
    ".zip", ".rar", ".tar", ".gz", ".7z",
    ".pdf", ".hwp", ".doc", ".docx", ".xls", ".xlsx",
    ".ppt", ".pptx", ".mp4", ".avi", ".mp3", ".wav",
    ".exe", ".apk", ".dll", ".git", ".download"
]

# 유효한 링크인지 확인
def is_valid_link(href):
    if not href:
        return False
    href = href.strip().lower()
    if href.startswith(("mailto:", "javascript:", "tel:")):
        return False
    parsed = urlparse(href)
    full_check_target = parsed.path + parsed.query
    if any(full_check_target.endswith(ext) for ext in EXCLUDED_EXTENSIONS):
        return False
    if parsed.scheme and parsed.scheme not in ["http", "https"]:
        return False
    if any(domain in href for domain in IGNORED_DOMAINS):
        return False
    return BASE_DOMAIN in parsed.netloc

# 각 페이지 처리
def process_page(url):
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=["--ignore-certificate-errors"])
            page = browser.new_page()
            page.goto(url, timeout=7000)
            page.wait_for_load_state("networkidle", timeout=7000)
            hrefs = page.eval_on_selector_all("a", "els => els.map(el => el.href)")
            page.close()
            browser.close()

            valid_links = set()
            for href in hrefs:
                if is_valid_link(href):
                    valid_links.add(href.split("#")[0])
            return (url, valid_links, None)
    except Exception as e:
        return (url, set(), str(e))

# 병렬 크롤링
def crawl_all_parallel():
    to_visit = set([START_URL])
    visited = set()
    all_links = set()
    failed_urls = set()

    progress = tqdm(total=MAX_PAGES, desc="🌐 병렬 크롤링", unit="page", 
                   bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')

    last_check = 0  # 직전 체크 시점 저장

    while to_visit and len(visited) < MAX_PAGES:
        current_batch = list(to_visit)[:CONCURRENT_WORKERS]
        to_visit.difference_update(current_batch)

        results = []
        with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
            futures = [executor.submit(process_page, url) for url in current_batch]
            for future in as_completed(futures):
                results.append(future.result())

        for url, new_links, error in results:
            visited.add(url)
            progress.update(1)

            if error:
                failed_urls.add(url)
            else:
                filtered_links = new_links - visited - to_visit
                to_visit.update(filtered_links)
                all_links.update(filtered_links)


        if len(visited) - last_check >= CHECK_INTERVAL:
            progress.write(
                f"📊 방문: {len(visited)} | 수집: {len(all_links)} | 대기중: {len(to_visit)} | 실패: {len(failed_urls)}"
            )
            last_check = len(visited)


    progress.close()

    # 결과 저장
    with open("recursive_links.json", "w", encoding="utf-8") as f:
        json.dump(list(all_links), f, ensure_ascii=False, indent=2)
    with open("failed_urls.json", "w", encoding="utf-8") as f:
        json.dump(list(failed_urls), f, ensure_ascii=False, indent=2)

    print(f"\n✅ 병렬 수집 완료: 방문 {len(visited)}개 | 실패 {len(failed_urls)}개")

# 메인 실행
if __name__ == "__main__":
    print("🔄 시작")
    print("🔄 초기 병렬 크롤링 시작")
    crawl_all_parallel()
    print("🔄 초기 병렬 크롤링 완료")
