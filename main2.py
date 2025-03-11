import os
import csv
import time
import re
import requests
import random

from bs4 import BeautifulSoup, Tag

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from selenium.common.exceptions import (
    StaleElementReferenceException,
    WebDriverException,
    TimeoutException
)

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ----------------------------------------------------------------
# GLOBAL CONSTANTS
# ----------------------------------------------------------------

MONTH_MAP = {
    'Jan': 'January', 'Feb': 'February', 'Mar': 'March', 'Apr': 'April', 'May': 'May',
    'Jun': 'June', 'Jul': 'July', 'Aug': 'August', 'Sep': 'September', 'Oct': 'October',
    'Nov': 'November', 'Dec': 'December'
}

UNIVERSITY_CSV_FILE = 'universities.csv'
COURSE_CSV_FILE = 'courses.csv'
PAGES_DB_FILE = 'pages_db.csv'
LOG_FILE = 'scraper.log'

# The categories we want to scrape:
CATEGORIES = ["Postgraduate", "Undergraduate", "Pre-sessional", "Foundation", "Pre-masters", "Research"]

# ----------------------------------------------------------------
# LOGGING
# ----------------------------------------------------------------

def log(msg):
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {msg}"
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + "\n")

# ----------------------------------------------------------------
# CSV PREPARATION / LOADING
# ----------------------------------------------------------------

def prepare_csv_files():
    if not os.path.exists(UNIVERSITY_CSV_FILE):
        with open(UNIVERSITY_CSV_FILE, 'w', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            w.writerow([
                'university_identifier',
                'university_name',
                'university_logo',
                'rank',
                'established',
                'famous_for',
                'fees',
                'location',
                'website_url',
                'overview_html',
                'services_html',
                'rankings_html',
                'fees_html',
                'scholarships_html',
                'accommodation_html',
                'faqs_html'
            ])

    if not os.path.exists(COURSE_CSV_FILE):
        with open(COURSE_CSV_FILE, 'w', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            w.writerow([
                'course_id',
                'title',
                'university_name',
                'intake',
                'degree',
                'course_meta',
                'category',
                'course_year',
                'start_month',
                'is_featured',
                'location',
                'university_rank',
                'university_logo'
            ])

    if not os.path.exists(PAGES_DB_FILE):
        with open(PAGES_DB_FILE, 'w', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            w.writerow(['year', 'category', 'page'])

def load_scraped_universities():
    s = set()
    if os.path.exists(UNIVERSITY_CSV_FILE):
        with open(UNIVERSITY_CSV_FILE, 'r', encoding='utf-8', newline='') as f:
            rd = csv.DictReader(f)
            for row in rd:
                uid = row['university_identifier'].strip()
                unm = row['university_name'].strip()
                s.add((uid, unm))
    return s

def load_scraped_courses():
    s = set()
    if os.path.exists(COURSE_CSV_FILE):
        with open(COURSE_CSV_FILE, 'r', encoding='utf-8', newline='') as f:
            rd = csv.DictReader(f)
            for row in rd:
                cid  = row['course_id'].strip()
                cttl = row['title'].strip()
                cmet = row['course_meta'].strip()
                cyer = row['course_year'].strip()
                s.add((cid, cttl, cmet, cyer))
    return s

def load_scraped_pages():
    s = set()
    if os.path.exists(PAGES_DB_FILE):
        with open(PAGES_DB_FILE, 'r', encoding='utf-8', newline='') as f:
            rd = csv.DictReader(f)
            for row in rd:
                y = row['year'].strip()
                c = row['category'].strip()
                p = row['page'].strip()
                s.add((y, c, p))
    return s

def save_university_data(data):
    with open(UNIVERSITY_CSV_FILE, 'a', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow([
            data.get('university_identifier', ''),
            data.get('university_name', ''),
            data.get('university_logo', ''),
            data.get('rank', ''),
            data.get('established', ''),
            data.get('famous_for', ''),
            data.get('fees', ''),
            data.get('location', ''),
            data.get('website_url', ''),
            data.get('overview_html', ''),
            data.get('services_html', ''),
            data.get('rankings_html', ''),
            data.get('fees_html', ''),
            data.get('scholarships_html', ''),
            data.get('accommodation_html', ''),
            data.get('faqs_html', '')
        ])

def save_course_data(data):
    with open(COURSE_CSV_FILE, 'a', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow([
            data.get('course_id', ''),
            data.get('title', ''),
            data.get('university_name', ''),
            data.get('intake', ''),
            data.get('degree', ''),
            data.get('course_meta', ''),
            data.get('category', ''),
            data.get('course_year', ''),
            data.get('start_month', ''),
            data.get('is_featured', ''),
            data.get('location', ''),
            data.get('university_rank', ''),
            data.get('university_logo', '')
        ])

def save_page_done(year, category, page):
    with open(PAGES_DB_FILE, 'a', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow([year, category, page])

# ----------------------------------------------------------------
# WAITING / LOADING
# ----------------------------------------------------------------

def wait_for_page_loaded(driver, max_wait=30):
    try:
        WebDriverWait(driver, max_wait).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        log("[WARN] Timed out waiting for document.readyState=complete")

    try:
        WebDriverWait(driver, max_wait).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, '.siuk-prelaoder'))
        )
    except TimeoutException:
        log("[WARN] Timed out waiting for .siuk-prelaoder vanish")

def wait_for_courses_load(driver, max_wait=30):
    wait_for_page_loaded(driver, max_wait)
    try:
        WebDriverWait(driver, max_wait).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.single-events-card'))
        )
    except TimeoutException:
        log("[WARN] Timed out waiting for .single-events-card. Possibly empty page or slow site.")

# ----------------------------------------------------------------
# SANITIZE HTML UTILS
# ----------------------------------------------------------------

def sanitize_html(html_content):
    if not html_content.strip():
        return html_content
    soup = BeautifulSoup(html_content, 'html.parser')
    for div in soup.select('.uni_course_enquire_now'):
        div.decompose()
    for div in soup.select('.et_pb_text_inner'):
        if div.contents:
            first_child = div.contents[0]
            if isinstance(first_child, Tag) and first_child.name == 'h3':
                div.decompose()
    for img_tag in soup.find_all('img'):
        img_tag.decompose()
    anchors = soup.find_all('a')
    for a_tag in anchors:
        txt_lower = a_tag.get_text(strip=True).lower()
        href_lower = a_tag.get('href', '').lower()
        if 'enquire' in txt_lower or 'enquiry' in txt_lower or 'enquire' in href_lower:
            a_tag.decompose()
        else:
            a_tag.unwrap()
    return str(soup)

def modify_section_html(raw_html):
    return sanitize_html(raw_html)

# ----------------------------------------------------------------
# UNIVERSITY PAGE SCRAPE
# ----------------------------------------------------------------

def get_section_inner_html(soup, section_id):
    section_div = soup.find('div', id=section_id)
    if not section_div:
        return ''
    raw_inner = ''.join(str(child) for child in section_div.contents)
    return modify_section_html(raw_inner)

def scrape_university_page(url, university_id, university_name, console_state):
    log(f"[INFO] Scraping univ ID={university_id}, name={university_name}")
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 404:
            log("[WARN] Univ page 404 => skip")
            return None
        if r.status_code == 429:
            log("[WARN] 429 => wait 120 & retry")
            time.sleep(120)
            return scrape_university_page(url, university_id, university_name, console_state)
        r.raise_for_status()
    except Exception as e:
        log(f"[ERROR] fetch univ => {e}")
        return None

    soup = BeautifulSoup(r.text, 'html.parser')
    data = {}
    data['university_identifier'] = university_id.strip()
    data['university_name'] = university_name.strip()

    logo_el = soup.select_one('.uni_logo img.single-event-image')
    data['university_logo'] = logo_el['src'] if logo_el and logo_el.has_attr('src') else ''

    if not data['university_name']:
        name_el = soup.select_one('.s_event_section.uni_section_wrapper h1')
        if name_el:
            data['university_name'] = name_el.get_text(strip=True)

    data['rank'] = ''
    data['established'] = ''
    data['famous_for'] = ''
    data['fees'] = ''

    rank_divs = soup.select('.head_desc .uni_rank')
    for rdv in rank_divs:
        txt = rdv.get_text(" ", strip=True)
        if txt.startswith("Rank "):
            data['rank'] = txt.replace("Rank ", "").strip()
        elif txt.startswith("Established "):
            data['established'] = txt.replace("Established ", "").strip()
        elif txt.startswith("Famous for "):
            data['famous_for'] = txt.replace("Famous for ", "").strip()
        elif txt.startswith("Fees "):
            data['fees'] = txt.replace("Fees ", "").strip()

    loc_el = soup.select_one('.uni_website a[href*="google.com/local"] span')
    data['location'] = loc_el.get_text(strip=True) if loc_el else ''

    web_el = soup.select_one('.uni_website.s_uni_web a')
    data['website_url'] = web_el['href'] if (web_el and web_el.has_attr('href')) else ''

    data['overview_html'] = get_section_inner_html(soup, 'overview')
    data['services_html'] = get_section_inner_html(soup, 'services')
    data['rankings_html'] = get_section_inner_html(soup, 'rankings')
    data['fees_html'] = get_section_inner_html(soup, 'fees')
    data['scholarships_html'] = get_section_inner_html(soup, 'scholarships')
    data['accommodation_html'] = get_section_inner_html(soup, 'accommodation')
    data['faqs_html'] = get_section_inner_html(soup, 'faqs')

    console_state['uni_scraped_count'] += 1
    log(f"[INFO] Univ ok => {data['university_name']} (count={console_state['uni_scraped_count']})")
    return data

# ----------------------------------------------------------------
# COURSE-BOX SCRAPING
# ----------------------------------------------------------------

def parse_course_box(box):
    data = {}
    c = box.get_attribute("class")
    data['is_featured'] = 'yes' if 'featured-course' in c else 'no'

    data['course_id'] = box.get_attribute("data-course") or ''
    try:
        data['title'] = box.find_element(By.CSS_SELECTOR, 'h3.siuk-card-title').text.strip()
    except:
        data['title'] = ''

    try:
        loc = box.find_element(By.CSS_SELECTOR, '.mini-university-location').text.strip()
    except:
        loc = ''
    data['location'] = loc

    try:
        uname = box.find_element(By.CSS_SELECTOR, 'h4.mini-university-title').text.strip()
    except:
        uname = ''
    data['university_name'] = uname

    try:
        leftinfo = box.find_element(By.CSS_SELECTOR, '.siuk-course-quick-leftinfo p:nth-of-type(2)').text.strip()
    except:
        leftinfo = ''
    data['intake'] = leftinfo

    try:
        rightinfo = box.find_element(By.CSS_SELECTOR, '.siuk-course-quick-rightinfo p:nth-of-type(2)').text.strip()
    except:
        rightinfo = ''
    data['degree'] = rightinfo

    try:
        meta_txt = box.find_element(By.CSS_SELECTOR, 'p.siuk-course-meta').text.strip()
    except:
        meta_txt = ''
    data['course_meta'] = meta_txt

    if '|' in meta_txt:
        data['category'] = meta_txt.split('|')[0].strip()
    else:
        data['category'] = meta_txt

    m = re.match(r'([A-Za-z]{3})\s+(\d{4})', leftinfo)
    if m:
        short_m = m.group(1)
        data['start_month'] = MONTH_MAP.get(short_m, short_m)
        data['course_year'] = m.group(2)
    else:
        data['start_month'] = ''
        data['course_year'] = ''

    return data

def parse_and_scrape_courses(driver, category, console_state):
    wait_for_courses_load(driver)
    cards = driver.find_elements(By.CSS_SELECTOR, '.single-events-card')
    log(f"[{category}] Found {len(cards)} courses on this page.")

    for card in cards:
        cdata = parse_course_box(card)
        c_key = (
            cdata['course_id'].strip(),
            cdata['title'].strip(),
            cdata['course_meta'].strip(),
            cdata['course_year'].strip()
        )
        if c_key in console_state['courses_scraped_set']:
            log(f"   -> Already have {c_key}")
            continue

        try:
            learn_more = card.find_element(By.CSS_SELECTOR, 'a.siuk-view-more-red')
        except:
            log("   -> no learn_more => skip course.")
            continue
        href = learn_more.get_attribute('href') or ''
        if not href:
            log("   -> no href => skip course.")
            continue

        try:
            r = requests.get(href, timeout=10)
            if r.status_code == 404:
                log("   -> 404 => skip.")
                continue
            if r.status_code == 429:
                log("   -> 429 => wait 120 & retry.")
                time.sleep(120)
                r = requests.get(href, timeout=10)
                if r.status_code in [404, 429]:
                    log("   -> still 404/429 => skip.")
                    continue
            r.raise_for_status()
        except Exception as e:
            log(f"   -> link test fail => {e}, skip course.")
            continue

        if '/university/' in href:
            uni_id = href.split('/university/')[-1].strip()
        else:
            uni_id = href.strip()
        uni_name = cdata['university_name'].strip()
        u_key = (uni_id, uni_name)
        if u_key not in console_state['universities_scraped_set']:
            univ_data = scrape_university_page(href, uni_id, uni_name, console_state)
            if not univ_data:
                log("   -> univ error => skip course.")
                continue
            save_university_data(univ_data)
            console_state['universities_scraped_set'].add(u_key)
            cdata['university_rank'] = univ_data.get('rank', '')
            cdata['university_logo'] = univ_data.get('university_logo', '')
        else:
            info = get_university_info_from_csv(uni_id, uni_name)
            cdata['university_rank'] = info.get('rank', '')
            cdata['university_logo'] = info.get('logo', '')

        save_course_data(cdata)
        console_state['course_scraped_count'] += 1
        console_state['courses_scraped_set'].add(c_key)
        log(f"   -> Saved course ID={cdata['course_id']} => {cdata['title']} (total={console_state['course_scraped_count']})")

def get_university_info_from_csv(university_id, university_name):
    if not os.path.exists(UNIVERSITY_CSV_FILE):
        return {}
    uid = university_id.strip()
    unm = university_name.strip()
    with open(UNIVERSITY_CSV_FILE, 'r', encoding='utf-8', newline='') as f:
        rd = csv.DictReader(f)
        for row in rd:
            rid = row['university_identifier'].strip()
            rnm = row['university_name'].strip()
            if rid == uid and rnm == unm:
                return {
                    'rank': row.get('rank', ''),
                    'logo': row.get('university_logo', '')
                }
    return {}

# ----------------------------------------------------------------
# PAGINATION STEPS
# ----------------------------------------------------------------

def get_max_page_number(driver):
    wait_for_page_loaded(driver)
    all_btns = driver.find_elements(By.CSS_SELECTOR, '.siuk-pagination-container button.siuk-filter-pagination-button')
    pages = []
    for b in all_btns:
        dp = b.get_attribute('data-page')
        if dp and dp.isdigit():
            pages.append(int(dp))
    return max(pages) if pages else 1

def click_page(driver, page_str):
    try:
        btn = driver.find_element(By.CSS_SELECTOR, f'.siuk-filter-pagination-button[data-page="{page_str}"]')
        driver.execute_script("arguments[0].click();", btn)
        log(f"[PAGE] Click => {page_str}")
        return True
    except (StaleElementReferenceException, WebDriverException) as e:
        log(f"[WARN] click page {page_str} => {e}")
        return False
    except Exception as ex:
        log(f"[ERROR] click page {page_str} => {ex}")
        return False

def direct_click_page(driver, page_str):
    """
    Directly select a page number using the dropdown input.
    This function locates the <select> element with class "siuk-pagination-dropdown",
    clicks it to open, then finds and clicks the option with value equal to page_str.
    """
    try:
        select_el = driver.find_element(By.CSS_SELECTOR, 'select.siuk-pagination-dropdown')
        driver.execute_script("arguments[0].click();", select_el)
        time.sleep(1)  # wait for the dropdown to open
        option_el = select_el.find_element(By.CSS_SELECTOR, f'option[value="{page_str}"]')
        option_el.click()
        log(f"[PAGE] Direct dropdown click => {page_str}")
        return True
    except Exception as ex:
        log(f"[ERROR] direct click page {page_str} => {ex}")
        return False

def reload_and_click_category(driver, category):
    driver.refresh()
    wait_for_page_loaded(driver)
    try:
        cat_el = driver.find_element(By.ID, category)
        driver.execute_script("arguments[0].click();", cat_el)
        log(f"[INFO] clicked category => {category} after reload")
        wait_for_courses_load(driver)
        return True
    except Exception as e:
        log(f"[ERROR] reload & click category => {e}")
        return False

def go_to_page_by_stepping(driver, category, year, target_page):
    log(f"[STEPPING] cat={category}, year={year}, from page=1..{target_page}")
    if not reload_and_click_category(driver, category):
        return False

    max_p = get_max_page_number(driver)
    if target_page > max_p:
        log(f"[WARN] stepping => target_page={target_page} > max={max_p}, using max page")
        target_page = max_p

    for p in range(2, target_page + 1):
        sp = str(p)
        ok = click_page(driver, sp)
        if not ok:
            log(f"[ERROR] stepping => cannot click page={sp}")
            return False
        wait_for_courses_load(driver)
    return True

def try_go_to_page(driver, category, year, page_idx):
    """
    Attempt to navigate to a target page with three strategies:
      1) Normal click
      2) Direct dropdown selection
      3) Stepping through pages sequentially
    """
    sp = str(page_idx)
    for attempt in range(1, 4):
        log(f"[ATTEMPT] page={sp}, attempt={attempt}")
        if attempt == 1:
            ok = click_page(driver, sp)
            if ok:
                wait_for_courses_load(driver)
                return True
        elif attempt == 2:
            ok = direct_click_page(driver, sp)
            if ok:
                wait_for_courses_load(driver)
                return True
        else:
            if go_to_page_by_stepping(driver, category, year, page_idx):
                return True

    log(f"[FAIL] page={sp} => after 3 attempts, skip it.")
    return False

def scrape_category_pages(driver, category, console_state, year):
    wait_for_page_loaded(driver)
    max_page = get_max_page_number(driver)
    log(f"[{category}][{year}] max_page => {max_page}")

    if (year, category, '1') not in console_state['pages_done_set']:
        parse_and_scrape_courses(driver, category, console_state)
        save_page_done(year, category, '1')
        console_state['pages_done_set'].add((year, category, '1'))
    else:
        log(f"[{category}][{year}] page=1 => in DB => skip parse")

    if max_page <= 1:
        return

    for p_idx in range(2, max_page + 1):
        sp = str(p_idx)
        if (year, category, sp) in console_state['pages_done_set']:
            log(f"[{category}][{year}] page={sp} => in DB => skip parse")
            continue

        got_page = try_go_to_page(driver, category, year, p_idx)
        if not got_page:
            log(f"[SKIP] page={sp} after repeated fails.")
            continue

        if (year, category, sp) not in console_state['pages_done_set']:
            parse_and_scrape_courses(driver, category, console_state)
            save_page_done(year, category, sp)
            console_state['pages_done_set'].add((year, category, sp))

# ----------------------------------------------------------------
# YEAR SELECTION
# ----------------------------------------------------------------

def get_available_years(driver):
    wait_for_page_loaded(driver)
    year_options = driver.find_elements(By.CSS_SELECTOR, '.siuk-filter-select.year option')
    vals = []
    for op in year_options:
        val = op.get_attribute("value").strip()
        if val:
            vals.append(val)
    return vals

def select_year(driver, year_val):
    log(f"[INFO] Changing year => {year_val}")
    try:
        wait_for_page_loaded(driver)
        sel = driver.find_element(By.CSS_SELECTOR, '.siuk-filter-select.year')
        driver.execute_script("arguments[0].click();", sel)
        time.sleep(1)
        op = driver.find_element(By.CSS_SELECTOR, f'.siuk-filter-select.year option[value="{year_val}"]')
        driver.execute_script("arguments[0].selected = true;", op)
        op.click()
        wait_for_page_loaded(driver)
        log(f"[INFO] year changed => {year_val}")
        return True
    except Exception as e:
        log(f"[WARN] year change => {e}, reload & retry..")
        driver.refresh()
        wait_for_page_loaded(driver)
        try:
            sel2 = driver.find_element(By.CSS_SELECTOR, '.siuk-filter-select.year')
            driver.execute_script("arguments[0].click();", sel2)
            time.sleep(1)
            op2 = driver.find_element(By.CSS_SELECTOR, f'.siuk-filter-select.year option[value="{year_val}"]')
            driver.execute_script("arguments[0].selected = true;", op2)
            op2.click()
            wait_for_page_loaded(driver)
            log(f"[INFO] year changed => {year_val} after reload.")
            return True
        except Exception as e2:
            log(f"[ERROR] still can't => {e2}")
            return False

# ----------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------

def main():
    start_time = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"\n\n=== Scraping started at {start_time} ===\n")

    prepare_csv_files()

    unis = load_scraped_universities()
    crses = load_scraped_courses()
    pages = load_scraped_pages()

    console_state = {
        'universities_scraped_set': unis,
        'courses_scraped_set': crses,
        'pages_done_set': pages,
        'uni_scraped_count': len(unis),
        'course_scraped_count': len(crses)
    }

    opts = Options()
    opts.headless = False
    opts.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=opts)
    driver.get("https://india.studyin-uk.com/find-courses/")
    wait_for_page_loaded(driver, max_wait=60)

    all_years = get_available_years(driver)
    log(f"[INFO] Found year options => {all_years}")

    for year_val in all_years:
        ok = select_year(driver, year_val)
        if not ok:
            log(f"[WARN] cannot set year => {year_val}, skipping it.")
            continue

        for cat in CATEGORIES:
            log(f"=== Category={cat}, Year={year_val} ===")
            try:
                cat_el = driver.find_element(By.ID, cat)
                driver.execute_script("arguments[0].click();", cat_el)
                log(f"[INFO] clicked category => {cat}, year={year_val}")
                wait_for_courses_load(driver)
                parse_and_scrape_courses(driver, cat, console_state)
                scrape_category_pages(driver, cat, console_state, year_val)
            except Exception as e:
                log(f"[ERROR] cat={cat}, year={year_val}, e={e}, reload & retry..")
                driver.refresh()
                wait_for_page_loaded(driver)
                try:
                    cat_el2 = driver.find_element(By.ID, cat)
                    driver.execute_script("arguments[0].click();", cat_el2)
                    wait_for_courses_load(driver)
                    parse_and_scrape_courses(driver, cat, console_state)
                    scrape_category_pages(driver, cat, console_state, year_val)
                except Exception as e2:
                    log(f"[ERROR] skip cat={cat}, year={year_val} => {e2}")

    driver.quit()

    end_time = time.strftime("%Y-%m-%d %H:%M:%S")
    log("[INFO] Done scraping.")
    log(f"[INFO] final => unis={console_state['uni_scraped_count']}, courses={console_state['course_scraped_count']}")
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"=== Scraping ended at {end_time} ===\n")

if __name__ == '__main__':
    main()
