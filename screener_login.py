#!/usr/bin/env python3
"""Screener.in concalls scraper - with PDF extraction"""

import os
import time
import re
import csv
import tempfile
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import base64
import json
import pdfplumber
import logging
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# Suppress PDF parsing warnings
logging.getLogger("pdfminer").setLevel(logging.ERROR)

# Google Sheets settings
SHEET_NAME = "Screener Concalls"
CREDENTIALS_FILE = "credentials.json"


def get_google_credentials():
    """Get Google credentials from file or environment variable."""
    # Try environment variable first (for GitHub Actions)
    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS_BASE64")
    if creds_b64:
        creds_json = base64.b64decode(creds_b64).decode('utf-8')
        creds_dict = json.loads(creds_json)
        return Credentials.from_service_account_info(creds_dict, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ])

    # Fall back to local file
    if os.path.exists(CREDENTIALS_FILE):
        return Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ])

    raise FileNotFoundError("No Google credentials found")


def write_to_google_sheets(concalls):
    """Write concalls data to Google Sheets."""
    print("\nConnecting to Google Sheets...")

    # Authenticate
    creds = get_google_credentials()
    client = gspread.authorize(creds)

    # Try to open existing sheet or create new one
    try:
        sheet = client.open(SHEET_NAME)
        print(f"Opened existing sheet: {SHEET_NAME}")
    except gspread.SpreadsheetNotFound:
        sheet = client.create(SHEET_NAME)
        print(f"Created new sheet: {SHEET_NAME}")
        # Share with yourself (optional - add your email)
        # sheet.share('your@email.com', perm_type='user', role='writer')

    worksheet = sheet.sheet1
    worksheet.clear()

    # Prepare data
    headers = ["Company Name", "Date", "Time", "Phone Number", "PDF Link"]
    rows = [headers]
    for c in concalls:
        rows.append([c['company'], c['date'], c['time'], c['phone'], c['pdf_url']])

    # Write all data
    print(f"Writing {len(concalls)} rows...")
    worksheet.update(rows, value_input_option='RAW')

    # Format header row (bold)
    worksheet.format('A1:E1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
    })

    # Set column widths using batch update
    sheet.batch_update({
        "requests": [
            {"updateDimensionProperties": {
                "range": {"sheetId": worksheet.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 150}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {
                "range": {"sheetId": worksheet.id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 130}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {
                "range": {"sheetId": worksheet.id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 3},
                "properties": {"pixelSize": 110}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {
                "range": {"sheetId": worksheet.id, "dimension": "COLUMNS", "startIndex": 3, "endIndex": 4},
                "properties": {"pixelSize": 280}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {
                "range": {"sheetId": worksheet.id, "dimension": "COLUMNS", "startIndex": 4, "endIndex": 5},
                "properties": {"pixelSize": 450}, "fields": "pixelSize"}},
        ]
    })

    # Freeze header row
    worksheet.freeze(rows=1)

    print(f"Sheet URL: {sheet.url}")
    return sheet.url


def extract_phone_from_pdf(pdf_url):
    """Download PDF and extract phone numbers."""
    try:
        # Download PDF
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(pdf_url, headers=headers, timeout=30)

        if response.status_code != 200:
            return "Download failed"

        # Save to temp file
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name

        # Extract text from PDF
        text = ""
        with pdfplumber.open(tmp_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"

        os.unlink(tmp_path)

        # Find phone numbers
        phone_patterns = [
            r'\+91[-\s]?\d{2}[-\s]?\d{4}[-\s]?\d{4}',  # +91 22 6280 1234
            r'\+91[-\s]?\d{10}',                         # +91 9876543210
            r'91[-\s]?\d{2}[-\s]?\d{4}[-\s]?\d{4}',     # 91 22 6280 1234
            r'\d{4}[-\s]?\d{3}[-\s]?\d{4}',             # 1800 123 4567
            r'\d{2,4}[-\s]?\d{4}[-\s]?\d{4}',           # 22 6280 1234
        ]

        phones = []
        for pattern in phone_patterns:
            matches = re.findall(pattern, text)
            phones.extend(matches)

        # Remove duplicates and return first few
        unique_phones = list(dict.fromkeys(phones))
        if unique_phones:
            return "; ".join(unique_phones[:3])
        return "Not found"

    except Exception as e:
        return f"Error: {str(e)[:30]}"


def main():
    # Get credentials
    username = os.environ.get("SCREENER_USERNAME")
    password = os.environ.get("SCREENER_PASSWORD")

    if not username or not password:
        print("Error: Set SCREENER_USERNAME and SCREENER_PASSWORD environment variables")
        return

    # Setup Chrome
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)

    try:
        # Login
        print("Logging in...")
        driver.get("https://www.screener.in/login/")
        time.sleep(2)

        driver.find_element(By.NAME, "username").send_keys(username)
        driver.find_element(By.NAME, "password").send_keys(password)

        login_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        driver.execute_script("arguments[0].click();", login_btn)
        time.sleep(3)

        if "login" in driver.current_url.lower():
            print("Login failed!")
            return

        print("Login successful!\n")

        # Scrape multiple pages to get 100 concalls
        concalls = []
        page = 1
        target_count = 100

        print(f"Fetching up to {target_count} concalls...")

        while len(concalls) < target_count:
            url = f"https://www.screener.in/concalls/upcoming/?p={page}"
            print(f"  Page {page}...", end=" ", flush=True)
            driver.get(url)
            time.sleep(2)

            rows = driver.find_elements(By.CSS_SELECTOR, "table tr")
            page_count = 0

            for row in rows:
                try:
                    th = row.find_element(By.TAG_NAME, "th")
                    tds = row.find_elements(By.TAG_NAME, "td")

                    if len(tds) >= 2:
                        company = th.text.strip()
                        date = tds[0].text.strip()
                        time_str = tds[1].text.strip()

                        # Get PDF link
                        pdf_url = ""
                        links = th.find_elements(By.TAG_NAME, "a")
                        for link in links:
                            href = link.get_attribute("href") or ""
                            if ".pdf" in href.lower():
                                pdf_url = href
                                break

                        if company and pdf_url:
                            concalls.append({
                                "company": company,
                                "date": date,
                                "time": time_str,
                                "pdf_url": pdf_url
                            })
                            page_count += 1
                except:
                    continue

            print(f"found {page_count}")

            if page_count == 0:
                break  # No more pages
            page += 1

        # Remove duplicates (same company + date + time)
        seen = set()
        unique_concalls = []
        for c in concalls:
            key = (c['company'], c['date'], c['time'])
            if key not in seen:
                seen.add(key)
                unique_concalls.append(c)

        concalls = unique_concalls[:target_count]
        print(f"\nTotal: {len(concalls)} unique concalls\n")

        # Extract phone numbers from PDFs
        print("Extracting phone numbers from PDFs...")
        print("-" * 60)

        for i, c in enumerate(concalls):
            print(f"[{i+1}/{len(concalls)}] {c['company'][:25]:<25} ", end="", flush=True)
            c['phone'] = extract_phone_from_pdf(c['pdf_url'])
            print(f"-> {c['phone'][:40]}")
            time.sleep(0.3)  # Be nice to servers

        # Sort by date and time (earliest first)
        print("\nSorting by date...")

        def parse_datetime(c):
            try:
                # Parse "24 January 2026" and "9:30:00 AM"
                date_str = c['date'] + " " + c['time']
                return datetime.strptime(date_str, "%d %B %Y %I:%M:%S %p")
            except:
                return datetime.max  # Put unparseable dates at end

        concalls.sort(key=parse_datetime)

        # Save to CSV (backup)
        csv_file = "concalls.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Company Name", "Date", "Time", "Phone Number", "PDF Link"])
            for c in concalls:
                writer.writerow([c['company'], c['date'], c['time'], c['phone'], c['pdf_url']])
        print(f"CSV backup saved: {csv_file}")

        # Write to Google Sheets
        sheet_url = write_to_google_sheets(concalls)

        print(f"\n{'='*60}")
        print(f"Done! {len(concalls)} concalls written to Google Sheets")
        print(f"{'='*60}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
