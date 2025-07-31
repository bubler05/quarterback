import sys
import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import argparse
import io
import warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Suppress only the single InsecureRequestWarning from urllib3
warnings.simplefilter('ignore', InsecureRequestWarning)

# Default headers to mimic a real browser
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/115.0 Safari/537.36'
    )
}


def slugify(name: str) -> str:
    """
    Convert a player name to the URL slug used by Sports Reference.
    """
    s = name.lower().strip()
    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"[^a-z0-9\-]", "", s)
    return s


def fetch_passing_table(player_name: str, max_retries: int = 3) -> pd.DataFrame:
    """
    Fetch the 'Passing' table for the given player and return it as a DataFrame,
    including handling commented-out tables.
    """
    base_slug = slugify(player_name)
    candidates = []
    # Already slugified name may include numeric suffix
    if re.search(r"-\d+$", base_slug):
        candidates.append(base_slug)
    candidates.extend([f"{base_slug}-1", f"{base_slug}-2"])

    for slug in candidates:
        url = f"https://www.sports-reference.com/cfb/players/{slug}.html"
        for attempt in range(1, max_retries + 1):
            resp = requests.get(url, headers=HEADERS)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')

                # 1) Try live table
                table = soup.find('table', id='passing_standard')
                if table is None:
                    # 2) Try commented-out div containing career table
                    commented_div = soup.find('div', id='div_passing_standard')
                    if commented_div:
                        html = str(commented_div)
                        html = re.sub(r'<!--|-->', '', html)
                        table = BeautifulSoup(html, 'html.parser').find('table')

                if table is not None:
                    return pd.read_html(str(table))[0]
                break
            elif resp.status_code in (429, 503):
                time.sleep(2 * attempt)
                continue
            else:
                break
    raise ValueError(f"Could not fetch passing table for '{player_name}' (tried: {', '.join(candidates)})")


def extract_career_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract the career totals row from the raw DataFrame.
    """
    df.columns = [re.sub(r"[†*#]", "", col).strip() for col in df.columns]
    season_col = 'Season' if 'Season' in df.columns else 'year_id'
    career_df = df[df[season_col] == 'Career']
    if career_df.empty:
        raise ValueError('Career row not found in the table')
    drop_cols = [season_col, 'Team', 'Conf', 'Class', 'Pos', 'Awards']
    career_df = career_df.drop(columns=[c for c in drop_cols if c in career_df.columns])
    for col in career_df.columns:
        career_df[col] = pd.to_numeric(career_df[col], errors='coerce')
    return career_df.reset_index(drop=True)


def batch_generate(input_csv: str, output_csv: str = None):
    """
    Read a CSV of player names (local file or URL), fetch each player's career passing stats,
    merge them back with the original labels, and write out a combined CSV.
    """
    # Load input CSV (URL or local)
    if input_csv.startswith(('http://', 'https://')):
        resp = requests.get(input_csv, headers=HEADERS, verify=False)
        resp.raise_for_status()
        csv_str = resp.content.decode('utf-8')
        players_df = pd.read_csv(io.StringIO(csv_str), engine='python', on_bad_lines='skip')
    else:
        players_df = pd.read_csv(input_csv, engine='python', on_bad_lines='skip')

    # Identify name column (case-insensitive)
    name_cols = [c for c in players_df.columns if c.strip().lower() in ('player', 'name')]
    if name_cols:
        name_col = name_cols[0]
    else:
        # Fallback to first column
        name_col = players_df.columns[0]
        print(f"Warning: No 'Player' or 'Name' column found. Using '{name_col}' as name column.", file=sys.stderr)

    players_df[name_col] = players_df[name_col].astype(str).str.strip()
    unique_names = players_df[name_col].drop_duplicates()

    stats_list = []
    for name in unique_names:
        try:
            raw = fetch_passing_table(name)
            career = extract_career_row(raw)
            career.insert(0, 'Player', name)
            stats_list.append(career)
        except Exception as e:
            print(f"Warning fetching {name}: {e}", file=sys.stderr)

    if not stats_list:
        print("No data fetched.", file=sys.stderr)
        sys.exit(1)

    stats_df = pd.concat(stats_list, ignore_index=True)

    # Merge original labels back
    labels_df = players_df.drop_duplicates(subset=[name_col]).set_index(name_col)
    merged = labels_df.join(stats_df.set_index('Player'), how='left')
    final_df = merged.reset_index()

    # Output to CSV
    if output_csv:
        final_df.to_csv(output_csv, index=False)
        print(f"Saved career stats to {output_csv}")
    else:
        print(final_df.to_csv(index=False))


def main():
    parser = argparse.ArgumentParser(
        description='Batch fetch career passing stats for multiple players and preserve labels.'
    )
    parser.add_argument('input_file', help='CSV with Player or Name column (path or URL).')
    parser.add_argument('-o', '--output', help='Output CSV path')
    args = parser.parse_args()
    try:
        batch_generate(args.input_file, args.output)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
