import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import argparse

def slugify(name):
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return re.sub(r"-+", "-", slug).strip("-")

def fetch_career_rushing(slug):
    url = f"https://www.sports-reference.com/cfb/players/{slug}-1.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    print(f">>> GET {url}")
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(f"    ✖ HTTP {r.status_code}")
        return None, None, None, None, None

    soup = BeautifulSoup(r.text, "html.parser")

    tbl = soup.find("table", id="rushing_standard")
    if not tbl:
        print("    ✖ No <table id='rushing_standard'> found")
        return None, None, None, None, None

    tfoot = tbl.find("tfoot")
    if not tfoot:
        print("    ✖ Table has no <tfoot>")
        return None, None, None, None, None

    cells = tfoot.find("tr").find_all("td")
    if len(cells) < 7:
        print(f"    ✖ Too few <td> in tfoot ({len(cells)} found)")
        return None, None, None, None, None

    att  = cells[2].text.strip()  # rush_att
    yds  = cells[3].text.strip()  # rush_yds
    ypa  = cells[4].text.strip()  # rush_yds_per_att
    td   = cells[5].text.strip()  # rush_td
    ypg  = cells[6].text.strip()  # rush_yds_per_g

    print(f"    ✔ Parsed: Att={att!r}, Yds={yds!r}, Y/A={ypa!r}, TD={td!r}, Y/G={ypg!r}")
    return att, yds, ypa, td, ypg

def main():
    p = argparse.ArgumentParser()
    p.add_argument("input_csv", help="e.g. stats.csv")
    p.add_argument("-o", "--output_csv",
                   default="stats_with_rushing.csv")
    args = p.parse_args()

    df = pd.read_csv(args.input_csv)
    if "G" in df.columns:
        df = df.drop(columns=["G"])

    for col in ("RushAtt","RushYds","RushYPA","RushTD","RushYPG"):
        df[col] = ""

    for i, row in df.iterrows():
        player = row["Player"]
        slug   = slugify(player)
        print(f"\nFetching career rushing for: {player!r}")
        att, yds, ypa, td, ypg = fetch_career_rushing(slug)
        df.at[i, "RushAtt"] = att  or ""
        df.at[i, "RushYds"] = yds  or ""
        df.at[i, "RushYPA"] = ypa  or ""
        df.at[i, "RushTD"]  = td   or ""
        df.at[i, "RushYPG"] = ypg  or ""
        time.sleep(1)

    df.to_csv(args.output_csv, index=False)
    print(f"\n✅ Saved → {args.output_csv}")

if __name__ == "__main__":
    main()
