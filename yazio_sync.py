"""
YAZIO -> Notion Full Sync
1. Fetches consumed items from YAZIO
2. Creates/updates Food entries in the Food DB
3. Creates Calculator entries (individual meals)
4. Updates Journal with daily totals + goals + weight

Usage:
  python yazio_sync.py              # sync today
  python yazio_sync.py 2026-04-10   # sync specific date
  python yazio_sync.py --backfill 7 # sync last 7 days
"""

import requests
import sys
import os
import json
from datetime import date, timedelta

# -- YAZIO Config --
YAZIO_BASE = "https://yzapi.yazio.com/v15"
YAZIO_CLIENT_ID = "1_4hiybetvfksgw40o0sog4s884kwc840wwso8go4k8c04goo4c"
YAZIO_CLIENT_SECRET = "6rok2m65xuskgkgogw40wkkk8sw0osg84s8cggsc4woos4s8o"
YAZIO_EMAIL = os.environ["YAZIO_EMAIL"]
YAZIO_PASSWORD = os.environ["YAZIO_PASSWORD"]

# -- Notion Config --
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_API = "https://api.notion.com/v1"
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# Notion DB IDs
JOURNAL_DB = "2deda7da88db811f98f5f860d49af03d"
FOOD_DB = "2deda7da88db818caa0bef2df3178e8b"
CALCULATOR_DB = "2deda7da88db8101b4e0dd3f1d0ba0eb"
TODAY_PAGE_ID = "2deda7da88db81c3bfc9dd213ebddd77"  # Fixed page in "Today" DB (not Journal)

# Meal mapping YAZIO -> Notion
MEAL_MAP = {
    "breakfast": "Breakfast",
    "lunch": "Lunch",
    "dinner": "Diner",  # Notion uses "Diner" not "Dinner"
    "snack": "Snack",
}


# =============================================
# YAZIO API
# =============================================

def yazio_login():
    r = requests.post(f"{YAZIO_BASE}/oauth/token", data={
        "grant_type": "password",
        "client_id": YAZIO_CLIENT_ID,
        "client_secret": YAZIO_CLIENT_SECRET,
        "username": YAZIO_EMAIL,
        "password": YAZIO_PASSWORD,
    })
    r.raise_for_status()
    return r.json()["access_token"]


def yazio_get_consumed(token, target_date):
    """Fetch individual consumed items for a date."""
    r = requests.get(
        f"{YAZIO_BASE}/user/consumed-items",
        params={"date": target_date},
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r.json()


def yazio_get_product(token, product_id):
    """Fetch product details (name, nutrients per gram)."""
    r = requests.get(
        f"{YAZIO_BASE}/products/{product_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r.json()


def yazio_get_daily_summary(token, target_date):
    """Fetch daily summary (goals, weight)."""
    r = requests.get(
        f"{YAZIO_BASE}/user/widgets/daily-summary",
        params={"date": target_date},
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return r.json()


# =============================================
# NOTION API helpers
# =============================================

def notion_query(db_id, filter_obj):
    r = requests.post(
        f"{NOTION_API}/databases/{db_id}/query",
        headers=NOTION_HEADERS,
        json={"filter": filter_obj},
    )
    r.raise_for_status()
    return r.json().get("results", [])


def notion_create_page(db_id, properties):
    r = requests.post(
        f"{NOTION_API}/pages",
        headers=NOTION_HEADERS,
        json={"parent": {"database_id": db_id}, "properties": properties},
    )
    r.raise_for_status()
    return r.json()


def notion_update_page(page_id, properties):
    r = requests.patch(
        f"{NOTION_API}/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": properties},
    )
    r.raise_for_status()
    return r.json()


# =============================================
# FOOD DB operations
# =============================================

def find_food_by_yazio_id(yazio_id):
    """Find a Food entry by YAZIO product ID."""
    pages = notion_query(FOOD_DB, {
        "property": "YAZIO ID",
        "rich_text": {"equals": yazio_id},
    })
    return pages[0] if pages else None


def create_food(name, yazio_id, nutrients_per_gram):
    """Create a Food entry with macros per 100g."""
    cal_100 = round(nutrients_per_gram.get("energy.energy", 0) * 100, 1)
    prot_100 = round(nutrients_per_gram.get("nutrient.protein", 0) * 100, 1)
    carbs_100 = round(nutrients_per_gram.get("nutrient.carb", 0) * 100, 1)
    fat_100 = round(nutrients_per_gram.get("nutrient.fat", 0) * 100, 1)

    # Sanity check: if values per 100g are absurdly high, YAZIO probably
    # returned nutrients already per 100g instead of per gram
    if cal_100 > 1500:  # No food has >1500 kcal per 100g (pure fat = 900)
        print(f"    ⚠ Suspicious kcal/100g for '{name}': {cal_100} — YAZIO may have returned per-100g values, dividing by 100")
        cal_100 = round(cal_100 / 100, 1)
        prot_100 = round(prot_100 / 100, 1)
        carbs_100 = round(carbs_100 / 100, 1)
        fat_100 = round(fat_100 / 100, 1)

    props = {
        "Food Name": {"title": [{"text": {"content": name}}]},
        "YAZIO ID": {"rich_text": [{"text": {"content": yazio_id}}]},
        "Calories": {"number": cal_100},
        "Protein": {"number": prot_100},
        "Carbs": {"number": carbs_100},
        "Fat": {"number": fat_100},
        "Reference": {"rich_text": [{"text": {"content": "per 100g"}}]},
    }
    return notion_create_page(FOOD_DB, props)


def normalize_food_if_needed(food_page):
    """If a Food entry has Reference='1g', convert to 'per 100g' (multiply by 100)."""
    props = food_page.get("properties", {})
    ref_rt = props.get("Reference", {}).get("rich_text", [])
    ref = ref_rt[0]["plain_text"] if ref_rt else ""

    if ref == "1g":
        old_cal = props.get("Calories", {}).get("number") or 0
        old_prot = props.get("Protein", {}).get("number") or 0
        old_carbs = props.get("Carbs", {}).get("number") or 0
        old_fat = props.get("Fat", {}).get("number") or 0

        notion_update_page(food_page["id"], {
            "Calories": {"number": round(old_cal * 100, 1)},
            "Protein": {"number": round(old_prot * 100, 1)},
            "Carbs": {"number": round(old_carbs * 100, 1)},
            "Fat": {"number": round(old_fat * 100, 1)},
            "Reference": {"rich_text": [{"text": {"content": "per 100g"}}]},
        })
        print(f"    Normalized Food '{food_page['id'][:12]}...' from 1g to per 100g")
        # Return updated values
        return {
            "calories": round(old_cal * 100, 1),
            "protein": round(old_prot * 100, 1),
            "carbs": round(old_carbs * 100, 1),
            "fat": round(old_fat * 100, 1),
        }
    else:
        return {
            "calories": props.get("Calories", {}).get("number") or 0,
            "protein": props.get("Protein", {}).get("number") or 0,
            "carbs": props.get("Carbs", {}).get("number") or 0,
            "fat": props.get("Fat", {}).get("number") or 0,
        }


def get_or_create_food(yazio_token, product_id):
    """Find or create a Food entry for a YAZIO product. Returns (page_id, macros_per_100g)."""
    existing = find_food_by_yazio_id(product_id)
    if existing:
        macros = normalize_food_if_needed(existing)
        return existing["id"], macros

    # Fetch from YAZIO and create
    product = yazio_get_product(yazio_token, product_id)
    nutrients = product.get("nutrients", {})
    page = create_food(
        name=product.get("name", "Unknown"),
        yazio_id=product_id,
        nutrients_per_gram=nutrients,
    )
    # Return the per-100g values we just stored
    cal_100 = round(nutrients.get("energy.energy", 0) * 100, 1)
    prot_100 = round(nutrients.get("nutrient.protein", 0) * 100, 1)
    carbs_100 = round(nutrients.get("nutrient.carb", 0) * 100, 1)
    fat_100 = round(nutrients.get("nutrient.fat", 0) * 100, 1)
    if cal_100 > 1500:
        cal_100 = round(cal_100 / 100, 1)
        prot_100 = round(prot_100 / 100, 1)
        carbs_100 = round(carbs_100 / 100, 1)
        fat_100 = round(fat_100 / 100, 1)
    macros = {"calories": cal_100, "protein": prot_100, "carbs": carbs_100, "fat": fat_100}
    return page["id"], macros


# =============================================
# CALCULATOR DB operations
# =============================================

def find_calculator_entry(yazio_item_id):
    """Check if a Calculator entry already exists for this YAZIO consumed item."""
    pages = notion_query(CALCULATOR_DB, {
        "property": "YAZIO ID",
        "rich_text": {"equals": yazio_item_id},
    })
    return pages[0] if pages else None


def find_journal_page(target_date):
    """Find the Journal page for a date."""
    pages = notion_query(JOURNAL_DB, {
        "property": "Date",
        "date": {"equals": target_date},
    })
    return pages[0]["id"] if pages else None


def get_food_macros(food_page_id):
    """Fetch macros (per 100g) from a Food page."""
    r = requests.get(
        f"{NOTION_API}/pages/{food_page_id}",
        headers=NOTION_HEADERS,
    )
    r.raise_for_status()
    props = r.json().get("properties", {})
    return {
        "calories": props.get("Calories", {}).get("number") or 0,
        "protein": props.get("Protein", {}).get("number") or 0,
        "carbs": props.get("Carbs", {}).get("number") or 0,
        "fat": props.get("Fat", {}).get("number") or 0,
    }


def create_calculator_entry(food_page_id, quantity, meal, yazio_item_id, intake_time, food_macros=None):
    """Create a Calculator entry with computed macro values."""
    # Compute actual macros from per-100g values
    if food_macros is None:
        food_macros = get_food_macros(food_page_id)

    factor = quantity / 100.0 if quantity else 0

    props = {
        " ": {"title": [{"text": {"content": ""}}]},
        "Food": {"relation": [{"id": food_page_id}]},
        "Quantity": {"number": quantity},
        "Calories": {"number": round(food_macros["calories"] * factor, 1)},
        "Protein": {"number": round(food_macros["protein"] * factor, 1)},
        "Carbs": {"number": round(food_macros["carbs"] * factor, 1)},
        "Fat": {"number": round(food_macros["fat"] * factor, 1)},
        "Meal": {"select": {"name": meal}},
        "YAZIO ID": {"rich_text": [{"text": {"content": yazio_item_id}}]},
        "Ate": {"checkbox": True},
        "Today": {"relation": [{"id": TODAY_PAGE_ID}]},
    }

    # Add intake time
    if intake_time:
        props["Intake Time"] = {"date": {"start": intake_time}}

    return notion_create_page(CALCULATOR_DB, props)


# =============================================
# JOURNAL update
# =============================================

def cleanup_stale_today_relations():
    """
    Unlink the 'Today' relation from Calculator entries whose Intake Time < today.
    This is the day-rollover mechanism: entries stay in the Calculator DB (= history)
    but disappear from the 'Today' view (which filters by: Today relation is set).
    """
    today_iso = date.today().isoformat()
    has_more = True
    start_cursor = None
    cleaned = 0

    while has_more:
        payload = {
            "filter": {
                "and": [
                    {"property": "Today", "relation": {"is_not_empty": True}},
                    {"property": "Intake Time", "date": {"before": today_iso}},
                ]
            },
            "page_size": 100,
        }
        if start_cursor:
            payload["start_cursor"] = start_cursor

        r = requests.post(
            f"{NOTION_API}/databases/{CALCULATOR_DB}/query",
            headers=NOTION_HEADERS,
            json=payload,
        )
        if r.status_code != 200:
            print(f"    Cleanup query failed ({r.status_code}); skipping")
            return
        data = r.json()
        for page in data.get("results", []):
            try:
                notion_update_page(page["id"], {"Today": {"relation": []}})
                cleaned += 1
            except Exception as e:
                print(f"    Failed to unlink {page['id'][:12]}: {e}")
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    if cleaned:
        print(f"    Rolled over {cleaned} stale entries to History")


def update_journal(page_id, nutrition):
    """Update Journal with daily totals from YAZIO."""
    properties = {
        "Calories": {"number": nutrition["calories"]},
        "Calories Goal": {"number": nutrition["calories_goal"]},
        "Protein": {"number": nutrition["protein"]},
        "Protein Goal": {"number": nutrition["protein_goal"]},
        "Carbs": {"number": nutrition["carbs"]},
        "Carbs Goal": {"number": nutrition["carbs_goal"]},
        "Fat": {"number": nutrition["fat"]},
        "Fat Goal": {"number": nutrition["fat_goal"]},
    }
    # Weight comes from Garmin (scale sync), NOT from YAZIO
    return notion_update_page(page_id, properties)


# =============================================
# MAIN SYNC
# =============================================

def sync_date(target_date, yazio_token):
    print(f"\n  [{target_date}]")

    # 1. Find Journal page
    journal_page_id = find_journal_page(target_date)
    if not journal_page_id:
        print(f"    No Journal page -- skipped")
        return False

    # 2. Fetch YAZIO consumed items
    try:
        consumed = yazio_get_consumed(yazio_token, target_date)
    except Exception as e:
        print(f"    YAZIO consumed items error: {e}")
        return False

    products = consumed.get("products", [])
    print(f"    {len(products)} items from YAZIO")

    # 3. For each consumed product -> Food DB + Calculator
    created = 0
    skipped = 0
    for item in products:
        yazio_item_id = item.get("id", "")
        product_id = item.get("product_id", "")
        amount = item.get("amount", 0)
        meal_yazio = item.get("daytime", "snack")
        meal_notion = MEAL_MAP.get(meal_yazio, "Snack")
        intake_time = item.get("date", "").replace(" ", "T")

        # Skip if already synced (but fix Today relation if wrong)
        existing = find_calculator_entry(yazio_item_id)
        if existing:
            # Check if Today relation needs fixing
            today_rel = existing.get("properties", {}).get("Today", {}).get("relation", [])
            has_correct_today = any(r["id"].replace("-", "") == TODAY_PAGE_ID.replace("-", "") for r in today_rel)
            if not has_correct_today:
                try:
                    notion_update_page(existing["id"], {
                        "Today": {"relation": [{"id": TODAY_PAGE_ID}]},
                    })
                    print(f"    Fixed Today relation for {yazio_item_id[:12]}...")
                except Exception as e:
                    print(f"    Fix Today error: {e}")
            skipped += 1
            continue

        # Get or create food
        try:
            food_page_id, food_macros = get_or_create_food(yazio_token, product_id)
        except Exception as e:
            print(f"    Food error ({product_id}): {e}")
            continue

        # Create calculator entry
        try:
            create_calculator_entry(
                food_page_id=food_page_id,
                quantity=amount,
                meal=meal_notion,
                yazio_item_id=yazio_item_id,
                intake_time=intake_time,
                food_macros=food_macros,
            )
            created += 1
        except Exception as e:
            print(f"    Calculator error: {e}")

    print(f"    Calculator: {created} created, {skipped} already synced")

    # 4. Update Journal with daily totals
    try:
        summary = yazio_get_daily_summary(yazio_token, target_date)
        goals = summary.get("goals", {})
        meals = summary.get("meals", {})
        weight = summary.get("user", {}).get("current_weight", 0)

        total = {"energy": 0, "protein": 0, "carbs": 0, "fat": 0}
        for meal_key in ("breakfast", "lunch", "dinner", "snack"):
            nutrients = meals.get(meal_key, {}).get("nutrients", {})
            total["energy"] += nutrients.get("energy.energy", 0)
            total["protein"] += nutrients.get("nutrient.protein", 0)
            total["carbs"] += nutrients.get("nutrient.carb", 0)
            total["fat"] += nutrients.get("nutrient.fat", 0)

        nutrition = {
            "calories": round(total["energy"]),
            "calories_goal": round(goals.get("energy.energy", 0)),
            "protein": round(total["protein"]),
            "protein_goal": round(goals.get("nutrient.protein", 0)),
            "carbs": round(total["carbs"]),
            "carbs_goal": round(goals.get("nutrient.carb", 0)),
            "fat": round(total["fat"]),
            "fat_goal": round(goals.get("nutrient.fat", 0)),
            "weight": round(weight, 1),
        }

        update_journal(journal_page_id, nutrition)
        print(f"    Journal: {nutrition['calories']} kcal | P:{nutrition['protein']}g C:{nutrition['carbs']}g F:{nutrition['fat']}g | W:{nutrition['weight']}kg")
        return True

    except Exception as e:
        print(f"    Journal update error: {e}")
        return False


def main():
    print("YAZIO -> Notion Full Sync")
    print("=" * 50)

    # Parse args
    dates_to_sync = []
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg == "--backfill":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
            today = date.today()
            dates_to_sync = [(today - timedelta(days=i)).isoformat() for i in range(days)]
        else:
            dates_to_sync = [arg]
    else:
        dates_to_sync = [date.today().isoformat()]

    # Login
    print("Logging into YAZIO...")
    try:
        token = yazio_login()
        print("  OK Authenticated\n")
    except Exception as e:
        print(f"  FAIL Login failed: {e}")
        sys.exit(1)

    # Roll over yesterday's entries from "Today" view to "History" view
    # by unlinking their Today relation
    cleanup_stale_today_relations()

    # Sync
    success = 0
    for d in dates_to_sync:
        if sync_date(d, token):
            success += 1

    print(f"\nDone: {success}/{len(dates_to_sync)} synced")


if __name__ == "__main__":
    main()
