import os
import json
import numpy as np
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ── Credentials: from env var (GitHub Secret) or local JSON file ──
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")

if creds_json:
    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
else:
    # local dev fallback
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "citifuel-387a0ec421a9.json", scope
    )

client = gspread.authorize(creds)

sheet = client.open_by_key("1EzR2IDRpu7NFfzy_4VLbkEwf0haenJ18_x3KR5lpRVQ")
worksheet = sheet.worksheet("Maintenance")

data = worksheet.get_all_records()
df = pd.DataFrame(data)
print("data loaded to df (connected to sheets)")

# ── Correcting date column ──
df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)

# ── Remove extra spaces ──
df['Customer Service Member'] = df['Customer Service Member'].str.strip()

# ── Filter allowed fleet agents ──
allowed_names = [
    "Tim", "David", "Mason", "Mike", "Eddy", "Alan",
    "Jeff", "Cassie", "Jane", "Daniel", "Tomas",
    "Dave", "Denis", "Mia", "Patrick", "Frank"
]
df = df[df['Customer Service Member'].isin(allowed_names)]

# ── Service points mapping ──
service_points_map = {
    "Eats": 1,
    "Truck Parking": 1,
    "Truck Wash": 1,
    "Parts purchase": 2,
    "Tire Replacement": 2,
    "Tire repair": 1,
    "PMs": 2,
    "DOT inspection": 2.5,
    "Dealership": 3.5,
    "RS/Tire Replacement": 4,
    "RS/Mechanical": 6,
    "Towing": 6,
    "Tire Replacement/PMs": 3,
    "Mechanical": 3,
    "Diagnosting": 3,
    "PMs/Mechanical": 5,
    "Tire Replacement/Mechanical": 5,
    "RS": 4
}
df["Point"] = df["Service type"].map(service_points_map)
print("cleaning done")

# ── Shop bonus ──
company_df = pd.read_csv('company list.csv', encoding='latin1')
company_set = set(company_df["List of Companies"].str.lower().str.strip())
df["is_in_company_list"] = df["Truck Stop"].str.lower().str.strip().isin(company_set).map({True: 0.75, False: 0.25})

df['Bonus'] = np.where(
    df['Status'] == 'In CMP',
    df['Point'] * df['is_in_company_list'],
    0
)
print('added bonus column')

# ── Data fixes ──
card_mask = df['Card number'].apply(lambda x: isinstance(x, str) and not x.strip().isdigit())
mn_mask   = df['MN card'].apply(lambda x: False if pd.isna(x) else not str(x).strip().replace('.', '', 1).isdigit())
cs_mask   = df['Case source'].apply(lambda x: str(x).strip() == '#REF!' if pd.notna(x) else False)

df.loc[card_mask, 'Card number'] = np.nan
df.loc[mn_mask,   'MN card']     = np.nan
df.loc[cs_mask,   'Case source'] = np.nan
print('all errors fixed')

# ── Agent grades ──
agent_grades = {
    'David': 'A', 'Mason': 'A', 'Mike': 'A', 'Eddy': 'A',
    'Alan': 'B',  'Jeff': 'A',  'Cassie': 'C', 'Jane': 'C',
    'Daniel': 'C','Tomas': 'A', 'Dave': 'B',  'Denis': 'A',
    'Mia': 'C',   'Patrick': 'C','Frank': 'C', 'Tim': 'B',
}
df['Grade'] = df['Customer Service Member'].map(agent_grades)

# ── Grade-based service points ──
grade_service_points = {
    'Eats'                      : (1.0, 1.0, 1.0),
    'Truck Parking'             : (0.0, 1.0, 2.0),
    'Truck Wash'                : (0.0, 1.0, 2.0),
    'Parts purchase'            : (1.0, 2.0, 3.0),
    'Tire Replacement'          : (1.0, 1.5, 2.0),
    'Tire repair'               : (0.0, 1.5, 2.0),
    'PMs'                       : (0.5, 1.0, 2.0),
    'DOT inspection'            : (1.0, 2.0, 3.0),
    'Dealership'                : (1.5, 2.5, 4.0),
    'RS/Tire Replacement'       : (2.0, 4.0, 6.0),
    'RS/Mechanical'             : (4.0, 6.0, 8.0),
    'Towing'                    : (4.0, 7.0, 8.0),
    'Tire Replacement/PMs'      : (1.0, 2.0, 4.0),
    'Mechanical'                : (2.0, 3.0, 5.0),
    'Diagnosting'               : (2.0, 3.0, 5.0),
    'PMs/Mechanical'            : (4.0, 7.0, 8.0),
    'Tire Replacement/Mechanical': (4.0, 7.0, 8.0),
}
df['Points_A'] = df['Service type'].map(lambda x: grade_service_points.get(x, (np.nan,)*3)[0])
df['Points_B'] = df['Service type'].map(lambda x: grade_service_points.get(x, (np.nan,)*3)[1])
df['Points_C'] = df['Service type'].map(lambda x: grade_service_points.get(x, (np.nan,)*3)[2])

unmapped_agents   = df[df['Grade'].isna()]['Customer Service Member'].dropna().unique()
unmapped_services = df[df['Points_A'].isna()]['Service type'].dropna().unique()
if len(unmapped_agents):   print("Unmapped agents:",   unmapped_agents)
if len(unmapped_services): print("Unmapped services:", unmapped_services)
print('all done')

# ── Upload cleaned data back to Google Sheets ──
print("uploading to Google Sheets...")

# Prepare: convert dates to string, fill NaN with empty string
df['Date'] = df['Date'].dt.strftime('%d/%m/%Y')
df = df.fillna('')

# Always wipe the 'Cleaned' sheet and reload from scratch (like a full Excel refresh)
sheet_new = client.open_by_key("1siD4yjHDmqVBKkpwTS4ZAnN_p73rC92qAk15yneSn3E")
output_ws = sheet_new.worksheet("Cleaned")
output_ws.clear()                          # delete every existing row
output_ws.update(
    [df.columns.tolist()] + df.values.tolist(),
    value_input_option='RAW'
)                                          # write header + all rows fresh

print("Done! Data uploaded to 'Cleaned' tab in Google Sheets.")