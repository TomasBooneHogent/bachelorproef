import os
import csv
import tempfile
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# 1. Configuratie
PROJECT_ID = "corpscore-be"
DATASET_ID = "kbo"
KEY_PATH = "../src/bigquery/corpscore-be-1ae72f82c936.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH
client = bigquery.Client(project=PROJECT_ID)

SCHEMAS = {
    "dim_enterprise": [
        bigquery.SchemaField("EnterpriseNumber", "STRING"),
        bigquery.SchemaField("Status", "STRING"),
        bigquery.SchemaField("JuridicalSituation", "STRING"),
        bigquery.SchemaField("TypeOfEnterprise", "STRING"),
        bigquery.SchemaField("JuridicalForm", "STRING"),
        bigquery.SchemaField("JuridicalFormCAC", "STRING"),
        bigquery.SchemaField("StartDate", "DATE"), # Script zal dit herkennen
    ],
    "dim_address": [
        bigquery.SchemaField("EntityNumber", "STRING"), # Let op: CSV header naam gebruiken
        bigquery.SchemaField("TypeOfAddress", "STRING"),
        bigquery.SchemaField("CountryNL", "STRING"),
        bigquery.SchemaField("CountryFR", "STRING"),
        bigquery.SchemaField("Zipcode", "STRING"),
        bigquery.SchemaField("MunicipalityNL", "STRING"),
        bigquery.SchemaField("MunicipalityFR", "STRING"),
        bigquery.SchemaField("StreetNL", "STRING"),
        bigquery.SchemaField("StreetFR", "STRING"),
        bigquery.SchemaField("HouseNumber", "STRING"),
        bigquery.SchemaField("Box", "STRING"),
        bigquery.SchemaField("ExtraAddressInfo", "STRING"),
        bigquery.SchemaField("DateStrikingOff", "DATE"), # Script zal dit herkennen
    ],
    "dim_activity": [
        bigquery.SchemaField("EntityNumber", "STRING"),
        bigquery.SchemaField("ActivityGroup", "STRING"),
        bigquery.SchemaField("NaceVersion", "STRING"),
        bigquery.SchemaField("NaceCode", "STRING"),
        bigquery.SchemaField("Classification", "STRING"),
        # Activiteiten hebben meestal geen datum, maar als er een was, zou hij hier moeten staan
    ],
    "dim_denomination": [
        bigquery.SchemaField("EntityNumber", "STRING"),
        bigquery.SchemaField("Language", "STRING"),
        bigquery.SchemaField("TypeOfDenomination", "STRING"),
        bigquery.SchemaField("Denomination", "STRING"),
    ],
    "dim_code": [
        bigquery.SchemaField("Category", "STRING"),
        bigquery.SchemaField("Code", "STRING"),
        bigquery.SchemaField("Language", "STRING"),
        bigquery.SchemaField("Description", "STRING"),
    ],
"dim_nace_mapping": [
        bigquery.SchemaField("Level", "STRING"),
        bigquery.SchemaField("Code", "STRING"),
        bigquery.SchemaField("Sector", "STRING"),
    ],
}

FILES_TO_UPLOAD = {
    "dim_nace_mapping": "nace_mapping.csv",
    # "dim_activity": "activity.csv",
    # "dim_denomination": "denomination.csv"
}

def get_date_fields(table_id):
    """Haalt een lijst op van kolomnamen die van het type DATE zijn volgens het schema."""
    schema = SCHEMAS.get(table_id, [])
    return [field.name for field in schema if field.field_type == 'DATE']

def preprocess_csv_dates(input_path, date_columns):
    """
    Leest de CSV (ISO-8859-1), zoekt naar kolommen in date_columns,
    converteert DD-MM-YYYY naar YYYY-MM-DD en schrijft een tijdelijke UTF-8 CSV.
    """
    # Maak een temp file die blijft bestaan na sluiten (nodig voor Windows/BigQuery client)
    temp_file = tempfile.NamedTemporaryFile(mode='w', newline='', encoding='utf-8', delete=False)

    print(f"⚙️  Converteren van datums {date_columns} in {input_path}...")

    with open(input_path, mode='r', encoding='ISO-8859-1') as infile, temp_file as outfile:
        reader = csv.DictReader(infile)

        # Controleer of de datumkolommen uit het schema daadwerkelijk in de CSV headers zitten
        valid_date_cols = [col for col in date_columns if col in reader.fieldnames]

        if not valid_date_cols and date_columns:
            print(f"⚠️  Let op: Datumvelden {date_columns} niet gevonden in CSV headers: {reader.fieldnames}")

        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            for col in valid_date_cols:
                raw_date = row.get(col, '')
                # Als waarde bestaat en '-' bevat (bv 09-08-1960)
                if raw_date and '-' in raw_date:
                    parts = raw_date.split('-')
                    if len(parts) == 3:
                        # DD-MM-YYYY -> YYYY-MM-DD
                        row[col] = f"{parts[2]}-{parts[1]}-{parts[0]}"

            writer.writerow(row)

    return temp_file.name

def upload_csv(table_id, csv_path):
    if not os.path.exists(csv_path):
        print(f"⚠️  Overslaan {table_id}: Bestand {csv_path} niet gevonden.")
        return

    # 1. Detecteer datumvelden uit het schema
    date_cols = get_date_fields(table_id)

    # 2. Bepaal welk bestand we uploaden (origineel of geconverteerd)
    path_to_upload = csv_path
    processed = False

    if date_cols:
        # Als er datumvelden zijn, voer de converter uit
        path_to_upload = preprocess_csv_dates(csv_path, date_cols)
        processed = True

    # 3. Upload configuratie
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    schema = SCHEMAS.get(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        autodetect=False, # We vertrouwen op ons strakke schema
        write_disposition="WRITE_TRUNCATE",
        # Als we geconverteerd hebben is het UTF-8, anders origineel ISO-8859-1
        encoding="UTF-8" if processed else "ISO-8859-1",
        allow_quoted_newlines=True, # Voor adressen met enters
        ignore_unknown_values=True
    )

    print(f"⏳ Uploaden naar {table_ref}...")

    try:
        with open(path_to_upload, "rb") as source_file:
            load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        load_job.result()
        table = client.get_table(table_ref)
        print(f"✅ Succes! {table.num_rows} rijen geladen in {table_id}.")

    except GoogleAPIError as e:
        print(f"❌ Fout bij uploaden {table_id}: {e}")
        if hasattr(e, 'errors'):
            print("Details:", e.errors)

    finally:
        # Ruim het tijdelijke bestand op als we er een gemaakt hebben
        if processed and path_to_upload != csv_path:
            os.remove(path_to_upload)

if __name__ == "__main__":
    for table, path in FILES_TO_UPLOAD.items():
        upload_csv(table, path)