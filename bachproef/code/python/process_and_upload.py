import os
import pandas as pd
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from api.AuthenticApiClient import AuthenticApiClient
from bigquery.BigQueryClient import BigQueryClient

# --- CONFIGURATION ---
CSV_PATH = "enterprises/enterprise.csv"
DATASET_ID = "accounts"
YEARS_TO_PROCESS = ["2020"]
MAX_WORKERS = 40
BATCH_SIZE = 150
CHECKPOINT_PATH = "processed_entities.txt"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

rows_accumulator = []
accumulator_lock = threading.Lock()
checkpoint_lock = threading.Lock()

def mark_as_processed(entity_ids):
    """Appends successfully uploaded IDs to the checkpoint file."""
    with checkpoint_lock:
        with open(CHECKPOINT_PATH, "a") as f:
            for eid in entity_ids:
                f.write(f"{eid}\n")

def upload_batch(rows, bq_client, year):
    """Uploads data and ONLY THEN marks entities as processed."""
    if not rows: return
    try:
        df_upload = pd.DataFrame(rows)
        table_id = f"{DATASET_ID}.{year}"

        # This blocks until the upload is successful
        bq_client.write_to_bigquery(df_upload, table_id)

        # SUCCESS: Now we can safely mark these entities as processed
        # We extract unique EnterpriseNumbers from the batch to update the checkpoint
        uploaded_entities = df_upload['EnterpriseNumber'].unique()
        mark_as_processed(uploaded_entities)

        logger.info(f"🚀 Batched Upload: {len(rows)} records. {len(uploaded_entities)} entities marked as done.")
    except Exception as e:
        # If this fails, the entities are NOT marked as processed and will be retried on restart
        logger.error(f"❌ Batch Upload Failed (Data preserved for retry): {e}")

def process_and_upload_entity(entity, year, api_client, bq_client):
    try:
        accounts = api_client.get_accounts(entity, year)
        if not accounts:
            # If no accounts found, mark as processed so we don't keep checking empty entities
            mark_as_processed([entity])
            return

        entity_rows = []
        for data, reference in accounts:
            row = {
                "ReferenceNumber": data.get("referenceNumber"),
                "DepositDate" : reference.get("DepositDate"),
                "StartDate" : reference.get("ExerciseDates", {}).get("startDate"),
                "EndDate" : reference.get("ExerciseDates", {}).get("endDate"),
                "ModelType" : reference.get("ModelType"),
                "DepositType" : reference.get("DepositType"),
                "Language" : reference.get("Language"),
                "Currency" : reference.get("Currency"),
                "EnterpriseNumber" : entity, # Use the original entity ID for the checkpoint link
                "EnterpriseName": reference.get("EnterpriseName"),
                "Address": reference.get("Address"),
                "LegalForm": reference.get("LegalForm"),
                "LegalSituation": reference.get("LegalSituation"),
                "FullFillLegalValidation": reference.get("FullFillLegalValidation"),
                "ActivityCode": reference.get("ActivityCode"),
                "AccountingDataURL" : reference.get("AccountingDataURL"),
                "ImprovementDate" : reference.get("ImprovementDate"),
                "CorrectedData" : reference.get("CorrectedData"),
                "DataVersion": reference.get("DataVersion"),
                "EnterpriseName_historic": data.get("EnterpriseName"),
                "Address_historic": data.get("Address"),
                "LegalForm_historic": data.get("LegalForm"),
                "Rubrics": data.get("Rubrics", [])
            }
            entity_rows.append(row)

        with accumulator_lock:
            rows_accumulator.extend(entity_rows)
            if len(rows_accumulator) >= BATCH_SIZE:
                batch = rows_accumulator[:BATCH_SIZE]
                del rows_accumulator[:BATCH_SIZE]
                # Start upload in a background thread to keep workers fetching
                threading.Thread(target=upload_batch, args=(batch, bq_client, year)).start()

    except Exception as e:
        logger.error(f"❌ Error fetching {entity}: {e}")

def load_processed_entities():
    if not os.path.exists(CHECKPOINT_PATH): return set()
    with open(CHECKPOINT_PATH, "r") as f:
        return set(line.strip() for line in f)

def main():
    api_client = AuthenticApiClient(pool_size=MAX_WORKERS)
    bq_client = BigQueryClient()
    processed_entities = load_processed_entities()

    try:
        df_csv = pd.read_csv(CSV_PATH)
        all_entities = [str(e) for e in df_csv['EnterpriseNumber'].unique()]
        entities_to_process = [e for e in all_entities if e not in processed_entities]
        logger.info(f"Total to process: {len(entities_to_process)} (Resuming from {len(processed_entities)})")
    except Exception as e:
        logger.error(f"CSV Load Error: {e}"); return

    for year in YEARS_TO_PROCESS:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for entity in entities_to_process:
                executor.submit(process_and_upload_entity, entity, year, api_client, bq_client)

        # Final Flush for the last remaining records
        with accumulator_lock:
            if rows_accumulator:
                upload_batch(rows_accumulator, bq_client, year)
                rows_accumulator.clear()

if __name__ == "__main__":
    main()