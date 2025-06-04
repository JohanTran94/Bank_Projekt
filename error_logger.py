import os
import json
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from models import ErrorRow

LOG_DIR = "_logs"
os.makedirs(LOG_DIR, exist_ok=True)

def serialize_for_json(obj):
    if isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serialize_for_json(i) for i in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

def log_error_row(session: Session, context: str, reason: str, row: dict, csv_file: str):
    clean_row = serialize_for_json(row)

    # Log to database
    error = ErrorRow(
        context=context,
        error_reason=reason,
        raw_data=clean_row,
        logged_at=datetime.utcnow()
    )
    try:
        session.add(error)
        session.commit()
        print("🟡 Error logged to database")
    except Exception as e:
        session.rollback()
        print(f"🔴 Failed to log error to database: {e}")

    # Log to CSV file
    file_path = os.path.join(LOG_DIR, csv_file)

    df = pd.DataFrame([{
        "context": context,
        "error_reason": reason,
        "raw_data": json.dumps(clean_row),
        "logged_at": datetime.utcnow().isoformat()
    }])

    try:
        if os.path.exists(file_path):
            df.to_csv(file_path, mode="a", header=False, index=False)
        else:
            df.to_csv(file_path, index=False)
        print("🟡 Error logged to CSV")
    except Exception as e:
        print(f"🔴 Failed to log error to CSV: {e}")

