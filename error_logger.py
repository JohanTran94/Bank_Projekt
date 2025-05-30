import os
import json
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from models import ErrorRow

class ErrorLogger:
    def __init__(self, session: Session, log_dir="_logs"):
        self.session = session
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)

    def serialize_for_json(self, obj):
        if isinstance(obj, dict):
            return {k: self.serialize_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.serialize_for_json(i) for i in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj

    def log_error_row(self, context: str, reason: str, row: dict, csv_file: str):
        clean_row = self.serialize_for_json(row)

        # Logga till databas
        error = ErrorRow(
            context=context,
            error_reason=reason,
            raw_data=clean_row,
            logged_at=datetime.utcnow()
        )
        self.session.add(error)
        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            print(f"⚠️  Kunde inte logga fel till DB: {e}")

        # Logga till CSV
        file_path = os.path.join(self.log_dir, csv_file)
        df = pd.DataFrame([{
            "context": context,
            "error_reason": reason,
            "raw_data": json.dumps(clean_row),
            "logged_at": datetime.utcnow().isoformat()
        }])

        if os.path.exists(file_path):
            df.to_csv(file_path, mode="a", header=False, index=False)
        else:
            df.to_csv(file_path, index=False)
