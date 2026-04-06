import pandas as pd
import numpy as np
import joblib
from sqlalchemy import create_engine
from features import FEATURES, build_features
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / '.env')

# --- Configuración ---
DB_URL     = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
MODEL_PATH = Path(__file__).parent.parent / "models" / "lgbm_v1.pkl"
PARK_MAPPING_PATH = Path(__file__).parent.parent / "models" / "park_mapping.pkl"


def load_model():
    return joblib.load(MODEL_PATH)


def get_recent_data(engine, ride_id: str, n: int = 700) -> pd.DataFrame:
    """
    Carga las últimas n mediciones de un ride concreto.
    Necesitamos suficiente historial para calcular lag_672 (7 días).
    """
    query = f"""
        SELECT
            wt.ride_id,
            p.park_name,
            p.country,
            r.tier,
            wt.timestamp,
            wt.wait_time,
            CASE WHEN wt.evento != '' THEN 1 ELSE 0 END as has_event,
            CASE WHEN h.holiday_id IS NOT NULL THEN 1 ELSE 0 END as is_holiday
        FROM wait_times wt
        JOIN rides r ON wt.ride_id = r.ride_id
        JOIN parks p ON r.park_id = p.park_id
        LEFT JOIN holidays h
            ON h.country = p.country
            AND h.date = DATE(wt.timestamp)
        WHERE wt.ride_id = '{ride_id}'
        AND wt.status = 'OPERATING'
        AND wt.wait_time IS NOT NULL
        ORDER BY wt.timestamp DESC
        LIMIT {n}
    """
    df = pd.read_sql(query, engine, parse_dates=['timestamp'])
    df['wait_time'] = df['wait_time'].astype('int16')
    df['has_event'] = df['has_event'].astype('int8')
    df['ride_id']   = df['ride_id'].astype('category')
    df['tier'] = df['tier'].astype('int8')
    df['is_holiday'] = df['is_holiday'].astype('int8')

    return df.sort_values('timestamp').reset_index(drop=True)



def predict_next(ride_id: str, target_datetime: pd.Timestamp) -> dict:
    engine       = create_engine(DB_URL)
    model        = load_model()
    park_mapping = joblib.load(PARK_MAPPING_PATH)

    df = get_recent_data(engine, ride_id, n=800)

    if len(df) < 100:
        return {'error': f'Datos insuficientes para ride_id {ride_id}'}

    # Construir features sobre el historial real
    df = build_features(df, park_mapping=park_mapping)
    df = df.dropna(subset=['lag_1', 'lag_4', 'lag_96'])

    if len(df) == 0:
        return {'error': 'No hay suficiente historial para calcular lags'}

    # Tomar la última fila real y sobreescribir las features temporales
    # con los valores del momento a predecir
    row = df.iloc[[-1]][FEATURES].copy()
    row['hour']       = target_datetime.hour
    row['dayofweek']  = target_datetime.dayofweek
    row['month']      = target_datetime.month
    row['is_weekend'] = int(target_datetime.dayofweek >= 5)


    prediction = float(model.predict(row)[0])
    prediction = max(0, round(prediction, 1))

    return {
        'ride_id':         ride_id,
        'target_datetime': target_datetime.isoformat(),
        'predicted_wait':  prediction,
        'unit':            'minutes'
    }


if __name__ == "__main__":
    
    result = predict_next(
        ride_id='5a43d1a7-ad53-4d25-abfe-25625f0da304',
        target_datetime=pd.Timestamp.now(tz='UTC')
    )
    print(result)