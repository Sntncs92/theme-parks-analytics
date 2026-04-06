import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib
from sqlalchemy import create_engine
from sklearn.metrics import mean_absolute_error
from features import FEATURES, build_features, drop_nulls
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / '.env')

# --- Configuración ---
DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
MODEL_PATH = Path(__file__).parent.parent / "models" / "lgbm_v1.pkl"

TRAIN_END = pd.Timestamp('2026-02-01', tz='UTC')
VAL_END   = pd.Timestamp('2026-03-01', tz='UTC')

PARAMS = {
    'objective':        'regression_l1',
    'metric':           'mae',
    'learning_rate':    0.05,
    'num_leaves':       127,
    'min_data_in_leaf': 50,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq':     5,
    'verbose':          -1
}


def load_data(engine) -> pd.DataFrame:
    query = """
        SELECT
            wt.ride_id,
            p.park_name,
            p.country,
            r.ride_name,
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
        WHERE wt.status = 'OPERATING'
        AND wt.wait_time IS NOT NULL
        AND wt.wait_time BETWEEN 1 AND 120
        ORDER BY wt.timestamp
"""
    df = pd.read_sql(query, engine, parse_dates=['timestamp'])
    df['wait_time'] = df['wait_time'].astype('int16')
    df['has_event'] = df['has_event'].astype('int8')
    df['ride_id'] = df['ride_id'].astype('category')
    df['tier'] = df['tier'].astype('int8')
    df['is_holiday'] = df['is_holiday'].astype('int8')
    return df


def train():
    print("Conectando a la base de datos...")
    engine = create_engine(DB_URL)

    print("Cargando datos...")
    df = load_data(engine)
    print(f"  {len(df):,} filas cargadas")

    print("Construyendo features...")
    df = build_features(df)
    df = drop_nulls(df)
    print(f"  {len(df):,} filas tras feature engineering")

    # Split temporal
    train_df = df[df['timestamp'] < TRAIN_END]
    val_df   = df[(df['timestamp'] >= TRAIN_END) & (df['timestamp'] < VAL_END)]

    X_train, y_train = train_df[FEATURES], train_df['wait_time']
    X_val,   y_val   = val_df[FEATURES],   val_df['wait_time']

    print(f"  Train: {len(X_train):,} · Val: {len(X_val):,}")

    print("Entrenando LightGBM...")
    dtrain = lgb.Dataset(X_train, label=y_train)
    dval   = lgb.Dataset(X_val,   label=y_val, reference=dtrain)

    model = lgb.train(
        PARAMS,
        dtrain,
        num_boost_round=2000,
        valid_sets=[dval],
        callbacks=[lgb.early_stopping(50), lgb.log_evaluation(200)]
    )

    val_preds = model.predict(X_val)
    mae = mean_absolute_error(y_val, val_preds)
    within_10 = np.mean(np.abs(y_val - val_preds) <= 10) * 100

    print(f"\nResultados en validación:")
    print(f"  MAE:     {mae:.2f} min")
    print(f"  ±10 min: {within_10:.1f}%")
    print(f"  Rondas:  {model.best_iteration}")

    print(f"\nGuardando modelo en {MODEL_PATH}...")
    joblib.dump(model, MODEL_PATH)
    print("Listo.")


if __name__ == "__main__":
    train()