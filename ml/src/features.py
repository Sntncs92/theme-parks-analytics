import pandas as pd
import numpy as np


FEATURES = [
    'hour', 'dayofweek', 'month', 'is_weekend', 'is_holiday',
    'has_event', 'park_encoded', 'tier',
    'lag_1', 'lag_4', 'lag_96', 'lag_672',
    'roll_mean_4', 'roll_mean_96', 'roll_max_4'
]


def build_features(df: pd.DataFrame, park_mapping: dict = None) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values(['ride_id', 'timestamp']).reset_index(drop=True)

    df['hour'] = df['timestamp'].dt.hour.astype('int8')
    df['dayofweek'] = df['timestamp'].dt.dayofweek.astype('int8')
    df['month'] = df['timestamp'].dt.month.astype('int8')
    df['is_weekend'] = (df['dayofweek'] >= 5).astype('int8')
    df['tier'] = df['tier'].astype('int8')
    df['is_holiday'] = df['is_holiday'].astype('int8')

    df['park_name'] = df['park_name'].astype('category')

    if park_mapping:
        df['park_encoded'] = df['park_name'].map(park_mapping).fillna(0).astype('int8')
    else:
        df['park_encoded'] = df['park_name'].cat.codes.astype('int8')

    grp = df.groupby('ride_id')['wait_time']

    df['lag_1']   = grp.shift(1)
    df['lag_4']   = grp.shift(4)
    df['lag_96']  = grp.shift(96)
    df['lag_672'] = grp.shift(672)

    df['roll_mean_4']  = grp.transform(lambda x: x.shift(1).rolling(4).mean())
    df['roll_mean_96'] = grp.transform(lambda x: x.shift(1).rolling(96).mean())
    df['roll_max_4']   = grp.transform(lambda x: x.shift(1).rolling(4).max())

    return df


def drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas sin lags suficientes.
    Separado de build_features para poder usar el DataFrame
    completo antes de filtrar (útil en predicción).
    """
    return df.dropna(subset=['lag_1', 'lag_4', 'lag_96']).reset_index(drop=True)