import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path

# Files
INPUT = Path(__file__).resolve().parent.parent / "data" / "data.xlsx"
OUT   = Path(__file__).resolve().parent.parent / "data" / "cleaned_data.xlsx"
LOG   = Path(__file__).resolve().parent / "clean.log"

# Logging
logging.basicConfig(filename=str(LOG), level=logging.INFO,
                    format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger()


def read_data(path):
    logger.info(f"Reading {path}")
    return pd.read_excel(path)


def clean_string_columns(df):
    for c in df.select_dtypes(include=['object']).columns:
        df[c] = df[c].astype(str).str.strip()
        df[c] = df[c].replace({"": np.nan, "nan": np.nan})
    return df


def coerce_columns_to_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            s = df[c].astype(str).str.strip()
            num = s.str.extract(r'(-?\d+\.?\d*)', expand=False)
            df[c] = pd.to_numeric(num, errors='coerce')
    return df


def drop_high_missing(df, row_thresh=0.5, col_thresh=0.9):
    rows_before = len(df)
    df = df.loc[df.isnull().mean(axis=1) <= row_thresh].copy()
    logger.info(f"Dropped {rows_before - len(df)} rows with >{row_thresh*100}% missing")

    cols_before = df.shape[1]
    keep_cols = df.columns[df.isnull().mean() <= col_thresh]
    df = df[keep_cols].copy()
    logger.info(f"Dropped {cols_before - df.shape[1]} columns with >{col_thresh*100}% missing")
    return df


def remove_duplicates(df):
    before = len(df)
    df = df.drop_duplicates()
    logger.info(f"Removed {before - len(df)} duplicate rows")
    return df


def impute_missing(df, numeric_strategy='median'):
    num_cols = df.select_dtypes(include=[np.number]).columns
    cat_cols = df.select_dtypes(exclude=[np.number]).columns

    for c in num_cols:
        if numeric_strategy == 'median':
            df[c] = df[c].fillna(df[c].median())
        else:
            df[c] = df[c].fillna(df[c].mean())

    for c in cat_cols:
        mode = df[c].mode(dropna=True)
        if not mode.empty:
            df[c] = df[c].fillna(mode[0])
        else:
            df[c] = df[c].fillna("Unknown")
    return df


def cap_outliers_iqr(df):
    num_cols = df.select_dtypes(include=[np.number]).columns
    for c in num_cols:
        Q1 = df[c].quantile(0.25)
        Q3 = df[c].quantile(0.75)
        IQR = Q3 - Q1
        lower, upper = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
        df[c] = df[c].clip(lower, upper)
    return df


def fix_id_column(df):
    if "id" in df.columns:
        df["id"] = df["id"].astype(str).str.strip()
        logger.info("Converted column 'id' to string type.")
        dup_count = df["id"].duplicated().sum()
        if dup_count > 0:
            logger.warning(f"Column 'id' has {dup_count} duplicate values.")
    return df


def main():
    if not INPUT.exists():
        logger.error(f"Input file {INPUT} not found. Put your data.xlsx here.")
        sys.exit(1)

    df = read_data(INPUT)
    logger.info(f"Original shape: {df.shape}")
    logger.info(f"Columns and dtypes:\n{df.dtypes}")
    df = fix_id_column(df)

    df = clean_string_columns(df)

    maybe_numeric = ['age', 'squad_number']
    df = coerce_columns_to_numeric(df, maybe_numeric)

    df = drop_high_missing(df, row_thresh=0.5, col_thresh=0.9)

    df = remove_duplicates(df)

    df = impute_missing(df, numeric_strategy='median')

    #  Xử lí ngoại lai
    df = cap_outliers_iqr(df)

    logger.info(f"Final shape: {df.shape}")
    logger.info("Missing per column after cleaning:")
    logger.info(df.isnull().sum().to_dict())
    logger.info("dtypes after cleaning:")
    logger.info(df.dtypes.to_dict())

    # Save xlsx
    df.to_excel(OUT, index=False)
    logger.info(f"Saved cleaned xlsx: {OUT}")
    print(f"Done. Cleaned file saved as: {OUT}")
    print(f"Log saved as: {LOG}")


if __name__ == "__main__":
    main()