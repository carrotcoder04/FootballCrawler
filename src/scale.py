from pathlib import Path
import pandas as pd
import logging
from sklearn.preprocessing import StandardScaler, MinMaxScaler

# Đường dẫn file
INPUT = Path(__file__).resolve().parent.parent / "data" / "cleaned_data.xlsx"
OUT_STD = Path(__file__).resolve().parent.parent / "data" / "scaled_standard.xlsx"
OUT_MM = Path(__file__).resolve().parent.parent / "data" / "scaled_minmax.xlsx"
LOG = Path(__file__).resolve().parent / "scale.log"

# Logging
logging.basicConfig(
    filename=LOG,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def scale_data(input_file, out_std, out_mm):
    try:
        # Đọc dữ liệu
        df = pd.read_excel(input_file)

        # Xác định cột số
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()

        # Nếu có 'id' thì loại bỏ
        if "id" in numeric_cols:
            numeric_cols.remove("id")

        logging.info(f"Các cột số để scale (loại trừ id): {numeric_cols}")

        # StandardScaler
        scaler_std = StandardScaler()
        df_std = df.copy()
        df_std[numeric_cols] = scaler_std.fit_transform(df[numeric_cols])

        # MinMaxScaler
        scaler_mm = MinMaxScaler()
        df_mm = df.copy()
        df_mm[numeric_cols] = scaler_mm.fit_transform(df[numeric_cols])

        # Xuất ra Excel
        df_std.to_excel(out_std, index=False)
        df_mm.to_excel(out_mm, index=False)

        logging.info(f"Scale thành công. Kết quả: {out_std}, {out_mm}")
    except Exception as e:
        logging.error(f"Lỗi trong quá trình scale: {str(e)}")
        raise

if __name__ == "__main__":
    scale_data(INPUT, OUT_STD, OUT_MM)
