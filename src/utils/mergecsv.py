import pandas as pd
import os

folder_path = r"D:\TimeDealer\itemcsv"
output_file = os.path.join(folder_path, r"D:\TimeDealer\data\items.csv")

# 🔹 Lấy danh sách CSV trong folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

if not csv_files:
    print("❌ Không tìm thấy file CSV nào trong folder.")
    exit()

first = True
chunksize = 50000  # số dòng đọc mỗi lần, có thể giảm nếu file cực lớn

with open(output_file, "w", encoding="utf-8-sig", newline="") as f_out:
    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        print(f"➡️ Đang ghép file: {file}")

        # Đọc theo từng chunk để không tràn RAM
        for chunk in pd.read_csv(
            file_path, chunksize=chunksize, low_memory=False, dtype=str
        ):
            chunk.to_csv(f_out, index=False, header=first)
            first = False  # chỉ ghi header 1 lần duy nhất

print(f"✅ Đã ghép {len(csv_files)} file CSV thành công -> {output_file}")
