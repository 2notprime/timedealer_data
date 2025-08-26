import json

file_path = r"D:\TimeDealer\2025_export_data.json"

with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)  # data bây giờ là dict hoặc list

print(type(data))  # xem kiểu dữ liệu (dict/list)
print(data[0])

