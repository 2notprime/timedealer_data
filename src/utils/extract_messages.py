import json
import re
import time
import os
import random
from dotenv import load_dotenv
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import google.generativeai as genai



load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

PROMPT = """
You are an AI assistant specialized in analyzing luxury watches from text messages and images.
TASK: First, check if message relates to luxury watches (mentions brands from list, 'watch', 'timepiece', or codes like '15210ST', 'RM' or something else). If not (e.g., bags like Hermes B25, jewelry, clothing, chat), return single object: "transaction" as one-word English category (e.g., 'handbag', 'jewelry', 'clothing', 'chat' or something else); others null; "note" empty.
Only for watches: Extract all models as JSON list of objects. Use similarity (>80% brands, >90% colors) with lists; ref as alpha-numeric >=4 chars, >=2 digits, not year/price/meaningful word; prices remove punctuation, expand k=000/m=000000; combine colors (e.g., 'Fullgold blue'); condition from 'new'/'used' variants; unclassified to "note". Consider header (no price/model) for common brand/condition.
JSON STRUCTURE FOR EACH OBJECT:
{
"transaction": "string",
"ref": "string",
"brand": "string",
"color": "string",
"price": "string | null",
"currency": "string | null",
"year": "string | null",
"condition": "string",
"note": "string"
}
WATCH BRANDS (match exact/similar/abbrev/parts; e.g., 'AP'->'Audemars Piguet', 'RM' prefix):
["Rolex","Patek Philippe","Audemars Piguet","Jaeger-LeCoultre","Vacheron Constantin","Cartier","Omega","Blancpain","Breguet","Hublot","Richard Mille","Ulysse Nardin","Franck Muller","IWC Schaffhausen","Breitling","Tudor","Piaget","Santos","Chopard","Glashutte","Roger Dubuis","Zenith","Chanel","Girard-Perregaux","JLC","Panerai","François-Paul Journe"]
Avoid non-watch (e.g., Hermes, Chanel fashion).
COLORS (match >90%, combine prefixes 'Fullgold','ice','gold'):
["black","blk","sundust","sun","white","wht","blue","mop","rhodium","motif","choco","cho","pink","dark grey","olive","green","salmon","brown","grey","mete","sliver","wim","champ","purple","Fullgold brown","Fullgold blue","Fullgold black","Fullgold blk","Fullgold white","ice blue","pink","Bubba"]
Avoid non-watch (e.g., 'etoupe').
RULES:
1. transaction: "wtb" (buy, incl. "ntq"/"Need to Quote"); "forsale" (sell). Non-watch: one-word like 'handbag', 'bag', 'jewelry', 'clothing', 'chat', 'unrelated' (fallback).
2. brand: First match applies to subsequent. Use list; infer codes (e.g., 'RM'->"Richard Mille",'Pam'->"Panerai"). Shorten multi-word for match.
3. Per watch:
- ref: Alpha-num code >=4 chars, >=2 digits; exclude year/price/words.
- color: From list, combine if prefixed.
- year: 4-digit prio; convert "n724"->"7/2024","102024"->"10/2024","2024"->"2024"; formats '/'; "N24"->condition "new".
- price: Numeric full (e.g., "143k"->"143000","1,435.00"->"143500","23500" remains); consecutive digits + 'k'/'m'/'00'; discounts % to "note".
- currency: Separate (e.g., "$"->"USD","HKD","AED","Euro").
- condition: only classify into 2 options: 'new'/'used'.
- note: Unclassified, discounts; empty for non-watch.
4. General: Null missing; pure JSON only; single object non-watch.
Examples:
- Non-watch: "B25 etoupe phw togo K 23500 USD" -> [{"transaction": "handbag", "ref": null, "brand": null, "color": null, "price": null, "currency": null, "year": null, "condition": null, "note": ""}]
-Watch: "AP STOCK\n15210ST Blue new 2023 143k USD" -> [{"transaction": "forsale", "ref": "15210ST", "brand": "Audemars Piguet", "color": "Blue", "price": "143000", "currency": "USD", "year": "2023", "condition": "new", "note": ""}]
- Non-watch: "Need Hermes B25 black" -> [{"transaction": "bag", "ref": null, "brand": null, "color": null, "price": null, "currency": null, "year": null, "condition": null, "note": ""}]
- Non-watch chat: "Hi how are you" -> [{"transaction": "chat", "ref": null, "brand": null, "color": null, "price": null, "currency": null, "year": null, "condition": null, "note": ""}]
- Watch multi: "Rolex: 126000 Green used n724 120k HKD; 15210ST Blue new 2023 143k USD extra" -> [{"transaction": "forsale", "ref": "126000", "brand": "Rolex", "color": "Green", "price": "120000", "currency": "HKD", "year": "7/2024", "condition": "used", "note": ""}, {"transaction": "forsale", "ref": "15210ST", "brand": "Audemars Piguet", "color": "Blue", "price": "143000", "currency": "USD", "year": "2023", "condition": "new", "note": "extra"}]
- Watch discount: "All watches 10% off, Rolex 126000 120k USD" -> [{"transaction": "forsale", "ref": "126000", "brand": "Rolex", "color": null, "price": "120000", "currency": "USD", "year": null, "condition": null, "note": "All watches 10% off"}]
- Mixed: "Hermes bag B25 23500 USD and Rolex watch 126000" -> [{"transaction": "forsale", "ref": "126000", "brand": "Rolex", "color": null, "price": null, "currency": null, "year": null, "condition": null, "note": ""}]
------
Original message:
"""

def extract_json_from_response(text: str) -> List[Dict]:
    match = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
    if match:
        json_string = match.group(1).strip()
    else:
        json_string = text.strip()
    try:
        parsed = json.loads(json_string)
        if not isinstance(parsed, list):
            parsed = [parsed]
        return parsed
    except json.JSONDecodeError:
        return []

def process_chunk(chunk: str, total_input_tokens, total_output_tokens) -> List[Dict]:
    content = PROMPT + "\n" + chunk
    attempts = 0
    while attempts < 3:
        try:
            response = model.generate_content(content)
            total_input_tokens[0] += response.usage_metadata.prompt_token_count
            total_output_tokens[0] += response.usage_metadata.candidates_token_count
            return extract_json_from_response(response.text)
        except Exception as e:
            print(f"Error: {str(e)}")
            attempts += 1
            if attempts < 3:
                time.sleep(2 ** attempts + random.uniform(0, 1))
    return []

def analyze_message(message_raw: str) -> List[Dict]:
    """Phân tích tin nhắn để extract thông tin đồng hồ, với chunking dựa trên dòng và xử lý parallel."""
    if not message_raw.strip():
        return []
    
    chunk_size = 1000
    
    lines = message_raw.splitlines()
    
    chunks = []
    current_chunk_lines = []
    current_length = 0
    
    for line in lines:
        line_length = len(line) + 1
        
        if line_length > chunk_size:
            if current_chunk_lines:
                chunks.append('\n'.join(current_chunk_lines))
            chunks.append(line)
            current_chunk_lines = []
            current_length = 0
            continue
        
        if current_length + line_length > chunk_size and current_chunk_lines:
            chunks.append('\n'.join(current_chunk_lines))
            current_chunk_lines = []
            current_length = 0
        
        current_chunk_lines.append(line)
        current_length += line_length
    
    if current_chunk_lines:
        chunks.append('\n'.join(current_chunk_lines))
    
    all_parsed = []
    total_input_tokens = [0]
    total_output_tokens = [0]
    if chunks:
        with ThreadPoolExecutor(max_workers=min(5, len(chunks))) as executor:
            futures = [executor.submit(process_chunk, chunk, total_input_tokens, total_output_tokens) for chunk in chunks]
            for future in as_completed(futures):
                all_parsed.extend(future.result())
    
    print(f"Total input tokens: {total_input_tokens[0]}")
    print(f"Total output tokens: {total_output_tokens[0]}")
    return all_parsed

if __name__ == "__main__":
    test_message = """ 
New 5822p N82025y full set 124m ready hk

    """
    result = analyze_message(test_message)
    if result:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print("Không có kết quả.")