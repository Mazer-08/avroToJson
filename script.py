import json
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader

def avro_to_json(avro_file_paths, json_file_path=None):
    """
    Read data from an Avro file and convert it to JSON
    
    Args:
        avro_file_path (str): Path to the input Avro file
        json_file_path (str, optional): Path to save JSON output. If None, returns JSON string
    
    Returns:
        str or None: JSON string if json_file_path is None, otherwise None
    """
    try:
        records = []
        # Read Avro files
        for avro_file_path in avro_file_paths:
            with open(avro_file_path, 'rb') as avro_file:
                reader = DataFileReader(avro_file, DatumReader())
                
                # Convert all records to a list
                for record in reader:
                    records.append(record)
                
                reader.close()
        
        # Extract Body field if it exists, otherwise use all records
        body_records = []
        for record in records:
            if isinstance(record, dict) and 'Body' in record:
                body_content = record['Body']
                # print(body_content)
                # If Body is a byte string, decode it
                if isinstance(body_content, bytes):
                    body_content = body_content.decode('utf-8')
                
                # If the decoded content is a JSON string, parse it
                if isinstance(body_content, str):
                    try:
                        parsed_body = json.loads(body_content)
                        body_records.extend(parsed_body if isinstance(parsed_body, list) else [parsed_body])
                    except json.JSONDecodeError:
                        # If it's not valid JSON, keep as string
                        body_records.append(body_content)
                else:
                    body_records.append(body_content)
            else:
                body_records.append(record)
        
        # Convert to JSON
        json_data = json.dumps(body_records, indent=2, default=str)
        
        # Save to file or return string
        if json_file_path:
            with open(json_file_path, 'w') as json_file:
                json_file.write(json_data)
            print(f"Successfully converted {avro_file_path} to {json_file_path}")
            return None
        else:
            return json_data
            
    except Exception as e:
        print(f"Error converting Avro to JSON: {e}")
        return None
    
    
if __name__ == "__main__":
    # Replace with your file paths
    avro_files = [
        "C:/Users/sabhy/WK/AvroToJson/avroToJson/salesken_topic/salesken_topic_1.avro",
        "C:/Users/sabhy/WK/AvroToJson/avroToJson/salesken_topic/salesken_topic_2.avro"
    ]
    json_file = "salesken_topic_04_09_2025.json"
    
    # Method 1: Convert to JSON file
    avro_to_json(avro_files, json_file)



# import avro.schema
# from avro.datafile import DataFileReader
# from avro.io import DatumReader
# import os

# def check_avro_file(avro_file_path):
#     try:
#         with open(avro_file_path, 'rb') as avro_file:
#             reader = DataFileReader(avro_file, DatumReader())
#             print(f"Attempting to read from: {avro_file_path}")

#             # Try to read the first record
#             first_record = next(reader, None)
#             if first_record:
#                 print("Successfully read the first record (or part of it). File structure seems okay.")
#                 print("First record keys:", first_record.keys())
#                 # print("First record:", first_record) # Uncomment to see the raw record
#             else:
#                 print("Avro file is empty or could not read any records.")

#             reader.close()
#             print("Avro file reader closed.")
#     except FileNotFoundError:
#         print(f"Error: Avro file not found at {avro_file_path}")
#     except Exception as e:
#         print(f"Error reading Avro file: {e}")

# if __name__ == "__main__":
#     avro_file = "C:/Users/sabhy/WK/AvroToJson/avroToJson/13_17_45_0.avro"
#     if not os.path.exists(avro_file):
#         print(f"ERROR: File not found at '{avro_file}'. Please check the path.")
#     else:
#         check_avro_file(avro_file)