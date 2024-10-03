import json


def write_json_file(file_path, data):
    """
    Writes data to a JSON file.

    Parameters:
        file_path (str): The path to the output JSON file.
        data: The data to write to the JSON file.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        print(f"Data successfully written to '{file_path}'.")
    except Exception as e:
        print(f"An error occurred while writing to '{file_path}': {e}")
        raise
