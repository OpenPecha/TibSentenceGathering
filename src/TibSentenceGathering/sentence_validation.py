import copy
import json
import os

from TibSentenceGathering.utils import write_json_file


def manhattan_distance(str1, str2):
    """
    Calculates the Manhattan distance between two strings.
    Returns -1 if the strings have unequal lengths.
    """
    if len(str1) != len(str2):
        return -1
    distance = sum(c1 != c2 for c1, c2 in zip(str1, str2))
    return distance


def replace_sentence_breaks_and_validate(data_point):
    """
    Processes a copy of the data point by removing spaces, replacing <sent_br> with newlines in target
    if there's a newline at the same position in source after removing spaces.
    Otherwise, removes <sent_br> from target.
    Then validates the processed source and target using Manhattan distance.

    Parameters:
        data_point (dict): A dictionary with "source" and "target" keys.

    Returns:
        bool: True if the data point is valid, False otherwise.
    """
    # Create a deep copy to avoid modifying the original data_point
    data_point_copy = copy.deepcopy(data_point)

    # Remove spaces from source and target
    source_no_spaces = data_point_copy["source"].replace(" ", "")
    target_no_spaces = data_point_copy["target"].replace(" ", "")

    # Do not change the source
    source_processed = source_no_spaces

    # Initialize indices
    src_idx = 0
    tgt_idx = 0

    # Reconstructed target after processing <sent_br>
    reconstructed_target = []

    # Inside the while loop
    while tgt_idx < len(target_no_spaces):
        if target_no_spaces.startswith("<sent_br>", tgt_idx):
            # Check for newline in source
            if src_idx < len(source_no_spaces) and source_no_spaces[src_idx] == "\n":
                # Corresponding newline found in source
                reconstructed_target.append("\n")
                src_idx += 1
            else:
                # No corresponding newline in source, remove <sent_br>
                pass
            tgt_idx += len("<sent_br>")
        else:
            if src_idx >= len(source_no_spaces):
                # Mismatch: Source ended before target
                return False
            # Append character
            reconstructed_target.append(target_no_spaces[tgt_idx])
            if target_no_spaces[tgt_idx] != source_no_spaces[src_idx]:
                # Mismatch between source and target characters
                return False
            src_idx += 1
            tgt_idx += 1

    # After processing, src_idx should reach the end of source_no_spaces or only have newlines left
    while src_idx < len(source_no_spaces):
        if source_no_spaces[src_idx] != "\n":
            # Extra character in source that's not a newline
            return False
        src_idx += 1

    # Reconstruct the processed target string
    processed_target = "".join(reconstructed_target)

    # Compute Manhattan distance
    distance = manhattan_distance(source_processed, processed_target)
    if distance == 0:
        # Data point is valid
        return True
    else:
        # Data point is invalid due to non-zero Manhattan distance
        return False


def process_tibetan_sentences(input_json_path):
    """
    Processes Tibetan sentence segmented data, validates data points using Manhattan distance.

    Parameters:
        input_json_path (str): Path to the input JSON file.

    Returns:
        tuple: (valid_data_points, invalid_data_points)
    """
    valid_data_points = []
    invalid_data_points = []
    # Check if the input file exists
    if not os.path.exists(input_json_path):
        print(f"Error: The input file '{input_json_path}' does not exist.")
        return [], []

    # Read the input JSON file line by line
    try:
        with open(input_json_path, encoding="utf-8") as infile:
            for line_number, line in enumerate(infile, start=1):
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        data_point = json.loads(line)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON on line {line_number}: {e}")
                        invalid_data_points.append(
                            {"line_number": line_number, "error": str(e), "line": line}
                        )
                        continue

                    # Ensure the data point has "source" and "target" keys
                    if "source" not in data_point or "target" not in data_point:
                        print(
                            f"Line {line_number}: Missing 'source' or 'target' keys. Adding to invalid data."
                        )
                        data_point["line_number"] = line_number
                        invalid_data_points.append(data_point)
                        continue

                    # Validate the data point without modifying it
                    is_valid = replace_sentence_breaks_and_validate(data_point)
                    if is_valid:
                        valid_data_points.append(data_point)
                    else:
                        invalid_data_points.append(data_point)
    except Exception as e:
        print(f"An error occurred while reading '{input_json_path}': {e}")
        return [], []

    # Return the valid and invalid data points
    return valid_data_points, invalid_data_points


if __name__ == "__main__":
    # Define the input and output file paths
    input_json_path = "data/input/sentence_segmentation.json"  # Replace with your actual input file path
    valid_output_path = "data/output/sentence_valid_output.json"  # Replace with your desired valid output file path
    invalid_output_path = "data/output/sentence_invalid_output.json"  # noqa # Replace with your desired invalid output file path

    # Call the processing function
    valid_data_points, invalid_data_points = process_tibetan_sentences(input_json_path)

    # Ensure the output directories exist
    for output_path in [valid_output_path, invalid_output_path]:
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
                print(f"Created output directory: '{output_dir}'")
            except Exception as e:
                print(f"Failed to create directory '{output_dir}': {e}")
                exit(1)

    # Write the valid data points to the valid output JSON file
    try:
        write_json_file(valid_output_path, valid_data_points)
        print(
            f"\nProcessing complete. {len(valid_data_points)} valid data points saved to '{valid_output_path}'."
        )
    except Exception as e:
        print(f"An error occurred while writing to '{valid_output_path}': {e}")

    # Write the invalid data points to the invalid output JSON file
    try:
        write_json_file(invalid_output_path, invalid_data_points)
        print(
            f"{len(invalid_data_points)} invalid data points saved to '{invalid_output_path}'."
        )
    except Exception as e:
        print(f"An error occurred while writing to '{invalid_output_path}': {e}")
