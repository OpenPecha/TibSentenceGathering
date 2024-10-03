import os

from TibSentenceGathering.sentence_validation import process_tibetan_sentences
from TibSentenceGathering.utils import write_json_file


def test_sentence_validation():
    # Define the input and output file paths
    input_json_path = "tests/data/input/sentence_segmentation.json"  # Replace with your actual input file path
    valid_output_path = "tests/data/output/valid_sentence_segmentation.json"  # noqa  # Replace with your desired valid output file path
    invalid_output_path = "tests/data/output/invalid_sentence_segmentation.json"  # noqa  # Replace with your desired invalid output file path

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
            f"Processing complete. {len(valid_data_points)} valid data points saved to '{valid_output_path}'."
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


if __name__ == "__main__":
    test_sentence_validation()
