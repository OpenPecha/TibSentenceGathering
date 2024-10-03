import os

from TibSentenceGathering.deduplication import process_in_batches


def test_sentence_deduplication():
    input_file = "tests/data/input/valid_sentence.json"
    output_file = "tests/data/output/deduplicated_sentence_segmentation.json"
    duplicates_output_file = "tests/data/output/duplicates_sentence_segmentation.json"

    # Ensure the output directory exists
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory {output_dir}")

    process_in_batches(
        input_file,
        output_file,
        duplicates_output_file,
        chunk_size=1000,
        checkpoint_interval=5,
    )
