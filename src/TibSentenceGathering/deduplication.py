import json
import os

import datasketch
from tqdm import tqdm


def create_deduplicated_chunk(chunk, start_idx, lsh, minhashes):
    """
    Deduplicates a chunk of data using MinHash and LSH, returning the unique entries.

    Args:
        chunk (list): The chunk of data (list of dictionaries) to process.
        start_idx (int): The starting index of the chunk.
        lsh (MinHashLSH): The LSH index to check for duplicates.
        minhashes (dict): A dictionary to keep track of MinHash objects.

    Returns:
        tuple: A tuple containing:
            - unique_entries (list): List of unique entries.
            - duplicated_entries (list): List of duplicate entries.
    """
    unique_entries = []
    duplicated_entries = []

    for idx, entry in enumerate(chunk):
        source_text = entry["source"]
        target_text = entry["target"]
        file_name = entry.get(
            "filename", None
        )  # Use .get() to handle missing 'filename'

        # Create a MinHash object for the source text
        minhash = datasketch.MinHash(num_perm=128)
        for paragraph in set(source_text.split("\n")):
            minhash.update(paragraph.encode("utf8"))

        # Check for duplicates in the LSH index
        if not lsh.query(minhash):
            # Insert unique source into LSH index
            lsh.insert(start_idx + idx, minhash)
            minhashes[start_idx + idx] = minhash
            # Build the unique entry
            unique_entry = {"source": source_text, "target": target_text}
            if file_name:
                unique_entry["filename"] = file_name
            unique_entries.append(unique_entry)
        else:
            # Build the duplicate entry
            duplicate_entry = {"source": source_text, "target": target_text}
            if file_name:
                duplicate_entry["filename"] = file_name
            duplicated_entries.append(duplicate_entry)

    return unique_entries, duplicated_entries


def save_checkpoint(output_file, entries, mode="a"):
    """
    Saves entries to the output JSON file.

    Args:
        output_file (str): The file where the data is saved.
        entries (list): The list of entries to save.
        mode (str): The file mode ('w' for write, 'a' for append).
    """
    if mode == "w" or not os.path.exists(output_file):
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, indent=4)
    else:
        with open(output_file, encoding="utf-8") as f:
            existing_entries = json.load(f)
        existing_entries.extend(entries)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(existing_entries, f, ensure_ascii=False, indent=4)


def process_in_batches(
    input_file,
    output_file,
    duplicates_output_file,
    chunk_size=1000,
    checkpoint_interval=5,
):
    """
    Processes the JSON file in batches, deduplicates the 'source' texts using MinHash and LSH,
    and saves the deduplicated dataset in chunks to reduce memory usage.

    Args:
        input_file (str): Path to the input JSON file.
        output_file (str): Output file path for the deduplicated dataset.
        duplicates_output_file (str): Output file path for the duplicate entries.
        chunk_size (int): Number of entries to process at a time.
        checkpoint_interval (int): Number of batches to process before saving a checkpoint.
    """
    with open(input_file, encoding="utf-8") as f:
        json_data = json.load(f)

    # Initialize LSH and MinHash tracking
    lsh = datasketch.MinHashLSH(threshold=0.9, num_perm=128)
    minhashes = {}

    total_rows = len(json_data)
    total_batches = (total_rows // chunk_size) + (
        1 if total_rows % chunk_size > 0 else 0
    )

    # Initialize processed batches
    processed_batches = 0
    if os.path.exists(output_file) and os.path.exists(duplicates_output_file):
        # Assuming that both files have been saved with the same checkpoint interval
        with open(output_file, encoding="utf-8") as f:
            dedup_entries = json.load(f)
        with open(duplicates_output_file, encoding="utf-8") as f:
            dup_entries = json.load(f)
        total_processed_entries = len(dedup_entries) + len(dup_entries)
        processed_batches = total_processed_entries // chunk_size
        print(f"Resuming from batch {processed_batches + 1}")
        # Reconstruct the LSH and minhashes up to the last processed entry
        for idx, entry in enumerate(dedup_entries):
            source_text = entry["source"]
            minhash = datasketch.MinHash(num_perm=128)
            for paragraph in set(source_text.split("\n")):
                minhash.update(paragraph.encode("utf8"))
            lsh.insert(idx, minhash)
            minhashes[idx] = minhash
    else:
        # Initialize output files
        save_checkpoint(output_file, [], mode="w")
        save_checkpoint(duplicates_output_file, [], mode="w")

    print(f"Total batches: {total_batches}, Processed batches: {processed_batches}")

    deduplicated_entries = []
    duplicate_entries = []

    with tqdm(
        total=total_batches,
        initial=processed_batches,
        desc="Processing JSON data in chunks",
    ) as pbar:
        for batch_num in range(processed_batches, total_batches):
            start_idx = batch_num * chunk_size
            end_idx = min(start_idx + chunk_size, total_rows)
            chunk = json_data[start_idx:end_idx]

            unique_entries, duplicated_entries_batch = create_deduplicated_chunk(
                chunk, start_idx, lsh, minhashes
            )

            deduplicated_entries.extend(unique_entries)
            duplicate_entries.extend(duplicated_entries_batch)

            # Save checkpoint after every `checkpoint_interval` batches or at the last batch
            if (
                batch_num + 1
            ) % checkpoint_interval == 0 or batch_num == total_batches - 1:
                save_checkpoint(output_file, deduplicated_entries)
                save_checkpoint(duplicates_output_file, duplicate_entries)
                print(f"Checkpoint saved at batch {batch_num + 1}/{total_batches}.")
                # Clear the entries after saving to reduce memory usage
                deduplicated_entries = []
                duplicate_entries = []

            pbar.update(1)  # Update progress bar

    # Print final statistics
    with open(output_file, encoding="utf-8") as f:
        total_dedup_entries = len(json.load(f))
    with open(duplicates_output_file, encoding="utf-8") as f:
        total_dup_entries = len(json.load(f))
    print(f"Total input entries: {total_rows}")
    print(f"Total deduplicated entries: {total_dedup_entries}")
    print(f"Total duplicate entries: {total_dup_entries}")
    print(
        f"Sum of deduplicated and duplicate entries: {total_dedup_entries + total_dup_entries}"
    )


if __name__ == "__main__":
    input_file = "data/output/sentence_valid_output.json"
    output_file = "data/output/deduplicated_sentence_valid_output.json"
    duplicates_output_file = "data/output/duplicates_sentence_valid_output.json"

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
