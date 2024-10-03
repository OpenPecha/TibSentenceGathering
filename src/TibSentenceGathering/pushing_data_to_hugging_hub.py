import pandas as pd
from datasets import Dataset, DatasetDict


def get_data_df(json_file):
    """
    This function reads a JSON file and converts it into a pandas DataFrame.
    The JSON file is expected to contain 'source', 'target', and 'filename' fields.
    """
    # Load JSON data into pandas DataFrame
    df = pd.read_json(json_file)

    # Check if the DataFrame is loaded correctly
    print(df.head())  # Optional: Just to verify if the data is loaded correctly

    return df


def push_df_to_hub(train_df):
    """
    This function directly pushes a pandas DataFrame to the Hugging Face Hub.
    """
    # Convert the pandas DataFrame to Hugging Face Dataset
    train_dataset = Dataset.from_pandas(train_df)

    # Create a DatasetDict for Hugging Face
    dataset_dict = DatasetDict(
        {
            "train": train_dataset,
        }
    )

    # Push the dataset to Hugging Face Hub
    dataset_dict.push_to_hub("Jimpa2000/deduplication_sentence_seg_data")


# Usage example
json_file = "data/output/deduplicated_sentence_valid_output.json"
df = get_data_df(json_file)
push_df_to_hub(df)
