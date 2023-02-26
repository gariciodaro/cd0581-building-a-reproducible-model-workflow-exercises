"""
Component to get data from remote pre process it and upload it back
as artifact.
"""
import argparse
import wandb
import pandas as pd


def preprocess_data(wdb_artifact_location: str):
    # create instance of run
    run = wandb.init(
        project="exercise_5",
        save_code=False
    )
    
    # get file from other run
    artifact = run.use_artifact(wdb_artifact_location)
    unprocessed_df = pd.read_parquet(artifact.file())
    
    # Let's drop the duplicates
    unprocessed_df = unprocessed_df.drop_duplicates().reset_index(drop=True)
    
    unprocessed_df['title'].fillna(value='', inplace=True)
    unprocessed_df['song_name'].fillna(value='', inplace=True)
    unprocessed_df['text_feature'] = unprocessed_df['title'] + ' ' + unprocessed_df['song_name']

    unprocessed_df.to_csv('./preprocessed_data.csv')
    # upload artifact
    artifact_to_upload = wandb.Artifact(
        name='preprocessed_data',
        type="file",
        description="pre processed data"
    )
    artifact_to_upload.add('./preprocessed_data.csv')
    run.finish()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="pre process and upload back file as artifact."
    )
    parser.add_argument(
        "--wdb_artifact_location", type=str, help="URL to the input file", required=True
    )

    args = parser.parse_args()
    
    wdb_artifact_location = args["wdb_artifact_location"]
    
    preprocess_data(wdb_artifact_location)

