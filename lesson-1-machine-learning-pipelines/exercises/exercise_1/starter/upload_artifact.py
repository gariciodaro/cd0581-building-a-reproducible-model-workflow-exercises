#!/usr/bin/env python
import argparse
import logging
import pathlib
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    logger.info("Creating run exercise_1")
    run = wandb.init(project="exercise_1", job_type="upload_file")

    logger.info("creating artifact object")
    artifact = wandb.Artifact(
        name=args.artifact_name, type=args.artifact_type, description=args.artifact_desc
    )

    logger.info("adding path to artifact object")
    artifact.add_file(local_path=args.input_file)

    logger.info("adding artifact object to run object")
    run.log_artifact(artifact)

    logger.info("Closing run")
    run.finish()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload an artifact to W&B", fromfile_prefix_chars="@"
    )

    parser.add_argument(
        "--input_file", type=pathlib.Path, help="Path to the input file", required=True
    )

    parser.add_argument(
        "--artifact_name", type=str, help="Name for the artifact", required=True
    )

    parser.add_argument(
        "--artifact_type", type=str, help="Type for the artifact", required=True
    )

    parser.add_argument(
        "--artifact_desc",
        type=str,
        help="Description for the artifact",
        required=True,
    )

    args = parser.parse_args()

    go(args)
