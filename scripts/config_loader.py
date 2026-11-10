from pathlib import Path
import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent

REQUIRED_SECTIONS = (
    "environment",
    "project",
    "data_generation",
    "storage",
    "spark",
)


def load_config(config_path="config/dev.yaml"):
    path = Path(config_path)

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    if not path.is_file():
        raise FileNotFoundError(
            f"Configuration file not found: {path}"
        )

    try:
        with path.open("r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
    except yaml.YAMLError as exc:
        raise ValueError(
            f"Invalid YAML configuration: {path}"
        ) from exc

    if not isinstance(config, dict):
        raise ValueError(
            "Configuration must be a YAML mapping."
        )

    for section in REQUIRED_SECTIONS:
        if section not in config:
            raise ValueError(
                f"Missing required configuration section: {section}"
            )

    generation = config["data_generation"]
    storage = config["storage"]
    spark = config["spark"]

    if not isinstance(generation, dict):
        raise ValueError("data_generation must be a mapping.")

    if not isinstance(storage, dict):
        raise ValueError("storage must be a mapping.")

    if not isinstance(spark, dict):
        raise ValueError("spark must be a mapping.")

    for key in ("flight_count", "booking_count", "ticket_count"):
        value = generation.get(key)

        if type(value) is not int or value <= 0:
            raise ValueError(
                f"data_generation.{key} must be a positive integer."
            )

    if type(generation.get("seed")) is not int:
        raise ValueError("data_generation.seed must be an integer.")

    if generation.get("output_format") not in ("csv", "parquet"):
        raise ValueError("Unsupported output_format.")

    for key in ("raw_path", "processed_path", "sample_path"):
        value = storage.get(key)

        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"storage.{key} must be a non-empty path.")

    if not isinstance(config["environment"], str) or not config["environment"].strip():
        raise ValueError("environment must be a non-empty string.")

    project = config["project"]
    if not isinstance(project, dict) or not isinstance(project.get("name"), str) or not project["name"].strip():
        raise ValueError("project.name must be a non-empty string.")

    for key in ("app_name", "master", "log_level"):
        if not isinstance(spark.get(key), str) or not spark[key].strip():
            raise ValueError(f"spark.{key} must be a non-empty string.")

    if type(spark.get("shuffle_partitions")) is not int or spark["shuffle_partitions"] <= 0:
        raise ValueError("spark.shuffle_partitions must be a positive integer.")

    return config


def resolve_local_path(path):
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate