import pytest

from scripts.config_loader import load_config, resolve_local_path


def test_load_valid_config():
    config = load_config()

    assert config["environment"] == "dev"
    assert config["data_generation"]["flight_count"] == 50000


def test_missing_config_file():
    with pytest.raises(FileNotFoundError):
        load_config("config/does_not_exist.yaml")


def test_invalid_yaml(tmp_path):
    file = tmp_path / "invalid.yaml"
    file.write_text("environment: [invalid", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid YAML"):
        load_config(file)


def test_missing_required_section(tmp_path):
    file = tmp_path / "incomplete.yaml"
    file.write_text("environment: dev", encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required"):
        load_config(file)


def test_invalid_flight_count(tmp_path):
    config = load_config()
    config["data_generation"]["flight_count"] = -10

    import yaml

    file = tmp_path / "invalid_count.yaml"
    file.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(ValueError, match="positive integer"):
        load_config(file)


def test_relative_path_resolves_from_project_root():
    path = resolve_local_path("data/raw")

    assert path.is_absolute()
    assert path.name == "raw"