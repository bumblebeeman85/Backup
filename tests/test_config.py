import os
import yaml


def test_load_example_tenants():
    path = os.path.join(os.path.dirname(__file__), "..", "tenants.example.yaml")
    path = os.path.abspath(path)
    assert os.path.exists(path), "Example tenants file must exist"
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert "tenants" in data
