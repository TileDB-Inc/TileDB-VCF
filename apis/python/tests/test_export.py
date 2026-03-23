import os

import pytest
import tiledbvcf

from .conftest import TESTS_INPUT_DIR


def test_export_default(tmp_path, v4_dataset):
    """export() with default parameters creates one vcf.gz per sample."""
    v4_dataset.export(output_dir=str(tmp_path))
    assert set(os.listdir(tmp_path)) == {"HG00280.vcf.gz", "HG01762.vcf.gz"}


def test_export_samples_filter(tmp_path, v4_dataset):
    """samples= restricts export to the specified samples only."""
    v4_dataset.export(samples=["HG00280"], output_dir=str(tmp_path))
    assert os.listdir(tmp_path) == ["HG00280.vcf.gz"]


def test_export_regions_filter(tmp_path, v4_dataset):
    """regions= restricts export to the specified genomic region."""
    v4_dataset.export(regions=["1:12000-13000"], output_dir=str(tmp_path))
    assert set(os.listdir(tmp_path)) == {"HG00280.vcf.gz", "HG01762.vcf.gz"}


@pytest.mark.parametrize(
    "output_format, expected_files",
    [
        ("z", {"HG00280.vcf.gz", "HG01762.vcf.gz"}),
        ("v", {"HG00280.vcf", "HG01762.vcf"}),
        ("b", {"HG00280.bcf", "HG01762.bcf"}),
        ("u", {"HG00280.bcf", "HG01762.bcf"}),
    ],
)
def test_export_output_format(tmp_path, output_format, expected_files):
    """output_format produces files with the correct extension."""
    ds = tiledbvcf.Dataset(
        os.path.join(TESTS_INPUT_DIR, "arrays/v4/ingested_2samples"), mode="r"
    )
    ds.export(output_format=output_format, output_dir=str(tmp_path))
    assert set(os.listdir(tmp_path)) == expected_files


def test_export_merge(tmp_path, v4_dataset):
    """merge=True produces a single combined output file at output_path."""
    out = str(tmp_path / "merged.vcf.gz")
    v4_dataset.export(merge=True, output_path=out, output_dir=str(tmp_path))
    assert os.path.exists(out)
    assert os.listdir(tmp_path) == ["merged.vcf.gz"]


def test_export_merge_without_output_path_raises(tmp_path, v4_dataset):
    """merge=True without output_path raises an exception."""
    with pytest.raises(Exception, match="output_path required when merge=True"):
        v4_dataset.export(merge=True, output_dir=str(tmp_path))


def test_export_samples_file(tmp_path, v4_dataset):
    """samples_file= restricts export to samples listed in the file."""
    samples_file = str(tmp_path / "samples.txt")
    out = str(tmp_path / "out")
    os.makedirs(out)
    with open(samples_file, "w") as f:
        f.write("HG00280\n")
    v4_dataset.export(samples_file=samples_file, output_dir=out)
    assert os.listdir(out) == ["HG00280.vcf.gz"]


def test_export_bed_file(tmp_path, v4_dataset):
    """bed_file= restricts export to regions defined in the BED file."""
    bed_file = str(tmp_path / "regions.bed")
    out = str(tmp_path / "out")
    os.makedirs(out)
    with open(bed_file, "w") as f:
        f.write("1\t12000\t13000\n")
    v4_dataset.export(bed_file=bed_file, output_dir=out)
    assert set(os.listdir(out)) == {"HG00280.vcf.gz", "HG01762.vcf.gz"}


def test_export_skip_check_samples(tmp_path, v4_dataset):
    """skip_check_samples=True skips existence check and produces no output for unknown samples."""
    v4_dataset.export(
        samples=["NOSUCHSAMPLE"], skip_check_samples=True, output_dir=str(tmp_path)
    )
    assert os.listdir(tmp_path) == []


def test_export_write_mode_raises(tmp_path):
    """export() raises when the dataset is open in write mode."""
    uri = str(tmp_path / "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    with pytest.raises(Exception, match="Dataset not open in read mode"):
        ds.export(output_dir=str(tmp_path))
