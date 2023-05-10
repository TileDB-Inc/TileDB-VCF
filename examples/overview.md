---
title: Overview
---

This section provides fully runnable examples.

## TileDB-VCF Basics

notebook: [tutorial_tiledbvcf_basics](https://cloud.tiledb.com/notebooks/details/TileDB-Inc/337a0fd0-7f10-446f-b4a3-e924f7d3c209/preview)

### Overview

Provides a high-level overview of how to work with VCF data using TileDB. You'll see how to use TileDB-VCF's Python package to:

* create and populate new TileDB-VCF datasets
* query variant data by genomic region and/or sample and selectively retrieve specific VCF attributes
* parallelize queries by partitioning the data and distributing the queries across multiple processes
* easily scale queries using TileDB Cloud's serverless infrastructure## Introduction


## TileDB-VCF Allele Frequencies

notebook: [tutorial_tiledbvcf_allele_frequencies](https://cloud.tiledb.com/notebooks/details/TileDB-Inc/3e07f857-12dc-4004-a103-6eaf5058c41c/preview)

### Overview

Provides a high-level overview of how to work with allele frequencies as part of TileDB-VCF. You'll see how to use TileDB-VCF's Python package to:

* Selecting allele frequencies
* Filtering on allele frequencies
* Direct allele frequency access


## TileDB-VCF Task Graph-Based Genome-wide Analysis

notebook: [tutorial_tiledbvcf_gwas](https://cloud.tiledb.com/notebooks/details/TileDB-Inc/e637f616-a541-4167-a0b3-45ef7277042c/preview)

### Overview

In this notebook we'll perform a rudimentary genome-wide association study using the 1000 Genomes (1KG) dataset. The goal of this tutorial is to demonstrate the mechanics of performing genome-wide analyses using variant call data stored with TileDB-VCF and how such analyses can be easily scaled using TileDB Cloud's serverless computation platform.