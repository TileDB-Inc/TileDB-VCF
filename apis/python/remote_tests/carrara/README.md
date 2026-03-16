# Carrara Tests

TileDB v3 aka Carrara introduces a new URL schema with supports relative paths and group membership lifecycle changes.

This directory contains unit tests for Carrara-specific behavior.

# Running the tests

Prerequisites

- You have a user account in a Carrara deployment
- You have set up your TileDB profile so that you can log into the account with a profile name
- You have created a teamspace for testing use

With that, set the environment variables and install package dependencies:

```bash
pip install tiledb_client
export CARRARA_TEST_PROFILE="..."    # Profile name
export CARRARA_TEST_WORKSPACE="..."  # Workspace name (must match workspace in the profile)
export CARRARA_TEST_TEAMSPACE="..."  # Teamspace to use for tests
```

Run the tests with pytest:
```bash
pytest api/python/remotes_tests/carrara/ --carrara -v
```

Objects will be created at the specified workspace/teamspace: `tiledb://${CARRARA_TEST_WORKSPACE}/${CARRARA_TEST_TEAMSPACE}/...`. The test should remove all created objects when completed; however, assets may remain in the case of a test failure.
