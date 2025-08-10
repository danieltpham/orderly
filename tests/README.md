# Test Suite for Orderly

This directory contains the test suite for the human-in-the-loop (HITL) stage.

## Running Tests

### Run all tests
```bash
pytest tests/
```

### Run specific test file
```bash
pytest tests/test_alias_cleaning.py
```

### Run with coverage
```bash
pytest tests/test_alias_cleaning.py --cov=hitl.utils.alias_cleaning --cov-report=term-missing
```

### Run with verbose output
```bash
pytest tests/test_alias_cleaning.py -v
```

## Test Files

### `test_alias_cleaning.py`
Comprehensive test suite for the `hitl.utils.alias_cleaning` module, covering:

- **Text Normalization** (8 tests)
  - Basic text cleaning and formatting
  - Punctuation removal and whitespace handling
  - Stop word filtering
  - Token extraction and filtering

- **Typo Collapse** (5 tests) 
  - Edit distance-based typo correction
  - Frequency-based representative selection
  - Minimum length requirements
  - Edge cases and empty inputs

- **Canonical Token Selection** (3 tests)
  - Most frequent token identification
  - Presence-based vs count-based frequency
  - Empty input handling

- **Alias Ranking** (4 tests)
  - Similarity scoring between aliases and target tokens
  - Frequency-based tie-breaking
  - Empty target handling
  - String transformation validation

- **End-to-End Pipeline** (6 tests)
  - Complete transformation workflow
  - Parameter preservation
  - Real-world example validation
  - Empty and edge case handling

- **Edge Cases & Integration** (3 tests)
  - Unicode character handling
  - Very long text processing
  - Realistic product consolidation scenarios

**Coverage: 84%** (29 tests passing)

## Test Configuration

### `conftest.py`
Pytest configuration with shared fixtures:
- `project_root_path`: Project root directory path
- `sample_aliases`: Common test data for alias processing
- `sample_typo_aliases`: Test data with intentional typos

### `requirements.txt`
Test-specific dependencies:
- `pytest>=7.0.0`: Testing framework
- `pytest-cov>=4.0.0`: Coverage reporting
- `pytest-mock>=3.10.0`: Mocking capabilities

## Test Data

Tests use realistic product alias examples including:
- Wireless keyboards with various naming conventions
- Common typos like "keybord" → "keyboard"
- Brand variations and abbreviations
- Technical specifications and color variants

This ensures the alias cleaning pipeline handles real-world data quality challenges effectively.
