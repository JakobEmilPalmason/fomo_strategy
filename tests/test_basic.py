import pytest
import pandas as pd
import pyarrow as pa
from pathlib import Path
import sys
import os

# Add scripts to path for testing
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

def test_imports():
    """Test that all required modules can be imported."""
    try:
        import split_adjust_minutes
        import coalesce_unadjusted_minutes
        import validation
        import verify_adjustment
        assert True
    except ImportError as e:
        pytest.fail(f"Failed to import module: {e}")

def test_environment():
    """Test that required packages are available."""
    assert pd.__version__ is not None
    assert pa.__version__ is not None

def test_project_structure():
    """Test that essential project files exist."""
    base = Path(__file__).parent.parent
    
    # Essential files
    assert (base / "requirements.txt").exists()
    assert (base / "README.md").exists()
    assert (base / "scripts" / "split_adjust_minutes.py").exists()
    
    # Optional but expected files
    assert (base / "env_template.txt").exists()
    assert (base / ".gitignore").exists()

def test_parquet_io():
    """Test basic parquet read/write functionality."""
    # Create test data
    test_data = pd.DataFrame({
        'ts': pd.date_range('2024-01-01', periods=10, freq='1min'),
        'open': [100.0] * 10,
        'high': [101.0] * 10,
        'low': [99.0] * 10,
        'close': [100.5] * 10,
        'volume': [1000] * 10
    })
    
    # Test write
    test_file = Path("test_data.parquet")
    try:
        test_data.to_parquet(test_file, index=False)
        assert test_file.exists()
        
        # Test read
        read_data = pd.read_parquet(test_file)
        assert len(read_data) == len(test_data)
        assert list(read_data.columns) == list(test_data.columns)
        
    finally:
        if test_file.exists():
            test_file.unlink()

if __name__ == "__main__":
    pytest.main([__file__]) 