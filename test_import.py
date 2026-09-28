#!/usr/bin/env python
"""Test if all new functions can be imported from sf_bulk_loader"""

try:
    from sf_bulk_loader import (
        parallel_snowflake_fetch, 
        get_object_complexity_score, 
        get_auto_chunk_size
    )
    print("SUCCESS: All functions imported from sf_bulk_loader")
    print(f"  - parallel_snowflake_fetch: {type(parallel_snowflake_fetch).__name__}")
    print(f"  - get_object_complexity_score: {type(get_object_complexity_score).__name__}")
    print(f"  - get_auto_chunk_size: {type(get_auto_chunk_size).__name__}")
    
    # Also test app.py imports
    print("\nTesting app.py imports...")
    from app import *
    print("SUCCESS: app.py imported all required functions")
    
except ImportError as e:
    print(f"IMPORT ERROR: {e}")
    import traceback
    traceback.print_exc()
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
