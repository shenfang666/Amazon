from __future__ import annotations

import sys
from pathlib import Path

# Ensure the parent directory is in the path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from server import GET_ROUTES, POST_ROUTES, DashboardHandler


def test_get_route_handlers_accept_query_arg():
    """All GET route handlers should accept query parameter for consistency."""
    failures = []
    for path, handler_name in GET_ROUTES.items():
        method = getattr(DashboardHandler, handler_name)
        # self + query parameter = 2 args minimum
        if method.__code__.co_argcount < 2:
            failures.append(f"{path} -> {handler_name} has {method.__code__.co_argcount} args (expected >= 2)")
    
    if failures:
        print("GET route handler signature failures:")
        for f in failures:
            print(f"  - {f}")
        assert False, "Some GET handlers don't accept query parameter"


def test_post_route_handlers_accept_query_arg():
    """All POST route handlers should accept query parameter for consistency."""
    failures = []
    for path, handler_name in POST_ROUTES.items():
        method = getattr(DashboardHandler, handler_name)
        # self + query parameter = 2 args minimum
        if method.__code__.co_argcount < 2:
            failures.append(f"{path} -> {handler_name} has {method.__code__.co_argcount} args (expected >= 2)")
    
    if failures:
        print("POST route handler signature failures:")
        for f in failures:
            print(f"  - {f}")
        assert False, "Some POST handlers don't accept query parameter"


def test_registered_get_handlers_exist():
    """All registered GET handlers should exist on DashboardHandler."""
    for path, handler_name in GET_ROUTES.items():
        assert hasattr(DashboardHandler, handler_name), f"{path} -> {handler_name} not found"


def test_registered_post_handlers_exist():
    """All registered POST handlers should exist on DashboardHandler."""
    for path, handler_name in POST_ROUTES.items():
        assert hasattr(DashboardHandler, handler_name), f"{path} -> {handler_name} not found"


if __name__ == "__main__":
    print("Running server route contract tests...")
    
    try:
        test_registered_get_handlers_exist()
        print("PASS: All registered GET handlers exist")
    except AssertionError as e:
        print(f"FAIL: {e}")
    
    try:
        test_registered_post_handlers_exist()
        print("PASS: All registered POST handlers exist")
    except AssertionError as e:
        print(f"FAIL: {e}")
    
    try:
        test_get_route_handlers_accept_query_arg()
        print("PASS: All GET handlers accept query arg")
    except AssertionError as e:
        print(f"FAIL: {e}")
    
    try:
        test_post_route_handlers_accept_query_arg()
        print("PASS: All POST handlers accept query arg")
    except AssertionError as e:
        print(f"FAIL: {e}")
    
    print("\nDone.")
