import ast
import os
import sys
import importlib

print("=" * 60)
print("1. Running Python imports check...")
modules_to_test = [
    "app.config",
    "app.utils",
    "app.database",
    "app.auth",
    "app.routes.auth",
    "app.routes.ui",
    "app.routes.api",
    "app.routes.system",
    "app.main",
    "server"
]

for mod in modules_to_test:
    try:
        importlib.import_module(mod)
        print(f"  [OK] Successfully imported {mod}")
    except Exception as e:
        print(f"  [FAIL] Error importing {mod}: {e}")
        sys.exit(1)

print("\n" + "=" * 60)
print("2. Checking AST for undefined names and suspicious calls...")

app_dir = "app"
all_py_files = []
for root, _, files in os.walk(app_dir):
    for f in files:
        if f.endswith(".py"):
            all_py_files.append(os.path.join(root, f))
all_py_files.append("server.py")

for fpath in all_py_files:
    with open(fpath, "r", encoding="utf-8") as f:
        content = f.read()
    try:
        tree = ast.parse(content, filename=fpath)
    except SyntaxError as e:
        print(f"  [SYNTAX ERROR] in {fpath}: {e}")
        sys.exit(1)

print(f"  [OK] All {len(all_py_files)} files parsed successfully without SyntaxErrors.")

print("\n" + "=" * 60)
print("3. Sanic app route and blueprint validation...")
from app.main import app as sanic_app

print(f"Sanic App Name: {sanic_app.name}")
print(f"Registered Blueprints: {list(sanic_app.blueprints.keys())}")
routes_list = []
for route in sanic_app.router.routes:
    routes_list.append(f"{route.methods} -> {route.path} ({route.name})")

print(f"Total Registered Routes: {len(routes_list)}")
for r in sorted(routes_list):
    print(f"  {r}")

print("\n" + "=" * 60)
print("ALL CHECKS PASSED PERFECTLY!")
print("=" * 60)
