import polars as pl
from pathlib import Path

def generate_users_parquet(output_path: str = "data/users.parquet"):
    Path("data").mkdir(exist_ok=True)
    
    df = pl.DataFrame({
        "user_id": [1, 2, 2, 3, None],
        "email": ["a@example.com", "b@example.com", "bademail", None, "c@example.com"],
        "status": ["active", "inactive", "pending", "active", "invalid"],
    })
    
    df.write_parquet(output_path)
    print(f"✅ users.parquet written to: {output_path}")

if __name__ == "__main__":
    generate_users_parquet()