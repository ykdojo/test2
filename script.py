import daft


def main(data: daft.DataFrame) -> daft.DataFrame:
    """Test function that receives the CommonCrawl example dataset."""
    print("=" * 60)
    print("Testing CommonCrawl Example Dataset")
    print("=" * 60)

    # Show schema
    print("\nSchema:")
    print(data.schema())

    # Show sample data
    print("\nSample data (5 rows):")
    result = data.limit(5).collect()
    print(result)

    # Count total records (just from the limited file)
    print("\nCounting records...")
    count = data.count_rows()
    print(f"Total records in dataset: {count}")

    return result
