import daft

print("Package loaded!")


def main(data: daft.DataFrame) -> daft.DataFrame:
    """Test function that receives the CommonCrawl example dataset."""
    print("main() function called!")
    print("=" * 60)
    print("Testing CommonCrawl Example Dataset")
    print("=" * 60)

    print("\nSchema:")
    print(data.schema())

    print("\nSample data (5 rows):")
    result = data.limit(5).collect()
    print(result)

    print("\nCounting records...")
    count = data.count_rows()
    print(f"Total records in dataset: {count}")

    return result
