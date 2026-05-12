from etl.pipeline import run_pipeline


def main():
    symbol = input("Enter stock ticker: ").strip().upper()

    if not symbol:
        print("Ticker cannot be empty")
        return

    run_pipeline(symbol)


if __name__ == "__main__":
    main()