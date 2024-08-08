from prettytable import PrettyTable

def print_table_with_index(df):
    """Print DataFrame in table format with index."""
    table = PrettyTable()
    table.field_names = ["Index"] + list(df.columns)
    for idx, row in df.iterrows():
        table.add_row([idx] + list(row))
    print(table)

def print_table_with_summary(df, num_display_columns=9):
    """Print DataFrame with dashed line separators and column summary."""
    displayed_columns = df.columns[:num_display_columns]
    hidden_columns = df.columns[num_display_columns:]

    table = PrettyTable()
    table.field_names = ["Index"] + list(displayed_columns) + ["Other columns"]

    for idx, row in df.iterrows():
        displayed_row = [str(row[col]) for col in displayed_columns]
        hidden_summary = f"Other columns ({len(hidden_columns)} more)"
        table.add_row([idx] + displayed_row + [hidden_summary])

    print(table)
    print(f"\nShowing {num_display_columns} columns out of {len(df.columns)} total columns.")
    print(f"Hidden columns ({len(hidden_columns)}): {', '.join(hidden_columns)}")
    