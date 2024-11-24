import pandas as pd

def do_group_by(df:pd.core.frame.DataFrame,field_name:str) -> pd.core.frame.DataFrame:
    return df.groupby(field_name).agg(
            Requests=('Success', lambda x: (x == 'Y').sum()),
            Errors=('Success', lambda x: (x == 'N').sum()),
            LastAccess=('TimeStamp', 'max')
          ).reset_index()


if __name__ == "__main__":
    df = pd.read_csv('analytics.csv')

    report_by_apikey = do_group_by(df,"APIKey")
    print("By API Key\n", report_by_apikey)

    report_by_apiname = do_group_by(df,"APIName")
    print("By API Name\n", report_by_apiname)


