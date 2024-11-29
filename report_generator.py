import pandas as pd

#If 1st arg is named as df, it is reported as shadow variable
#because it shares the same name as variable we declared in
#within if block . It is very bad
def do_group_by(data_frame:pd.core.frame.DataFrame, field_name:str) -> pd.core.frame.DataFrame:
    return data_frame.groupby(field_name).agg(
            Requests=('Success', lambda x: (x == 'Y').sum()),
            Errors=('Success', lambda x: (x == 'N').sum()),
            LastAccess=('TimeStamp', 'max')
          ).reset_index()


if __name__ == "__main__":
    df = pd.read_csv('analytics.csv')

    report_by_apikey = do_group_by(df,"APIKey")
    print("By API Key\n", report_by_apikey)

    report_by_api_name = do_group_by(df,"APIName")
    print("By API Name\n", report_by_api_name)



