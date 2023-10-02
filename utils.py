import asyncio


async def prepare_data(metrics):  # noqa
    """
    Fetches data from the API for the given metrics
    :param metrics: list of Metric objects
    :return: pandas DataFrame
    """
    tasks = [metric.fetch_data() for metric in metrics]
    results = await asyncio.gather(*tasks)
    data = {key: value for result in results for key, value in result.items()}
    return data


def merge_dataframes(data_dict, metric_names):
    """
    Merges multiple DataFrames into a single DataFrame
    :param data_dict: dict with metric names as keys and DataFrames as values
    :param metric_names: list of metric names to merge
    :return: merged DataFrame
    """
    # Start with the first metric's DataFrame
    try:
        merged_df = data_dict[metric_names[0]]
    except KeyError:
        raise ValueError(f'Metric not found in data dictionary: {metric_names[0]}')

    # Merge the rest of the DataFrames
    for metric_name in metric_names[1:]:
        try:
            df = data_dict[metric_name]
        except KeyError:
            raise ValueError(f'Metric not found in data dictionary: {metric_name}')

        # Check for duplicate column names before merging
        common_cols = set(merged_df.columns) & set(df.columns)
        for col in common_cols:
            df = df.rename(columns={col: f'{metric_name}_{col}'})

        # Merge the DataFrame, handling missing values
        merged_df = merged_df.join(df, how='outer')
        # merged_df = pd.merge(merged_df, df, how='outer', left_index=True, right_index=True)

    # Handle missing values in the merged DataFrame
    merged_df = merged_df.fillna(0)  # replace NaNs with 0; change this as needed

    return merged_df


def aggregate_stablecoin_supplies(data_dict):
    """
    Aggregates stablecoin supplies into a single DataFrame
    :param data_dict: dict with metric names as keys and DataFrames as values
    :return: aggregated DataFrame
    """
    df = merge_dataframes(data_dict, ['USDT Supply', 'USDC Supply', 'TUSD Supply', 'BUSD Supply', 'GUSD Supply',
                                      'DAI Supply', 'FRAX Supply'])
    crv_usd = data_dict['crvUSD Supply (DeFi Llama)']['Total_circulating_supply'].to_frame('crvUSD Supply').resample(
        'D').first()
    lusd = data_dict['LUSD Supply (DeFi Llama)']['Total_circulating_supply'].to_frame('LUSD Supply').resample(
        'D').first()
    mim = data_dict['MIM Supply (DeFi Llama)']['Total_circulating_supply'].to_frame('MIM Supply').resample('D').first()
    fei = data_dict['FEI Supply (DeFi Llama)']['Total_circulating_supply'].to_frame('FEI Supply').resample('D').first()
    df = df.merge(crv_usd, how='left', left_index=True, right_index=True)
    df = df.merge(lusd, how='left', left_index=True, right_index=True)
    df = df.merge(mim, how='left', left_index=True, right_index=True)
    df = df.merge(fei, how='left', left_index=True, right_index=True)
    df = df.interpolate(method='linear', limit_direction='forward', axis=0)
    return df
