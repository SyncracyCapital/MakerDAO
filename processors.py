from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd
from pandas import json_normalize

import streamlit as st


class Processor(ABC):
    @abstractmethod
    def process(self, data):
        pass


class GlassNodeProcessor(Processor):
    """
    Processor for GlassNode API data
    """

    def process(self, data, metric_name):  # noqa
        """
        Process data for Glassnode endpoints
        :param data: json response
        :param metric_name: name of metric
        :return: pandas dataframe
        """
        df = json_normalize(data)
        df.set_index('t', inplace=True)
        if len(df.columns) > 1:
            df.columns = [x.split('.')[1] for x in df.columns]
        else:
            df.columns = [metric_name]
        df.index = pd.to_datetime(df.index, unit='s')
        return df


class DeFiLlamaProcessor(Processor):
    """
    Processor for DeFiLlama API data
    """

    def process(self, data, metric_name):  # noqa
        """
        Process data for DeFiLlama endpoints
        :param data: json response
        :param metric_name: name of metric
        :return: pandas dataframe
        """
        merged_df = pd.DataFrame()
        chains = list(data['chainBalances'].keys())
        for chain in chains:
            data_list = []
            for entry in data['chainBalances'][chain]['tokens']:
                date = datetime.fromtimestamp(entry['date'])
                circulating_supply = entry['circulating']['peggedUSD']
                data_list.append({'date': date, f'{chain}_circulating_supply': circulating_supply})
            chain_df = pd.DataFrame(data_list).set_index('date')
            if merged_df.empty:
                merged_df = chain_df
            else:
                merged_df = merged_df.join(chain_df, how='outer')
        merged_df['Total_circulating_supply'] = merged_df.sum(axis=1)
        return merged_df


class BlockAnalyticaProcessor(Processor):
    """
    Processor for BlockAnalytica API data
    """

    def process(self, data, metric_name):  # noqa
        if metric_name == 'Debt-at-Risk':
            df = pd.DataFrame(data['results'])
            return df
        elif metric_name == 'PSMS':
            df = pd.DataFrame(data)
            df.set_index('datetime', inplace=True)
            df.index = pd.to_datetime([x[:10] for x in df.index], format='%Y-%m-%d')
            return df
        else:
            return data


class MKRBurnProcessor(Processor):
    """
    Processor for MKR Burn API data
    """

    def process(self, data, metric_name):  # noqa
        """
        Process data for MKR Burn endpoints
        :param data: json response
        :param metric_name: name of metric
        :return: pandas dataframe
        """
        if metric_name == 'Surplus Buffer':
            df = pd.DataFrame(data)
            df.set_index('date', inplace=True)
            df.index = pd.to_datetime([x[:10] for x in df.index], format='%Y-%m-%d')
            return df
        elif metric_name == 'Treasury':
            df = pd.DataFrame(data['history'])
            df.set_index('date', inplace=True)
            df.index = pd.to_datetime([x[:10] for x in df.index], format='%Y-%m-%d')
            df['MKR Balance'] = df['mkr_price'] * df['mkr_balance']
            df['AAVE Balance'] = df['aave_price'] + df['aave_balance']
            df['ENS Balance'] = df['ens_price'] * df['ens_balance']
            return df
        else:
            return data


class DuneProcessor(Processor):
    """
    Processor for Dune API data
    """
    def process(self, data, metric_name):  # noqa
        """
        Process data for MKR Burn endpoints
        :param data: json response
        :param metric_name: name of metric
        :return: pandas dataframe
        """
        df = pd.DataFrame(data['result']['rows'])
        if 'date' in list(df.columns):
            df.set_index('date', inplace=True)
            df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
        else:
            df.set_index('dt', inplace=True)
            df.index = pd.to_datetime([x[:10] for x in df.index], format='%Y-%m-%d')
        return df
